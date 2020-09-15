/*
 * Copyright 2020 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel.uring;

import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import static java.lang.Math.max;
import static java.lang.Math.min;

final class IOUringSubmissionQueue {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(IOUringSubmissionQueue.class);

    private static final long SQE_SIZE = 64;
    private static final int INT_SIZE = Integer.BYTES; //no 32 Bit support?
    private static final int KERNEL_TIMESPEC_SIZE = 16; //__kernel_timespec

    //these offsets are used to access specific properties
    //SQE https://github.com/axboe/liburing/blob/master/src/include/liburing/io_uring.h#L21
    private static final int SQE_OP_CODE_FIELD = 0;
    private static final int SQE_FLAGS_FIELD = 1;
    private static final int SQE_IOPRIO_FIELD = 2; // u16
    private static final int SQE_FD_FIELD = 4; // s32
    private static final int SQE_OFFSET_FIELD = 8;
    private static final int SQE_ADDRESS_FIELD = 16;
    private static final int SQE_LEN_FIELD = 24;
    private static final int SQE_RW_FLAGS_FIELD = 28;
    private static final int SQE_USER_DATA_FIELD = 32;
    private static final int SQE_PAD_FIELD = 40;

    private static final int KERNEL_TIMESPEC_TV_SEC_FIELD = 0;
    private static final int KERNEL_TIMESPEC_TV_NSEC_FIELD = 8;

    //these unsigned integer pointers(shared with the kernel) will be changed by the kernel
    private final long kHeadAddress;
    private final long kTailAddress;
    private final long fFlagsAdress;
    private final long kDroppedAddress;
    private final long arrayAddress;

    private final long submissionQueueArrayAddress;

    private final int ringEntries;
    private final int ringMask; // = ringEntries - 1

    private final int ringSize;
    private final long ringAddress;
    private final int ringFd;
    private final Runnable submissionCallback;

    private final long timeoutMemoryAddress;

    private int head;
    private int tail;

    private int ioInflightCount;

    IOUringSubmissionQueue(long kHeadAddress, long kTailAddress, long kRingMaskAddress, long kRingEntriesAddress,
                           long fFlagsAdress, long kDroppedAddress, long arrayAddress, long submissionQueueArrayAddress,
                           int ringSize, long ringAddress, int ringFd, Runnable submissionCallback) {
        this.kHeadAddress = kHeadAddress;
        this.kTailAddress = kTailAddress;
        this.fFlagsAdress = fFlagsAdress;
        this.kDroppedAddress = kDroppedAddress;
        this.arrayAddress = arrayAddress;
        this.submissionQueueArrayAddress = submissionQueueArrayAddress;
        this.ringSize = ringSize;
        this.ringAddress = ringAddress;
        this.ringFd = ringFd;
        this.submissionCallback = submissionCallback;
        this.ringEntries = PlatformDependent.getIntVolatile(kRingEntriesAddress);
        this.ringMask = PlatformDependent.getIntVolatile(kRingMaskAddress);
        this.head = PlatformDependent.getIntVolatile(kHeadAddress);
        this.tail = PlatformDependent.getIntVolatile(kTailAddress);

        this.timeoutMemoryAddress = PlatformDependent.allocateMemory(KERNEL_TIMESPEC_SIZE);

        // Zero the whole SQE array first
        PlatformDependent.setMemory(submissionQueueArrayAddress, ringEntries * SQE_SIZE, (byte) 0);

        // Fill SQ array indices (1-1 with SQE array) and set nonzero constant SQE fields
        long address = arrayAddress;
        long sqeFlagsAddress = submissionQueueArrayAddress + SQE_FLAGS_FIELD;
        for (int i = 0; i < ringEntries; i++, address += INT_SIZE, sqeFlagsAddress += SQE_SIZE) {
            PlatformDependent.putInt(address, i);
            // TODO: Make it configurable if we should use this flag or not.
            PlatformDependent.putByte(sqeFlagsAddress, (byte) Native.IOSQE_ASYNC);
        }
    }

    private boolean enqueueSqe(int op, int rwFlags, int fd,
            long bufferAddress, int length, long offset) {
        return enqueueSqe(op, rwFlags, fd, bufferAddress, length, offset, (short) 0);
    }

    private boolean enqueueFixedSqe(int op, int fd, long bufferAddress, int length, short bufIndex) {
        return enqueueSqe(op, 0, fd, bufferAddress, length, 0, bufIndex);
    }

    private boolean enqueueSqe(int op, int rwFlags, int fd,
            long bufferAddress, int length, long offset, short bufIndex) {
        int pending = tail - head;
        boolean submit = pending == ringEntries;
        if (submit) {
            int submitted = submit();
            if (submitted == 0) {
                // We have a problem, could not submit to make more room in the ring
                throw new RuntimeException("SQ ring full and no submissions accepted");
            }
        }
        long sqe = submissionQueueArrayAddress + (tail++ & ringMask) * SQE_SIZE;
        setData(sqe, op, rwFlags, fd, bufferAddress, length, offset, bufIndex);
        return submit;
    }

    private void setData(long sqe, int op, int rwFlags, int fd,
            long bufferAddress, int length, long offset, short bufIndex) {
        //set sqe(submission queue) properties

        PlatformDependent.putByte(sqe + SQE_OP_CODE_FIELD, (byte) op);
        // These two constants are set up-front
        //TODO maybe don't set async for timeout remove
        //PlatformDependent.putByte(sqe + SQE_FLAGS_FIELD, (byte) Native.IOSQE_ASYNC);
        //PlatformDependent.putShort(sqe + SQE_IOPRIO_FIELD, (short) 0);
        PlatformDependent.putInt(sqe + SQE_FD_FIELD, fd);
        PlatformDependent.putLong(sqe + SQE_OFFSET_FIELD, offset);
        PlatformDependent.putLong(sqe + SQE_ADDRESS_FIELD, bufferAddress);
        PlatformDependent.putInt(sqe + SQE_LEN_FIELD, length);
        PlatformDependent.putInt(sqe + SQE_RW_FLAGS_FIELD, rwFlags);

        // normalize completion op to read or write
        //TODO maybe simpler
        if (op == Native.IORING_OP_READ_FIXED) {
            op = Native.IORING_OP_READ;
        } else if (op == Native.IORING_OP_WRITE_FIXED | op == Native.IORING_OP_WRITEV) {
            op = Native.IORING_OP_WRITE;
        }
        long userData = convertToUserData(op, fd, rwFlags);
        PlatformDependent.putLong(sqe + SQE_USER_DATA_FIELD, userData);
        PlatformDependent.putShort(sqe + SQE_PAD_FIELD, bufIndex);

        logger.trace("UserDataField: {}", userData);
        logger.trace("BufferAddress: {}", bufferAddress);
        logger.trace("Length: {}", length);
        logger.trace("Offset: {}", offset);
    }

    public void addTimeout(long nanoSeconds) {
        setTimeout(nanoSeconds);
        enqueueSqe(Native.IORING_OP_TIMEOUT, 0, -1, timeoutMemoryAddress, 1, 0);
    }

    public void addTimeoutRemove() {
        enqueueSqe(Native.IORING_OP_TIMEOUT_REMOVE, 0, -1,
                convertToUserData(Native.IORING_OP_TIMEOUT, -1, 0), 0, 0);
    }

    public void addPollIn(int fd) {
        addPoll(fd, Native.POLLIN);
    }

    public void addPollRdHup(int fd) {
        addPoll(fd, Native.POLLRDHUP);
    }

    public void addPollOut(int fd) {
        addPoll(fd, Native.POLLOUT);
    }

    boolean addPoll(int fd, int pollMask) {
        return enqueueSqe(Native.IORING_OP_POLL_ADD, pollMask, fd, 0, 0, 0);
    }

    public void addRead(int fd, long bufferAddress, int pos, int limit, short bufIndex,
            boolean blocking) {
        if (bufIndex >= 0) {
            enqueueFixedSqe(Native.IORING_OP_READ_FIXED, fd,
                    bufferAddress + pos, limit - pos, bufIndex);
        } else {
            enqueueSqe(Native.IORING_OP_READ, 0, fd, bufferAddress + pos, limit - pos, 0);
        }
        if (!blocking) {
            ioInflightCount++;
        }
    }

    public void addWrite(int fd, long bufferAddress, int pos, int limit, short bufIndex) {
        if (bufIndex >= 0) {
            enqueueFixedSqe(Native.IORING_OP_WRITE_FIXED, fd,
                    bufferAddress + pos, limit - pos, bufIndex);
        } else {
            enqueueSqe(Native.IORING_OP_WRITE, 0, fd, bufferAddress + pos, limit - pos, 0);
        }
        ioInflightCount++;
    }

    public void addAccept(int fd) {
        enqueueSqe(Native.IORING_OP_ACCEPT, Native.SOCK_NONBLOCK | Native.SOCK_CLOEXEC, fd, 0, 0, 0);
        ioInflightCount++;
    }

    //fill the address which is associated with server poll link user_data
    public void addPollRemove(int fd, int pollMask) {
        enqueueSqe(Native.IORING_OP_POLL_REMOVE, 0, fd,
                convertToUserData(Native.IORING_OP_POLL_ADD, fd, pollMask), 0, 0);
    }

    public void addReadCancel(int fd) {
        enqueueSqe(Native.IORING_OP_ASYNC_CANCEL, 0, fd,
                convertToUserData(Native.IORING_OP_READ, fd, 0), 0, 0);
    }

    public void addConnect(int fd, long socketAddress, long socketAddressLength) {
        enqueueSqe(Native.IORING_OP_CONNECT, 0, fd, socketAddress, 0, socketAddressLength);
        ioInflightCount++;
    }

    public void addWritev(int fd, long iovecArrayAddress, int length) {
        enqueueSqe(Native.IORING_OP_WRITEV, 0, fd, iovecArrayAddress, length, 0);
        ioInflightCount++;
    }

    public void addClose(int fd) {
        enqueueSqe(Native.IORING_OP_CLOSE, 0, fd, 0, 0, 0);
    }

    public int submit() {
        int submit = tail - head;
        return submit > 0 ? submit(submit, 0, 0) : 0;
    }

    public int submitAndWait() {
        int submit = tail - head;
        if (submit > 0) {
            return submit(submit, 1, Native.IORING_ENTER_GETEVENTS);
        }
        assert submit == 0;
        int ret = Native.ioUringEnter(ringFd, 0, 1, Native.IORING_ENTER_GETEVENTS);
        if (ret < 0) {
            throw new RuntimeException("ioUringEnter syscall returned " + ret);
        }
        return ret; // should be 0
    }

    private int submit(int toSubmit, int minComplete, int flags) {
        PlatformDependent.putIntOrdered(kTailAddress, tail); // release memory barrier
        int ret = Native.ioUringEnter(ringFd, toSubmit, minComplete, flags);
        head = PlatformDependent.getIntVolatile(kHeadAddress); // acquire memory barrier
        if (ret != toSubmit) {
            if (ret < 0) {
                throw new RuntimeException("ioUringEnter syscall returned " + ret);
            }
            logger.warn("Not all submissions succeeded");
        }
        submissionCallback.run();
        return ret;
    }

    private void setTimeout(long timeoutNanoSeconds) {
        long seconds, nanoSeconds;

        //Todo
        if (timeoutNanoSeconds == 0) {
            seconds = 0;
            nanoSeconds = 0;
        } else {
            seconds = (int) min(timeoutNanoSeconds / 1000000000L, Integer.MAX_VALUE);
            nanoSeconds = (int) max(timeoutNanoSeconds - seconds * 1000000000L, 0);
        }

        PlatformDependent.putLong(timeoutMemoryAddress + KERNEL_TIMESPEC_TV_SEC_FIELD, seconds);
        PlatformDependent.putLong(timeoutMemoryAddress + KERNEL_TIMESPEC_TV_NSEC_FIELD, nanoSeconds);
    }

    private static long convertToUserData(int op, int fd, int pollMask) {
        int opMask = op << 16 | (pollMask & 0xFFFF);
        return (long) fd << 32 | opMask & 0xFFFFFFFFL;
    }

    public int pendingCount() {
        return tail - head;
    }

    //delete memory
    public void release() {
        PlatformDependent.freeMemory(timeoutMemoryAddress);
    }

    public int getRingFd() {
        return this.ringFd;
    }

    void ioOpComplete() {
        ioInflightCount--;
    }

    boolean ioInFlight() {
        return ioInflightCount > 0;
    }
}
