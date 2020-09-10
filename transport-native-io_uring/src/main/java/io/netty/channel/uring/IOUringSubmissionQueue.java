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

    private boolean enqueueSqe(int op, int rwFlags, int fd, long bufferAddress, int length, long offset) {
        boolean submitted = false;
        int pending = tail - head;
        if (pending == ringEntries) {
            submit();
            submitted = true;
        }
        long sqe = submissionQueueArrayAddress + (tail++ & ringMask) * SQE_SIZE;
        setData(sqe, op, rwFlags, fd, bufferAddress, length, offset);
        return submitted;
    }

    private void setData(long sqe, int op, int rwFlags, int fd, long bufferAddress, int length, long offset) {
        //set sqe(submission queue) properties

        PlatformDependent.putByte(sqe + SQE_OP_CODE_FIELD, (byte) op);
        // These two constants are set up-front
        //PlatformDependent.putByte(sqe + SQE_FLAGS_FIELD, (byte) Native.IOSQE_ASYNC);
        //PlatformDependent.putShort(sqe + SQE_IOPRIO_FIELD, (short) 0);
        PlatformDependent.putInt(sqe + SQE_FD_FIELD, fd);
        PlatformDependent.putLong(sqe + SQE_OFFSET_FIELD, offset);
        PlatformDependent.putLong(sqe + SQE_ADDRESS_FIELD, bufferAddress);
        PlatformDependent.putInt(sqe + SQE_LEN_FIELD, length);
        PlatformDependent.putInt(sqe + SQE_RW_FLAGS_FIELD, rwFlags);
        long userData = convertToUserData(op, fd, rwFlags);
        PlatformDependent.putLong(sqe + SQE_USER_DATA_FIELD, userData);

        logger.trace("UserDataField: {}", userData);
        logger.trace("BufferAddress: {}", bufferAddress);
        logger.trace("Length: {}", length);
        logger.trace("Offset: {}", offset);
    }

    public boolean addTimeout(long nanoSeconds) {
        setTimeout(nanoSeconds);
        return enqueueSqe(Native.IORING_OP_TIMEOUT, 0, -1, timeoutMemoryAddress, 1, 0);
    }

    public boolean addPollIn(int fd) {
        return addPoll(fd, Native.POLLIN);
    }

    public boolean addPollRdHup(int fd) {
        return addPoll(fd, Native.POLLRDHUP);
    }

    public boolean addPollOut(int fd) {
        return addPoll(fd, Native.POLLOUT);
    }

    private boolean addPoll(int fd, int pollMask) {
        return enqueueSqe(Native.IORING_OP_POLL_ADD, pollMask, fd, 0, 0, 0);
    }

    //return true -> submit() was called
    public boolean addRead(int fd, long bufferAddress, int pos, int limit) {
        return enqueueSqe(Native.IORING_OP_READ, 0, fd, bufferAddress + pos, limit - pos, 0);
    }

    public boolean addWrite(int fd, long bufferAddress, int pos, int limit) {
        return enqueueSqe(Native.IORING_OP_WRITE, 0, fd, bufferAddress + pos, limit - pos, 0);
    }

    public boolean addAccept(int fd) {
        return enqueueSqe(Native.IORING_OP_ACCEPT, Native.SOCK_NONBLOCK | Native.SOCK_CLOEXEC, fd, 0, 0, 0);
    }

    //fill the address which is associated with server poll link user_data
    public boolean addPollRemove(int fd, int pollMask) {
        return enqueueSqe(Native.IORING_OP_POLL_REMOVE, 0, fd,
                convertToUserData(Native.IORING_OP_POLL_ADD, fd, pollMask), 0, 0);
    }

    public boolean addConnect(int fd, long socketAddress, long socketAddressLength) {
        return enqueueSqe(Native.IORING_OP_CONNECT, 0, fd, socketAddress, 0, socketAddressLength);
    }

    public boolean addWritev(int fd, long iovecArrayAddress, int length) {
        return enqueueSqe(Native.IORING_OP_WRITEV, 0, fd, iovecArrayAddress, length, 0);
    }

    public boolean addClose(int fd) {
        return enqueueSqe(Native.IORING_OP_CLOSE, 0, fd, 0, 0, 0);
    }

    public void submit() {
        int submit = tail - head;
        if (submit > 0) {
            submit(submit, 0, 0);
        }
    }

    public void submitAndWait() {
        int submit = tail - head;
        if (submit > 0) {
            submit(submit, 1, Native.IORING_ENTER_GETEVENTS);
        } else {
            int ret = Native.ioUringEnter(ringFd, 0, 1, Native.IORING_ENTER_GETEVENTS);
            if (ret < 0) {
                throw new RuntimeException("ioUringEnter syscall");
            }
        }
    }

    public void submit(int toSubmit, int minComplete, int flags) {
        PlatformDependent.putIntOrdered(kTailAddress, tail); // release memory barrier
        int ret = Native.ioUringEnter(ringFd, toSubmit, minComplete, flags);
        head = PlatformDependent.getIntVolatile(kHeadAddress); // acquire memory barrier
        if (ret != toSubmit) {
            if (ret < 0) {
                throw new RuntimeException("ioUringEnter syscall");
            }
            logger.warn("Not all submissions succeeded");
        }
        submissionCallback.run();
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

    public long count() {
        return tail - head;
    }

    //delete memory
    public void release() {
        PlatformDependent.freeMemory(timeoutMemoryAddress);
    }

    public int getRingEntries() {
        return this.ringEntries;
    }

    public long getArrayAddress() {
        return this.arrayAddress;
    }

    public long getSubmissionQueueArrayAddress() {
        return this.submissionQueueArrayAddress;
    }

    public int getRingFd() {
        return this.ringFd;
    }

    public int getRingSize() {
        return this.ringSize;
    }

    public long getRingAddress() {
        return this.ringAddress;
    }

}
