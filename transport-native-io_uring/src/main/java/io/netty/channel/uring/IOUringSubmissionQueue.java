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

import io.netty.channel.unix.Buffer;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.nio.ByteBuffer;

final class IOUringSubmissionQueue {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(IOUringSubmissionQueue.class);

    // This can be set smaller than the ring size to flush submissions more frequently.
    // It may make sense to use a different metric such as event loop iterations or
    // time elapsed.
    private static final int MAX_SUBMISSION_BATCH_SIZE = Integer.MAX_VALUE;

    private static final int IORING_ENTER_GETEVENTS = 1;

    private static final long SQE_SIZE = 64; //TODO sizeof
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
    private final long kRingMaskAddress;
    private final long kRingEntriesAddress;
    private final long fFlagsAdress;
    private final long kDroppedAddress;
    private final long arrayAddress;

    private final long submissionQueueArrayAddress;

    private final int ringMask;
    private int sqeSlots;
    private int sqeIndex;
    private int ringTail;
    private int ringHead;

    private final int ringSize;
    private final long ringAddress;
    private final int ringFd;

    private final ByteBuffer timeoutMemory;
    private final long timeoutMemoryAddress;

    //private int sqeSubmitCounter;

    IOUringSubmissionQueue(long kHeadAddress, long kTailAddress, long kRingMaskAddress, long kRingEntriesAddress,
                           long fFlagsAdress, long kDroppedAddress, long arrayAddress,
                           long submissionQueueArrayAddress, int ringSize,
                           long ringAddress, int ringFd) {
        this.kHeadAddress = kHeadAddress;
        this.kTailAddress = kTailAddress;
        this.kRingMaskAddress = kRingMaskAddress;
        this.kRingEntriesAddress = kRingEntriesAddress;
        this.fFlagsAdress = fFlagsAdress;
        this.kDroppedAddress = kDroppedAddress;
        this.arrayAddress = arrayAddress;
        this.submissionQueueArrayAddress = submissionQueueArrayAddress;
        this.ringSize = ringSize;
        this.ringAddress = ringAddress;
        this.ringFd = ringFd;

        this.ringMask = PlatformDependent.getInt(kRingMaskAddress);
        this.sqeSlots = PlatformDependent.getInt(kRingEntriesAddress);

        this.ringHead = PlatformDependent.getInt(kHeadAddress);
        this.ringTail = PlatformDependent.getInt(kTailAddress);

        timeoutMemory = Buffer.allocateDirectWithNativeOrder(KERNEL_TIMESPEC_SIZE);
        timeoutMemoryAddress = Buffer.memoryAddress(timeoutMemory);
        // Zero the whole array first
        PlatformDependent.setMemory(submissionQueueArrayAddress, sqeSlots * SQE_SIZE, (byte) 0);
    }

    // Reserve an entry at the end of the the sqe array. Other entries are filled from the start
    public int reserveSqe(int op, int pollMask, int fd, long bufferAddress, int length, long offset) {
        if (sqeSlots < 2) {
            throw new IllegalStateException();
        }
        int index = --sqeSlots;
        setData(index, op, pollMask, fd, bufferAddress, length, offset);
        return index;
    }

    private void setData(int sqeIndex, int op, int pollMask, int fd,
            long bufferAddress, int length, long offset) {
        setData(submissionQueueArrayAddress + SQE_SIZE * sqeIndex, op, pollMask, fd,
                bufferAddress, length, offset);
    }

    private void setData(long sqe, int op, int rwFlags, int fd, long bufferAddress, int length, long offset) {
        //set sqe(submission queue) properties
        // never-used fields are omitted since we zero the entire array up-front
        
        PlatformDependent.putByte(sqe + SQE_OP_CODE_FIELD, (byte) op);
        //PlatformDependent.putByte(sqe + SQE_FLAGS_FIELD, (byte) 0);
        //PlatformDependent.putShort(sqe + SQE_IOPRIO_FIELD, (short) 0);
        PlatformDependent.putInt(sqe + SQE_FD_FIELD, fd);
        PlatformDependent.putLong(sqe + SQE_OFFSET_FIELD, offset);
        PlatformDependent.putLong(sqe + SQE_ADDRESS_FIELD, bufferAddress);
        PlatformDependent.putInt(sqe + SQE_LEN_FIELD, length);
        PlatformDependent.putInt(sqe + SQE_RW_FLAGS_FIELD, rwFlags);
        long userData = convertToUserData(op, fd, rwFlags);
        PlatformDependent.putLong(sqe + SQE_USER_DATA_FIELD, userData);
        // pad field array -> all fields should be zero
//        long offsetIndex = 0;
//        for (int i = 0; i < 3; i++) {
//            PlatformDependent.putLong(sqe + SQE_PAD_FIELD + offsetIndex, 0);
//            offsetIndex += 8;
//        }

        logger.trace("UserDataField: {}", userData);
        logger.trace("BufferAddress: {}", bufferAddress);
        logger.trace("Length: {}", length);
        logger.trace("Offset: {}", offset);
    }

    public void addTimeout(long nanoSeconds) {
        setTimeout(nanoSeconds);
        enqueueSqe(Native.IORING_OP_TIMEOUT, 0, -1, timeoutMemoryAddress, 1, 0);
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

    private void addPoll(int fd, int pollMask) {
        enqueueSqe(Native.IORING_OP_POLL_ADD, pollMask, fd, 0, 0, 0);
    }

    void enqueueSqe(int op, int pollMask, int fd, long bufferAddress, int length, long offset) {
        if (sqeIndex == sqeSlots) {
            ioUringEnter(false); // no slots left
        }
        int index = sqeIndex++;
        setData(index, op, pollMask, fd, bufferAddress, length, offset);
        enqueueSqe(index, op == Native.IORING_OP_TIMEOUT);
    }

    public void enqueueReservedSqe(int index) {
        assert index >= sqeSlots; // && < entries
        enqueueSqe(index, false);
    }

    private void enqueueSqe(int index, boolean noflush) {
        PlatformDependent.putInt(arrayAddress + (ringTail++ & ringMask) * INT_SIZE, index);
        if (!noflush && count() >= MAX_SUBMISSION_BATCH_SIZE) {
            ioUringEnter(false); // max submission threshold
        }
    }

    public void addRead(int fd, long bufferAddress, int pos, int limit) {
        enqueueSqe(Native.IORING_OP_READ, 0, fd, bufferAddress + pos, limit - pos, 0);
    }

    public void addWrite(int fd, long bufferAddress, int pos, int limit) {
        enqueueSqe(Native.IORING_OP_WRITE, 0, fd, bufferAddress + pos, limit - pos, 0);
    }

    public void addAccept(int fd) {
        enqueueSqe(Native.IORING_OP_ACCEPT, Native.SOCK_NONBLOCK | Native.SOCK_CLOEXEC, fd, 0, 0, 0);
    }

    //fill the address which is associated with the poll add user_data
    public void addPollRemove(int fd, int pollMask) {
        enqueueSqe(Native.IORING_OP_POLL_REMOVE, 0, fd,
                convertToUserData(Native.IORING_OP_POLL_ADD, fd, pollMask), 0, 0);
    }

    public void addConnect(int fd, long socketAddress, long socketAddressLength) {
        enqueueSqe(Native.IORING_OP_CONNECT, 0, fd, socketAddress, 0, socketAddressLength);
    }

    public void addWritev(int fd, long iovecArrayAddress, int length) {
        enqueueSqe(Native.IORING_OP_WRITEV, 0, fd, iovecArrayAddress, length, 0);
    }

    public void ioUringEnter(boolean getEvents) {
        int submit = count();
        if (submit != 0) {
            PlatformDependent.putIntOrdered(kTailAddress, ringTail);
        }
        int ret = getEvents
                ? Native.ioUringEnter(ringFd, submit, 1, IORING_ENTER_GETEVENTS)
                : Native.ioUringEnter(ringFd, submit, 0, 0);
        if (ret < 0) {
            throw new RuntimeException("ioUringEnter syscall ret=" + ret);
        }
        ringHead += ret;
        sqeIndex = 0;
        if (ret != submit) {
            //TODO handle this
            throw new RuntimeException("Unexpected - submitted "
                    + submit + ", conusmed " + ret);
        }
    }

    private void setTimeout(long timeoutNanoSeconds) {
        long seconds, nanoSeconds;

        //Todo
        if (timeoutNanoSeconds == 0) {
            seconds = 0;
            nanoSeconds = 0;
        } else {
            seconds = timeoutNanoSeconds / 1000000000L;
            nanoSeconds = timeoutNanoSeconds % 1000;
        }

        PlatformDependent.putLong(timeoutMemoryAddress + KERNEL_TIMESPEC_TV_SEC_FIELD, seconds);
        PlatformDependent.putLong(timeoutMemoryAddress + KERNEL_TIMESPEC_TV_NSEC_FIELD, nanoSeconds);
    }

    private static long convertToUserData(int op, int fd, int pollMask) {
        int opMask = (op << 16) | (pollMask & 0xFFFF);
        return (long) fd << 32 | opMask & 0xFFFFFFFFL;
    }

    public int count() {
        return ringTail - ringHead;
    }

    //delete memory
    public void release() {
        Buffer.free(timeoutMemory);
    }

    public long getKHeadAddress() {
        return this.kHeadAddress;
    }

    public long getKTailAddress() {
        return this.kTailAddress;
    }

    public long getKRingMaskAddress() {
        return this.kRingMaskAddress;
    }

    public long getKRingEntriesAddress() {
        return this.kRingEntriesAddress;
    }

    public long getFFlagsAdress() {
        return this.fFlagsAdress;
    }

    public long getKDroppedAddress() {
        return this.kDroppedAddress;
    }

    public long getArrayAddress() {
        return this.arrayAddress;
    }

    public long getSubmissionQueueArrayAddress() {
        return this.submissionQueueArrayAddress;
    }

    public int getRingFd() {
        return ringFd;
    }

    public int getRingSize() {
        return this.ringSize;
    }

    public long getRingAddress() {
        return this.ringAddress;
    }

    public static long getUInt(long address) {
        return PlatformDependent.getInt(address) & 0xffffffffL;
    }

    public static long getUIntVolatile(long address) {
        return PlatformDependent.getIntVolatile(address) & 0xffffffffL;
    }
}