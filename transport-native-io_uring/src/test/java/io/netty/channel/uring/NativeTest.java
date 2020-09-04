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

import io.netty.channel.unix.FileDescriptor;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

import io.netty.buffer.ByteBuf;

public class NativeTest {

    @BeforeClass
    public static void loadJNI() {
        assumeTrue(IOUring.isAvailable());
    }

    @Test
    public void canWriteFile() throws Exception {
        ByteBufAllocator allocator = new UnpooledByteBufAllocator(true);
        final ByteBuf writeEventByteBuf = allocator.directBuffer(100);
        final String inputString = "Hello World!";
        writeEventByteBuf.writeCharSequence(inputString, Charset.forName("UTF-8"));

        int fd = Native.createFile();

        RingBuffer ringBuffer = Native.createRingBuffer(32);
        IOUringSubmissionQueue submissionQueue = ringBuffer.getIoUringSubmissionQueue();
        IOUringCompletionQueue completionQueue = ringBuffer.getIoUringCompletionQueue();

        assertNotNull(ringBuffer);
        assertNotNull(submissionQueue);
        assertNotNull(completionQueue);

        submissionQueue.addWrite(fd, writeEventByteBuf.memoryAddress(),
                                            writeEventByteBuf.readerIndex(), writeEventByteBuf.writerIndex());
        submissionQueue.ioUringEnter(false);

        submissionQueue.ioUringEnter(true);
        assertEquals(1, completionQueue.process(new IOUringCompletionQueue.IOUringCompletionQueueCallback() {
            @Override
            public boolean handle(int fd, int res, int flags, int op, int mask) {
                assertEquals(inputString.length(), res);
                writeEventByteBuf.release();
                return true;
            }
        }));

        final ByteBuf readEventByteBuf = allocator.directBuffer(100);
        submissionQueue.addRead(fd, readEventByteBuf.memoryAddress(),
                                           readEventByteBuf.writerIndex(), readEventByteBuf.capacity());
        submissionQueue.ioUringEnter(false);

        submissionQueue.ioUringEnter(true);
        assertEquals(1, completionQueue.process(new IOUringCompletionQueue.IOUringCompletionQueueCallback() {
            @Override
            public boolean handle(int fd, int res, int flags, int op, int mask) {
                assertEquals(inputString.length(), res);
                readEventByteBuf.writerIndex(res);
                return true;
            }
        }));
        byte[] dataRead = new byte[inputString.length()];
        readEventByteBuf.readBytes(dataRead);

        assertArrayEquals(inputString.getBytes(), dataRead);
        readEventByteBuf.release();

        ringBuffer.close();
    }

    @Test
    public void timeoutTest() throws Exception {

        RingBuffer ringBuffer = Native.createRingBuffer(32);
        final IOUringSubmissionQueue submissionQueue = ringBuffer.getIoUringSubmissionQueue();
        final IOUringCompletionQueue completionQueue = ringBuffer.getIoUringCompletionQueue();

        assertNotNull(ringBuffer);
        assertNotNull(submissionQueue);
        assertNotNull(completionQueue);

        Thread thread = new Thread() {
            @Override
            public void run() {
                submissionQueue.ioUringEnter(true);
                try {
                    completionQueue.process(new IOUringCompletionQueue.IOUringCompletionQueueCallback() {
                        @Override
                        public boolean handle(int fd, int res, int flags, int op, int mask) {
                            assertEquals(-62, res);
                            return true;
                        }
                    });
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        thread.start();
        try {
            Thread.sleep(80);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        submissionQueue.addTimeout(0);
        submissionQueue.ioUringEnter(false);

        thread.join();
        ringBuffer.close();
    }

    //Todo clean
    @Test
    public void eventfdTest() throws Exception {
        RingBuffer ringBuffer = Native.createRingBuffer(32);
        IOUringSubmissionQueue submissionQueue = ringBuffer.getIoUringSubmissionQueue();
        final IOUringCompletionQueue completionQueue = ringBuffer.getIoUringCompletionQueue();

        assertNotNull(ringBuffer);
        assertNotNull(submissionQueue);
        assertNotNull(completionQueue);

        final FileDescriptor eventFd = Native.newEventFd();
        submissionQueue.addPollIn(eventFd.intValue());
        submissionQueue.ioUringEnter(false);

        new Thread() {
            @Override
            public void run() {
                Native.eventFdWrite(eventFd.intValue(), 1L);
            }
        }.start();

        submissionQueue.ioUringEnter(true);
        assertEquals(1, completionQueue.process(new IOUringCompletionQueue.IOUringCompletionQueueCallback() {
            @Override
            public boolean handle(int fd, int res, int flags, int op, int mask) {
                assertEquals(1, res);
                return true;
            }
        }));
        try {
            eventFd.close();
        } finally {
            ringBuffer.close();
        }
    }

    //Todo clean
    //eventfd signal doesnt work when ioUringWaitCqe and eventFdWrite are executed in a thread
    //created this test to reproduce this "weird" bug
    @Test(timeout = 8000)
    public void eventfdNoSignal() throws Exception {

        RingBuffer ringBuffer = Native.createRingBuffer(32);
        final IOUringSubmissionQueue submissionQueue = ringBuffer.getIoUringSubmissionQueue();
        final IOUringCompletionQueue completionQueue = ringBuffer.getIoUringCompletionQueue();

        assertNotNull(ringBuffer);
        assertNotNull(submissionQueue);
        assertNotNull(completionQueue);

        Thread waitingCqe = new Thread() {
            @Override
            public void run() {
                submissionQueue.ioUringEnter(true);
                assertEquals(1, completionQueue.process(new IOUringCompletionQueue.IOUringCompletionQueueCallback() {
                    @Override
                    public boolean handle(int fd, int res, int flags, int op, int mask) {
                        assertEquals(1, res);
                        return true;
                    }
                }));
            }
        };
        waitingCqe.start();
        final FileDescriptor eventFd = Native.newEventFd();
        submissionQueue.addPollIn(eventFd.intValue());
        submissionQueue.ioUringEnter(false);

        new Thread() {
            @Override
            public void run() {
                Native.eventFdWrite(eventFd.intValue(), 1L);
            }
        }.start();

        waitingCqe.join();

        ringBuffer.close();
    }

    @Test
    public void ioUringExitTest() {
        RingBuffer ringBuffer = Native.createRingBuffer();
        ringBuffer.close();
    }

    @Test
    public void ioUringPollRemoveTest() throws Exception {
        RingBuffer ringBuffer = Native.createRingBuffer(32);
        final IOUringSubmissionQueue submissionQueue = ringBuffer.getIoUringSubmissionQueue();
        final IOUringCompletionQueue completionQueue = ringBuffer.getIoUringCompletionQueue();

        FileDescriptor eventFd = Native.newEventFd();
        submissionQueue.addPollIn(eventFd.intValue());
        submissionQueue.ioUringEnter(false);
        submissionQueue.addPollRemove(eventFd.intValue(), Native.POLLIN);
        submissionQueue.ioUringEnter(false);

        final AtomicReference<AssertionError> errorRef = new AtomicReference<AssertionError>();
        Thread waitingCqe = new Thread() {
            private final IOUringCompletionQueue.IOUringCompletionQueueCallback verifyCallback =
                    new IOUringCompletionQueue.IOUringCompletionQueueCallback() {
                @Override
                public boolean handle(int fd, int res, int flags, int op, int mask) {
                    if (op == Native.IORING_OP_POLL_ADD) {
                        assertEquals(IOUringEventLoop.ECANCELED, res);
                    } else if (op == Native.IORING_OP_POLL_REMOVE) {
                        assertEquals(0, res);
                    } else {
                        fail("op " + op);
                    }
                    return false;
                }
            };

            @Override
            public void run() {
                try {
                    submissionQueue.ioUringEnter(true);
                    assertEquals(1, completionQueue.process(verifyCallback));
                    submissionQueue.ioUringEnter(true);
                    assertEquals(1, completionQueue.process(verifyCallback));
                } catch (AssertionError error) {
                    errorRef.set(error);
                }
            }
        };
        waitingCqe.start();
        waitingCqe.join();
        try {
            eventFd.close();
            AssertionError error = errorRef.get();
            if (error != null) {
                throw error;
            }
        } finally {
            ringBuffer.close();
        }
    }
}
