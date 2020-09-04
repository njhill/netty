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

import io.netty.channel.EventLoopGroup;
import io.netty.channel.SingleThreadEventLoop;
import io.netty.channel.unix.Errors;
import io.netty.channel.unix.FileDescriptor;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

final class IOUringEventLoop extends SingleThreadEventLoop implements
                                                           IOUringCompletionQueue.IOUringCompletionQueueCallback {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(IOUringEventLoop.class);

    private static final long efdReadBuf = PlatformDependent.allocateMemory(8);

    //Todo set config ring buffer size
    private static final int ringSize = 32;
    private static final long ETIME = -62;
    static final long ECANCELED = -125;

    private final IntObjectMap<AbstractIOUringChannel> channels = new IntObjectHashMap<AbstractIOUringChannel>(4096);
    private final RingBuffer ringBuffer;

    private static final long AWAKE = -1L;
    private static final long NONE = Long.MAX_VALUE;

    // nextWakeupNanos is:
    //    AWAKE            when EL is awake
    //    NONE             when EL is waiting with no wakeup scheduled
    //    other value T    when EL is waiting with wakeup scheduled at time T
    private final AtomicLong nextWakeupNanos = new AtomicLong(AWAKE);
    private final FileDescriptor eventfd;
    private final IovecArrayPool iovecArrayPool;
    
    private int eventfdSqe;

    private long prevDeadlineNanos = NONE;
    private boolean pendingWakeup;

    IOUringEventLoop(final EventLoopGroup parent, final Executor executor, final boolean addTaskWakesUp) {
        super(parent, executor, addTaskWakesUp);

        ringBuffer = Native.createRingBuffer(ringSize);
        eventfd = Native.newEventFd();
        logger.trace("New EventLoop: {}", this.toString());
        iovecArrayPool = new IovecArrayPool();
    }

    @Override
    protected Queue<Runnable> newTaskQueue(int maxPendingTasks) {
        return newTaskQueue0(maxPendingTasks);
    }

    private static Queue<Runnable> newTaskQueue0(int maxPendingTasks) {
        // This event loop never calls takeTask()
        return maxPendingTasks == Integer.MAX_VALUE? PlatformDependent.<Runnable>newMpscQueue()
                : PlatformDependent.<Runnable>newMpscQueue(maxPendingTasks);
    }

    public void add(AbstractIOUringChannel ch) {
        logger.trace("Add Channel: {} ", ch.socket.intValue());
        int fd = ch.socket.intValue();

        channels.put(fd, ch);
    }

    public void remove(AbstractIOUringChannel ch) {
        logger.trace("Remove Channel: {}", ch.socket.intValue());
        int fd = ch.socket.intValue();

        AbstractIOUringChannel old = channels.remove(fd);
        if (old != null && old != ch) {
            // The Channel mapping was already replaced due FD reuse, put back the stored Channel.
            channels.put(fd, old);

            // If we found another Channel in the map that is mapped to the same FD the given Channel MUST be closed.
            assert !ch.isOpen();
        }
    }

    private void closeAll() {
        logger.trace("CloseAll IOUringEvenloop");
        // Using the intermediate collection to prevent ConcurrentModificationException.
        // In the `close()` method, the channel is deleted from `channels` map.
        AbstractIOUringChannel[] localChannels = channels.values().toArray(new AbstractIOUringChannel[0]);

        for (AbstractIOUringChannel ch : localChannels) {
            ch.unsafe().close(ch.unsafe().voidPromise());
        }
    }

    @Override
    protected void run() {
        final IOUringCompletionQueue completionQueue = ringBuffer.getIoUringCompletionQueue();
        final IOUringSubmissionQueue submissionQueue = ringBuffer.getIoUringSubmissionQueue();

        // Lets add the eventfd related events before starting to do any real work.
        eventfdSqe = submissionQueue.reserveSqe(Native.IORING_OP_READ, 0, eventfd.intValue(), efdReadBuf, 8, 0);
        submissionQueue.addReservedSqe(eventfdSqe);

        for (;;) {
            for (;;) {
                // userspace loop
                completionQueue.process(this);
                boolean ranTasks = runAllTasks();
                if (!ranTasks) {
                    break;
                }
            }

            try {
                if (isShuttingDown()) {
                    closeAll();
                    if (confirmShutdown()) {
                        break;
                    }
                }
            } catch (Throwable t) {
                logger.info("Exception error: {}", t);
            }

            logger.trace("Run IOUringEventLoop {}", this.toString());
            long curDeadlineNanos = nextScheduledTaskDeadlineNanos();
            if (curDeadlineNanos == -1L) {
                curDeadlineNanos = NONE; // nothing on the calendar
            }
            nextWakeupNanos.set(curDeadlineNanos);

            // Only submit a timeout if there are no tasks to process and do a blocking operation
            // on the completionQueue.
            if (!hasTasks()) {
                try {
                    if (curDeadlineNanos != prevDeadlineNanos) {
                        prevDeadlineNanos = curDeadlineNanos;
                        submissionQueue.addTimeout(deadlineToDelayNanos(curDeadlineNanos));
                    }

                    // Check there were any completion events to process
                    if (!completionQueue.hasCompletions()) {
                        // Block if there is nothing to process after this try again to call process(....)
                        logger.trace("ioUringWaitCqe {}", this.toString());
                        submissionQueue.ioUringEnter(true);
                    }
                } catch (Throwable t) {
                    //Todo handle exception
                } finally {
                    if (nextWakeupNanos.get() == AWAKE || nextWakeupNanos.getAndSet(AWAKE) == AWAKE) {
                        pendingWakeup = true;
                    }
                }
            }
        }
    }

    @Override
    public boolean handle(int fd, int res, int flags, int op, int pollMask) {
        IOUringSubmissionQueue submissionQueue = ringBuffer.getIoUringSubmissionQueue();
        if (op == Native.IORING_OP_READ || op == Native.IORING_OP_ACCEPT) {
            if (eventfd.intValue() == fd) {
                pendingWakeup = false;
                submissionQueue.addReservedSqe(eventfdSqe);
            } else {
                handleRead(fd, res);
            }
        } else if (op == Native.IORING_OP_WRITEV || op == Native.IORING_OP_WRITE) {
            handleWrite(fd, res);
        } else if (op == Native.IORING_OP_POLL_ADD) {
            if (res == ECANCELED) {
                logger.trace("IORING_POLL_ADD cancelled");
                return true;
            }
            handlePollAdd(fd, res, pollMask);
        } else if (op == Native.IORING_OP_POLL_REMOVE) {
            if (res == Errors.ERRNO_ENOENT_NEGATIVE) {
                logger.trace("IORING_POLL_REMOVE not successful");
            } else if (res == 0) {
                logger.trace("IORING_POLL_REMOVE successful");
            }
        } else if (op == Native.IORING_OP_CONNECT) {
            handleConnect(fd, res);
        } else if (op == Native.IORING_OP_TIMEOUT) {
            if (res == ETIME) {
                prevDeadlineNanos = NONE;
            }
        }

        return true;
    }

    private void handleRead(int fd, int res) {
        AbstractIOUringChannel readChannel = channels.get(fd);
        if (readChannel != null) {
            ((AbstractIOUringChannel.AbstractUringUnsafe) readChannel.unsafe()).readComplete(res);
        }
    }

    private void handleWrite(int fd, int res) {
        AbstractIOUringChannel writeChannel = channels.get(fd);
        if (writeChannel != null) {
            ((AbstractIOUringChannel.AbstractUringUnsafe) writeChannel.unsafe()).writeComplete(res);
        }
    }

    private void handlePollAdd(int fd, int res, int pollMask) {
        AbstractIOUringChannel channel = channels.get(fd);
        if (channel != null) {
            if ((pollMask & Native.POLLOUT) != 0) {
                ((AbstractIOUringChannel.AbstractUringUnsafe) channel.unsafe()).pollOut(res);
            }
            if ((pollMask & Native.POLLIN) != 0) {
                ((AbstractIOUringChannel.AbstractUringUnsafe) channel.unsafe()).pollIn(res);
            }
            if ((pollMask & Native.POLLRDHUP) != 0) {
                ((AbstractIOUringChannel.AbstractUringUnsafe) channel.unsafe()).pollRdHup(res);
            }
        }
    }

    private void handleConnect(int fd, int res) {
        AbstractIOUringChannel channel = channels.get(fd);
        if (channel != null) {
            ((AbstractIOUringChannel.AbstractUringUnsafe) channel.unsafe()).connectComplete(res);
        }
    }

    @Override
    protected void cleanup() {
        try {
            eventfd.close();
        } catch (IOException e) {
            logger.warn("Failed to close the event fd.", e);
        }
        ringBuffer.close();
        iovecArrayPool.release();
    }

    public RingBuffer getRingBuffer() {
        return ringBuffer;
    }

    @Override
    protected void wakeup(boolean inEventLoop) {
        if (!inEventLoop && nextWakeupNanos.getAndSet(AWAKE) != AWAKE) {
            // write to the evfd which will then wake-up epoll_wait(...)
            Native.eventFdWrite(eventfd.intValue(), 1L);
        }
    }

    public IovecArrayPool getIovecArrayPool() {
        return iovecArrayPool;
    }
}
