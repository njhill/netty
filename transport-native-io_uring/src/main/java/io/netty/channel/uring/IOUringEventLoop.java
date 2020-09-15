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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.EventLoopTaskQueueFactory;
import io.netty.channel.SingleThreadEventLoop;
import io.netty.channel.unix.Errors;
import io.netty.channel.unix.FileDescriptor;
import io.netty.channel.unix.IovArray;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import io.netty.util.concurrent.RejectedExecutionHandler;
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

    private final ByteBuf eventfdReadBuf;
    private final long eventfdReadAddress;

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

    private final IovArrays iovArrays;

    private final FixedBufferTracker tracker;
    private boolean buffersRegistered;

    private long prevDeadlineNanos = NONE;
    private boolean pendingWakeup;

    IOUringEventLoop(IOUringEventLoopGroup parent, Executor executor, int ringSize,
                     RejectedExecutionHandler rejectedExecutionHandler, EventLoopTaskQueueFactory queueFactory) {
        super(parent, executor, false, newTaskQueue(queueFactory), newTaskQueue(queueFactory),
                rejectedExecutionHandler);
        // Ensure that we load all native bits as otherwise it may fail when try to use native methods in IovArray
        IOUring.ensureAvailability();

        // TODO: Let's hard code this to 8 IovArrays to keep the memory overhead kind of small. We may want to consider
        //       allow to change this in the future.
        iovArrays = new IovArrays(8);
        ringBuffer = Native.createRingBuffer(ringSize, new Runnable() {
            @Override
            public void run() {
                // Once we submitted its safe to clear the IovArrays and so be able to re-use these.
                iovArrays.clear();
            }
        });

        eventfd = Native.newBlockingEventFd();

        PooledByteBufAllocator alloc = PooledByteBufAllocator.DEFAULT; //TODO configurable

        ByteBuf buf = null;
        if (alloc != null) {
            buf = PooledByteBufAllocator.DEFAULT.directBuffer(8);
            if (!buf.hasMemoryAddress()) {
                buf.release();
                buf = null;
            }
            tracker = new FixedBufferTracker(this, alloc);
        } else {
            tracker = FixedBufferTracker.NOOP_TRACKER;
        }
        eventfdReadBuf = buf;
        eventfdReadAddress = buf != null ? buf.memoryAddress() : PlatformDependent.allocateMemory(8);

        logger.trace("New EventLoop: {}", this.toString());
    }

    // returns -1 for none
    short getFixedBufferIndex(ByteBuf buf) {
        return tracker.getIndex(ByteBufUtil.getPooledMemoryAddress(buf));
    }

    private static Queue<Runnable> newTaskQueue(
            EventLoopTaskQueueFactory queueFactory) {
        if (queueFactory == null) {
            return newTaskQueue0(DEFAULT_MAX_PENDING_TASKS);
        }
        return queueFactory.newTaskQueue(DEFAULT_MAX_PENDING_TASKS);
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

        tracker.start();
        if (tracker.isDirty()) {
            registerFixedBuffers(submissionQueue.getRingFd());
        }

        // Lets add the eventfd related events before starting to do any real work.
        addEventFdRead(submissionQueue);

        for (;;) {
            try {
                logger.trace("Run IOUringEventLoop {}", this);

                // Avoid blocking for as long as possible - loop until available work exhausted
                boolean maybeMoreWork = true;
                do {
                    try {
                        // CQE processing can produce tasks, and new CQEs could arrive while
                        // processing tasks. So run both on every iteration and break when
                        // they both report that nothing was done (| means always run both).
                        maybeMoreWork = completionQueue.process(this) != 0 | runAllTasks();
                    } catch (Throwable t) {
                        handleLoopException(t);
                    }
                    // Always handle shutdown even if the loop processing threw an exception
                    try {
                        if (isShuttingDown()) {
                            closeAll();
                            if (confirmShutdown()) {
                                tracker.close();
                                return;
                            }
                            if (!maybeMoreWork) {
                                maybeMoreWork = hasTasks() || completionQueue.hasCompletions();
                            }
                        }
                    } catch (Throwable t) {
                        handleLoopException(t);
                    }
                } while (maybeMoreWork);

                if (tracker.isDirty() && !submissionQueue.ioInFlight()) {
                    pauseLongIo(submissionQueue);
                    registerFixedBuffers(submissionQueue.getRingFd());
                    continue; // might have more work by now
                }

                // Prepare to block wait
                long curDeadlineNanos = nextScheduledTaskDeadlineNanos();
                if (curDeadlineNanos == -1L) {
                    curDeadlineNanos = NONE; // nothing on the calendar
                }
                nextWakeupNanos.set(curDeadlineNanos);

                // Only submit a timeout if there are no tasks to process and do a blocking operation
                // on the completionQueue.
                try {
                    if (!hasTasks()) {
                        if (curDeadlineNanos != prevDeadlineNanos) {
                            if (prevDeadlineNanos != NONE) {
                                submissionQueue.addTimeoutRemove(); //TODO batch the removals
                            }
                            if (curDeadlineNanos != NONE) {
                                submissionQueue.addTimeout(deadlineToDelayNanos(curDeadlineNanos));
                            }
                            prevDeadlineNanos = curDeadlineNanos;
                        }

                        // Check there were any completion events to process
                        if (!completionQueue.hasCompletions()) {
                            // Block if there is nothing to process after this try again to call process(....)
                            logger.trace("submitAndWait {}", this);
                            submissionQueue.submitAndWait();
                        }
                    }
                } finally {
                    if (nextWakeupNanos.get() == AWAKE || nextWakeupNanos.getAndSet(AWAKE) == AWAKE) {
                        pendingWakeup = true;
                    }
                }
            } catch (Throwable t) {
                handleLoopException(t);
            }
        }
    }

    /**
     * Visible only for testing!
     */
    void handleLoopException(Throwable t) {
        logger.warn("Unexpected exception in the io_uring event loop", t);

        // Prevent possible consecutive immediate failures that lead to
        // excessive CPU consumption.
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // Ignore.
        }
    }

    @Override
    public void handle(int fd, int res, int flags, int op, int pollMask) {
        IOUringSubmissionQueue submissionQueue = ringBuffer.getIoUringSubmissionQueue();

        if (op == Native.IORING_OP_READ && eventfd.intValue() == fd) {
            //TODO not if loop is shutting down
            pendingWakeup = false;
            addEventFdRead(submissionQueue);
        } else if (op == Native.IORING_OP_TIMEOUT) {
            if (res == Native.ERRNO_ETIME_NEGATIVE) {
                prevDeadlineNanos = NONE;
            }
        } else {
            // Remaining events should be channel-specific
            final AbstractIOUringChannel channel = channels.get(fd);
            if (channel == null) {
                return;
            }
            if (op == Native.IORING_OP_READ || op == Native.IORING_OP_ACCEPT) {
                submissionQueue.ioOpComplete();
                handleRead(channel, res); // also includes READ_FIXED
            } else if (op == Native.IORING_OP_WRITE) {
                submissionQueue.ioOpComplete();
                handleWrite(channel, res); // also includes WRITEV and WRITE_FIXED
            } else if (op == Native.IORING_OP_POLL_ADD) {
                if (res != Native.ERRNO_ECANCELED_NEGATIVE) {
                    handlePollAdd(channel, res, pollMask);
                } else if (channel.isActive()) {
                    // reinstate poll following register pause
                    ringBuffer.getIoUringSubmissionQueue().addPoll(fd, pollMask);
                }
            } else if (op == Native.IORING_OP_POLL_REMOVE) {
                if (res == Errors.ERRNO_ENOENT_NEGATIVE) {
                    logger.trace("IORING_POLL_REMOVE not successful");
                } else if (res == 0) {
                    logger.trace("IORING_POLL_REMOVE successful");
                }
                if (!channel.isActive() && !channel.ioScheduled()) {
                    // We cancelled the POLL ops which means we are done and should remove the mapping.
                    channels.remove(fd);
                    return;
                }
            } else if (op == Native.IORING_OP_CONNECT) {
                submissionQueue.ioOpComplete();
                handleConnect(channel, res);
            }
            channel.ioUringUnsafe().processDelayedClose();
        }
    }

    private void handleRead(AbstractIOUringChannel channel, int res) {
        channel.ioUringUnsafe().readComplete(res);
    }

    private void handleWrite(AbstractIOUringChannel channel, int res) {
        channel.ioUringUnsafe().writeComplete(res);
    }

    private void handlePollAdd(AbstractIOUringChannel channel, int res, int pollMask) {
        if ((pollMask & Native.POLLOUT) != 0) {
            channel.ioUringUnsafe().pollOut(res);
        }
        if ((pollMask & Native.POLLIN) != 0) {
            channel.ioUringUnsafe().pollIn(res);
        }
        if ((pollMask & Native.POLLRDHUP) != 0) {
            channel.ioUringUnsafe().pollRdHup(res);
        }
    }

    private void addEventFdRead(IOUringSubmissionQueue submissionQueue) {
        submissionQueue.addRead(eventfd.intValue(), eventfdReadAddress, 0, 8, (short) -1, true);
    }

    private void handleConnect(AbstractIOUringChannel channel, int res) {
        channel.ioUringUnsafe().connectComplete(res);
    }

    @Override
    protected void cleanup() {
        try {
            eventfd.close();
        } catch (IOException e) {
            logger.warn("Failed to close the event fd.", e);
        }
        ringBuffer.close();
        iovArrays.release();
        if (eventfdReadBuf != null) {
            eventfdReadBuf.release();
        } else {
            PlatformDependent.freeMemory(eventfdReadAddress);
        }
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

    public IovArray iovArray() {
        IovArray iovArray = iovArrays.next();
        if (iovArray == null) {
            ringBuffer.getIoUringSubmissionQueue().submit();
            iovArray = iovArrays.next();
            assert iovArray != null;
        }
        return iovArray;
    }

    private void registerFixedBuffers(int ringFd) {
        if (buffersRegistered) {
            // Need to unregister first
            int ret = Native.ioUringRegister(ringFd, Native.IORING_UNREGISTER_BUFFERS,
                    0L /*NULL*/, 0);
            if (ret < 0 && ret != Native.ERRNO_ENXIO_NEGATIVE) {
                logger.warn("UNREGISTER_BUFFERS returned " + ret); //TODO
                return;
            }
            buffersRegistered = false;
        }
        int bufCount = tracker.getCount();
        if (bufCount > 0) {
            final ByteBuf ioVecs = tracker.getIoVecs();
            try {
                bufCount = tracker.getCount(); // note this may have changed due to the iovec alloc
                int ret = Native.ioUringRegister(ringFd, Native.IORING_REGISTER_BUFFERS,
                        ioVecs.memoryAddress(), bufCount);
                if (ret < 0) {
                    //TODO reset tracker indices here
                    throw new RuntimeException("REGISTER_BUFFERS returned " + ret);  //TODO
                }
                buffersRegistered = true;
            } finally {
                ioVecs.release();
            }
        }
    }

    private void pauseLongIo(IOUringSubmissionQueue submissionQueue) {
        for (IntObjectMap.PrimitiveEntry<AbstractIOUringChannel> ent : channels.entries()) {
            ent.value().removePolls();
        }
        if (!pendingWakeup) {
            submissionQueue.addReadCancel(eventfd.intValue());
        }
        if (prevDeadlineNanos != NONE) {
            submissionQueue.addTimeoutRemove();
            prevDeadlineNanos = NONE;
        }
        submissionQueue.submit();
    }
}
