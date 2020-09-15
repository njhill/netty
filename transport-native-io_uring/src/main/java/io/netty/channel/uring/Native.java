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
import io.netty.channel.unix.Socket;
import io.netty.util.internal.NativeLibraryLoader;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.ThrowableUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.nio.channels.Selector;
import java.util.Locale;

final class Native {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(Native.class);
    static final int DEFAULT_RING_SIZE = Math.max(64, SystemPropertyUtil.getInt("io.netty.uring.ringSize", 4096));

    static {
        Selector selector = null;
        try {
            // We call Selector.open() as this will under the hood cause IOUtil to be loaded.
            // This is a workaround for a possible classloader deadlock that could happen otherwise:
            //
            // See https://github.com/netty/netty/issues/10187
            selector = Selector.open();
        } catch (IOException ignore) {
            // Just ignore
        }
        try {
            // First, try calling a side-effect free JNI method to see if the library was already
            // loaded by the application.
            Native.createFile();
        } catch (UnsatisfiedLinkError ignore) {
            // The library was not previously loaded, load it now.
            loadNativeLibrary();
        } finally {
            try {
                if (selector != null) {
                    selector.close();
                }
            } catch (IOException ignore) {
                // Just ignore
            }
        }
        Socket.initialize();
    }
    static final int SOCK_NONBLOCK = NativeStaticallyReferencedJniMethods.sockNonblock();
    static final int SOCK_CLOEXEC = NativeStaticallyReferencedJniMethods.sockCloexec();
    static final int POLLIN = NativeStaticallyReferencedJniMethods.pollin();
    static final int POLLOUT = NativeStaticallyReferencedJniMethods.pollout();
    static final int POLLRDHUP = NativeStaticallyReferencedJniMethods.pollrdhup();
    static final int ERRNO_ECANCELED_NEGATIVE = -NativeStaticallyReferencedJniMethods.ecanceled();
    static final int ERRNO_ETIME_NEGATIVE = -NativeStaticallyReferencedJniMethods.etime();
    static final int ERRNO_ENXIO_NEGATIVE = -NativeStaticallyReferencedJniMethods.enxio();

    static final int IORING_OP_POLL_ADD = NativeStaticallyReferencedJniMethods.ioringOpPollAdd();
    static final int IORING_OP_TIMEOUT = NativeStaticallyReferencedJniMethods.ioringOpTimeout();
    static final int IORING_OP_TIMEOUT_REMOVE = NativeStaticallyReferencedJniMethods.ioringOpTimeoutRemove();
    static final int IORING_OP_ACCEPT = NativeStaticallyReferencedJniMethods.ioringOpAccept();
    static final int IORING_OP_READ = NativeStaticallyReferencedJniMethods.ioringOpRead();
    static final int IORING_OP_WRITE = NativeStaticallyReferencedJniMethods.ioringOpWrite();
    static final int IORING_OP_POLL_REMOVE = NativeStaticallyReferencedJniMethods.ioringOpPollRemove();
    static final int IORING_OP_CONNECT = NativeStaticallyReferencedJniMethods.ioringOpConnect();
    static final int IORING_OP_CLOSE = NativeStaticallyReferencedJniMethods.ioringOpClose();
    static final int IORING_OP_WRITEV = NativeStaticallyReferencedJniMethods.ioringOpWritev();
    static final int IORING_OP_READ_FIXED = NativeStaticallyReferencedJniMethods.ioringOpReadFixed();
    static final int IORING_OP_WRITE_FIXED = NativeStaticallyReferencedJniMethods.ioringOpWriteFixed();
    static final int IORING_OP_ASYNC_CANCEL = NativeStaticallyReferencedJniMethods.ioringOpAsyncCancel();

    static final int IORING_REGISTER_BUFFERS = NativeStaticallyReferencedJniMethods.ioringRegisterBuffers();
    static final int IORING_UNREGISTER_BUFFERS = NativeStaticallyReferencedJniMethods.ioringUnregisterBuffers();

    static final int IORING_ENTER_GETEVENTS = NativeStaticallyReferencedJniMethods.ioringEnterGetevents();
    static final int IOSQE_ASYNC = NativeStaticallyReferencedJniMethods.iosqeAsync();

    static RingBuffer createRingBuffer(int ringSize) {
        //Todo throw Exception if it's null
        return ioUringSetup(ringSize, new Runnable() {
            @Override
            public void run() {
                // Noop
            }
        });
    }

    static RingBuffer createRingBuffer(int ringSize, Runnable submissionCallback) {
        //Todo throw Exception if it's null
        return ioUringSetup(ringSize, submissionCallback);
    }

    static RingBuffer createRingBuffer(Runnable submissionCallback) {
        return createRingBuffer(DEFAULT_RING_SIZE, submissionCallback);
    }

    private static native RingBuffer ioUringSetup(int entries, Runnable submissionCallback);

    public static native int ioUringEnter(int ringFd, int toSubmit, int minComplete, int flags);

    public static native int ioUringRegister(int ringFd, int opcode, long argsAddress, int count);

    public static native void eventFdWrite(int fd, long value);

    public static FileDescriptor newBlockingEventFd() {
        return new FileDescriptor(blockingEventFd());
    }

    public static native void ioUringExit(RingBuffer ringBuffer);

    private static native int blockingEventFd();

    // for testing(it is only temporary)
    public static native int createFile();

    public static Socket newSocketStream() {
        return Socket.newSocketStream();
    }

    private Native() {
        // utility
    }

    // From io_uring native library
    private static void loadNativeLibrary() {
        String name = PlatformDependent.normalizedOs().toLowerCase(Locale.UK).trim();
        if (!name.startsWith("linux")) {
            throw new IllegalStateException("Only supported on Linux");
        }
        String staticLibName = "netty_transport_native_io_uring";
        String sharedLibName = staticLibName + '_' + PlatformDependent.normalizedArch();
        ClassLoader cl = PlatformDependent.getClassLoader(Native.class);
        try {
            NativeLibraryLoader.load(sharedLibName, cl);
        } catch (UnsatisfiedLinkError e1) {
            try {
                NativeLibraryLoader.load(staticLibName, cl);
                logger.info("Failed to load io_uring");
            } catch (UnsatisfiedLinkError e2) {
                ThrowableUtil.addSuppressed(e1, e2);
                throw e1;
            }
        }
    }
}
