/*
 * Copyright 2013 The Netty Project
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
package io.netty.util;

import static io.netty.util.internal.ObjectUtil.checkPositive;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import io.netty.util.internal.PlatformDependent;

/**
 * Abstract base class for classes wants to implement {@link ReferenceCounted}.
 */
public abstract class AbstractReferenceCounted implements ReferenceCounted {
    private static final long REFCNT_FIELD_OFFSET;
    private static final AtomicIntegerFieldUpdater<AbstractReferenceCounted> refCntUpdater =
            AtomicIntegerFieldUpdater.newUpdater(AbstractReferenceCounted.class, "refCnt");

    // even => "real" refcount is (refCnt >>> 1); odd => "real" refcount is 0
    @SuppressWarnings("unused")
    private volatile int refCnt = 2;

    static {
        long refCntFieldOffset = -1;
        try {
            if (PlatformDependent.hasUnsafe()) {
                refCntFieldOffset = PlatformDependent.objectFieldOffset(
                        AbstractReferenceCounted.class.getDeclaredField("refCnt"));
            }
        } catch (Throwable ignore) {
            refCntFieldOffset = -1;
        }

        REFCNT_FIELD_OFFSET = refCntFieldOffset;
    }

    private static int realRef(int refCnt) {
        return (refCnt & 1) != 0 ? 0 : refCnt >>> 1;
    }

    private int nonVolatileRefCnt() {
        // TODO: Once we compile against later versions of Java we can replace the Unsafe usage here by varhandles.
        return REFCNT_FIELD_OFFSET != -1 ? PlatformDependent.getInt(this, REFCNT_FIELD_OFFSET) : volatileRefCnt();
    }

    private int volatileRefCnt() {
        return refCntUpdater.get(this);
    }

    @Override
    public int refCnt() {
        return realRef(volatileRefCnt());
    }

    /**
     * An unsafe operation intended for use by a subclass that sets the reference count of the buffer directly
     */
    protected final void setRefCnt(int refCnt) {
        refCntUpdater.set(this, refCnt << 1); // overflow OK here
    }

    @Override
    public ReferenceCounted retain() {
        return retain0(1);
    }

    @Override
    public ReferenceCounted retain(int increment) {
        return retain0(checkPositive(increment, "increment"));
    }

    private ReferenceCounted retain0(final int increment) {
        // all changes to the raw count are 2x the "real" change
        int adjustIncrement = increment << 1; // overflow OK here
        int oldRef = refCntUpdater.getAndAdd(this, adjustIncrement);
        if ((oldRef & 1) != 0) {
            throw new IllegalReferenceCountException(0, increment);
        }
        // don't pass 0!
        if ((oldRef <= 0 && oldRef + adjustIncrement >= 0)
                || (oldRef >= 0 && oldRef + adjustIncrement < oldRef)) {
            // overflow case
            refCntUpdater.getAndAdd(this, -increment);
            throw new IllegalReferenceCountException(realRef(oldRef), increment);
        }
        return this;
    }

    @Override
    public ReferenceCounted touch() {
        return touch(null);
    }

    @Override
    public boolean release() {
        return release0(1);
    }

    @Override
    public boolean release(int decrement) {
        return release0(checkPositive(decrement, "decrement"));
    }

    private boolean release0(int decrement) {
        int rawCnt = nonVolatileRefCnt();
        for (boolean firstTry = true;; firstTry = false) {
            if ((rawCnt & 1) != 0) {
                throw new IllegalReferenceCountException(0, -decrement);
            }
            int realCnt = rawCnt >>> 1;
            if (decrement == realCnt) {
                // most likely case
                if (refCntUpdater.compareAndSet(this, rawCnt, 1)) { // any odd number
                    deallocate();
                    return true;
                }
                if (!firstTry) {
                    // this benefits throughput under high contention
                    Thread.yield();
                }
            } else if (decrement < realCnt) {
                // all changes to the raw count are 2x the "real" change
                if (refCntUpdater.compareAndSet(this, rawCnt, rawCnt - (decrement << 1))) {
                    return false;
                }
                if (!firstTry) {
                    // this benefits throughput under high contention
                    Thread.yield();
                }
            } else if (!firstTry) {
                throw new IllegalReferenceCountException(realCnt, -decrement);
            }
            rawCnt = volatileRefCnt();
        }
    }
    /**
     * Called once {@link #refCnt()} is equals 0.
     */
    protected abstract void deallocate();
}
