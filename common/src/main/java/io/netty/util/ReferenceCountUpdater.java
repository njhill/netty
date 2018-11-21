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
 * Common logic for {@link ReferenceCounted} implementations
 */
public final class ReferenceCountUpdater<T extends ReferenceCounted> {

    // For updated int field:
    //   Even => "real" refcount is (refCnt >>> 1)
    //   Odd => "real" refcount is 0

    private final long refCntFieldOffset;
    private final AtomicIntegerFieldUpdater<T> refCntUpdater;

    // Unfortunately the owning class must create the AtomicIntegerFieldUpdater
    private ReferenceCountUpdater(Class<T> clz, String fieldName, AtomicIntegerFieldUpdater<T> fieldUpdater) {
        long fieldOffset = -1;
        try {
            if (PlatformDependent.hasUnsafe()) {
                fieldOffset = PlatformDependent.objectFieldOffset(clz.getDeclaredField(fieldName));
            }
        } catch (Throwable ignore) {
            // fall-back
        }
        refCntFieldOffset = fieldOffset;
        refCntUpdater = fieldUpdater;
    }

    public static <T extends ReferenceCounted> ReferenceCountUpdater<T> newUpdater(Class<T> clz,
            String fieldName, AtomicIntegerFieldUpdater<T> fieldUpdater) {
        return new ReferenceCountUpdater<T>(clz, fieldName, fieldUpdater);
    }

    public int initialValue() {
        return 2;
    }

    private static int realCount(int refCnt) {
        return (refCnt & 1) != 0 ? 0 : refCnt >>> 1;
    }

    private int nonVolatileRawCnt(T instance) {
        // TODO: Once we compile against later versions of Java we can replace the Unsafe usage here by varhandles.
        return refCntFieldOffset != -1 ? PlatformDependent.getInt(instance, refCntFieldOffset)
                : refCntUpdater.get(instance);
    }

    public int refCnt(T instance) {
        return realCount(refCntUpdater.get(instance));
    }

    public int nonVolatileRefCnt(T instance) {
        return realCount(nonVolatileRawCnt(instance));
    }

    /**
     * An unsafe operation that sets the reference count directly
     */
    public void setRefCnt(T instance, int refCnt) {
        refCntUpdater.set(instance, refCnt << 1); // overflow OK here
    }

    public T retain(T instance) {
        return retain0(instance, 1);
    }

    public T retain(T instance, int increment) {
        return retain0(instance, checkPositive(increment, "increment"));
    }

    private T retain0(T instance, final int increment) {
        // all changes to the raw count are 2x the "real" change
        int adjustIncrement = increment << 1; // overflow OK here
        int oldRef = refCntUpdater.getAndAdd(instance, adjustIncrement);
        if ((oldRef & 1) != 0) {
            throw new IllegalReferenceCountException(0, increment);
        }
        // don't pass 0!
        if ((oldRef <= 0 && oldRef + adjustIncrement >= 0)
                || (oldRef >= 0 && oldRef + adjustIncrement < oldRef)) {
            // overflow case
            refCntUpdater.getAndAdd(instance, -increment);
            throw new IllegalReferenceCountException(realCount(oldRef), increment);
        }
        return instance;
    }

    public boolean release(T instance) {
        return release0(instance, 1);
    }

    public boolean release(T instance, int decrement) {
        return release0(instance, checkPositive(decrement, "decrement"));
    }

    private boolean release0(T instance, int decrement) {
        int rawCnt = nonVolatileRawCnt(instance);
        for (boolean firstTry = true;; firstTry = false) {
            if ((rawCnt & 1) != 0) {
                throw new IllegalReferenceCountException(0, -decrement);
            }
            int realCnt = rawCnt >>> 1;
            if (decrement == realCnt) {
                // most likely case
                if (refCntUpdater.compareAndSet(instance, rawCnt, 1)) { // any odd number
                    return true;
                }
                if (!firstTry) {
                    // this benefits throughput under high contention
                    Thread.yield();
                }
            } else if (decrement < realCnt) {
                // all changes to the raw count are 2x the "real" change
                if (refCntUpdater.compareAndSet(instance, rawCnt, rawCnt - (decrement << 1))) {
                    return false;
                }
                if (!firstTry) {
                    // this benefits throughput under high contention
                    Thread.yield();
                }
            } else if (!firstTry) {
                throw new IllegalReferenceCountException(realCnt, -decrement);
            }
            rawCnt = refCntUpdater.get(instance);
        }
    }
}
