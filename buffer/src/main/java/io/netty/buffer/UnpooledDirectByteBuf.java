/*
 * Copyright 2012 The Netty Project
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
package io.netty.buffer;

import static io.netty.util.internal.ObjectUtil.checkPositiveOrZero;

import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.ObjectUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A NIO {@link ByteBuffer} based buffer. It is recommended to use
 * {@link UnpooledByteBufAllocator#directBuffer(int, int)}, {@link Unpooled#directBuffer(int)} and
 * {@link Unpooled#wrappedBuffer(ByteBuffer)} instead of calling the constructor explicitly.
 */
public class UnpooledDirectByteBuf extends UnpooledByteBuf {

    ByteBuffer buffer; // accessed by UnpooledUnsafeNoCleanerDirectByteBuf.reallocateDirect()
    private int capacity;
    private boolean doNotFree;

    /**
     * Creates a new direct buffer.
     *
     * @param initialCapacity the initial capacity of the underlying direct buffer
     * @param maxCapacity     the maximum capacity of the underlying direct buffer
     */
    public UnpooledDirectByteBuf(ByteBufAllocator alloc, int initialCapacity, int maxCapacity) {
        super(alloc, maxCapacity);
        if (checkPositiveOrZero(initialCapacity, "initialCapacity") > maxCapacity) {
            throw new IllegalArgumentException(String.format(
                    "initialCapacity(%d) > maxCapacity(%d)", initialCapacity, maxCapacity));
        }

        setByteBuffer(allocateDirect(initialCapacity), false);
    }

    /**
     * Creates a new direct buffer by wrapping the specified initial buffer.
     *
     * @param maxCapacity the maximum capacity of the underlying direct buffer
     */
    protected UnpooledDirectByteBuf(ByteBufAllocator alloc, ByteBuffer initialBuffer, int maxCapacity) {
        this(alloc, initialBuffer, maxCapacity, false, true);
    }

    UnpooledDirectByteBuf(ByteBufAllocator alloc, ByteBuffer initialBuffer,
            int maxCapacity, boolean doFree, boolean slice) {
        super(alloc, maxCapacity);
        ObjectUtil.checkNotNull(initialBuffer, "initialBuffer");
        if (!initialBuffer.isDirect()) {
            throw new IllegalArgumentException("initialBuffer is not a direct buffer.");
        }
        if (initialBuffer.isReadOnly()) {
            throw new IllegalArgumentException("initialBuffer is a read-only buffer.");
        }

        int initialCapacity = initialBuffer.remaining();
        if (initialCapacity > maxCapacity) {
            throw new IllegalArgumentException(String.format(
                    "initialCapacity(%d) > maxCapacity(%d)", initialCapacity, maxCapacity));
        }

        doNotFree = !doFree;
        setByteBuffer((slice ? initialBuffer.slice() : initialBuffer).order(ByteOrder.BIG_ENDIAN), false);
        writerIndex(initialCapacity);
    }

    /**
     * Allocate a new direct {@link ByteBuffer} with the given initialCapacity.
     */
    protected ByteBuffer allocateDirect(int initialCapacity) {
        return ByteBuffer.allocateDirect(initialCapacity);
    }

    /**
     * Free a direct {@link ByteBuffer}
     */
    protected void freeDirect(ByteBuffer buffer) {
        PlatformDependent.freeDirectBuffer(buffer);
    }

    void setByteBuffer(ByteBuffer buffer, boolean tryFree) {
        if (tryFree) {
            ByteBuffer oldBuffer = this.buffer;
            if (oldBuffer != null) {
                if (doNotFree) {
                    doNotFree = false;
                } else {
                    freeDirect(oldBuffer);
                }
            }
        }

        this.buffer = buffer;
        capacity = buffer.remaining();
    }

    @Override
    public boolean isDirect() {
        return true;
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    void replaceBuffer(int newCapacity, int bytesToCopy) {
        ByteBuffer oldBuffer = buffer;
        ByteBuffer newBuffer = allocateDirect(newCapacity);
        oldBuffer.position(0).limit(bytesToCopy);
        newBuffer.position(0).limit(bytesToCopy);
        newBuffer.put(oldBuffer).clear();
        setByteBuffer(newBuffer, true);
    }

    @Override
    public boolean hasArray() {
        return false;
    }

    @Override
    public byte[] array() {
        throw new UnsupportedOperationException("direct buffer");
    }

    @Override
    public int arrayOffset() {
        throw new UnsupportedOperationException("direct buffer");
    }

    @Override
    protected byte _getByte(int index) {
        return buffer.get(index);
    }

    @Override
    protected short _getShort(int index) {
        return buffer.getShort(index);
    }

    @Override
    protected short _getShortLE(int index) {
        return ByteBufUtil.swapShort(buffer.getShort(index));
    }

    @Override
    protected int _getUnsignedMedium(int index) {
        return (getByte(index) & 0xff)     << 16 |
               (getByte(index + 1) & 0xff) << 8  |
               getByte(index + 2) & 0xff;
    }

    @Override
    protected int _getUnsignedMediumLE(int index) {
        return getByte(index) & 0xff             |
               (getByte(index + 1) & 0xff) << 8  |
               (getByte(index + 2) & 0xff) << 16;
    }

    @Override
    protected int _getInt(int index) {
        return buffer.getInt(index);
    }

    @Override
    protected int _getIntLE(int index) {
        return ByteBufUtil.swapInt(buffer.getInt(index));
    }

    @Override
    protected long _getLong(int index) {
        return buffer.getLong(index);
    }

    @Override
    protected long _getLongLE(int index) {
        return ByteBufUtil.swapLong(buffer.getLong(index));
    }

    @Override
    protected void _setByte(int index, int value) {
        buffer.put(index, (byte) value);
    }

    @Override
    protected void _setShort(int index, int value) {
        buffer.putShort(index, (short) value);
    }

    @Override
    protected void _setShortLE(int index, int value) {
        buffer.putShort(index, ByteBufUtil.swapShort((short) value));
    }

    @Override
    protected void _setMedium(int index, int value) {
        setByte(index, (byte) (value >>> 16));
        setByte(index + 1, (byte) (value >>> 8));
        setByte(index + 2, (byte) value);
    }

    @Override
    protected void _setMediumLE(int index, int value) {
        setByte(index, (byte) value);
        setByte(index + 1, (byte) (value >>> 8));
        setByte(index + 2, (byte) (value >>> 16));
    }

    @Override
    protected void _setInt(int index, int value) {
        buffer.putInt(index, value);
    }

    @Override
    protected void _setIntLE(int index, int value) {
        buffer.putInt(index, ByteBufUtil.swapInt(value));
    }

    @Override
    protected void _setLong(int index, long value) {
        buffer.putLong(index, value);
    }

    @Override
    protected void _setLongLE(int index, long value) {
        buffer.putLong(index, ByteBufUtil.swapLong(value));
    }

    @Override
    public int setBytes(int index, InputStream in, int length) throws IOException {
        ensureAccessible();
        if (buffer.hasArray()) {
            return in.read(buffer.array(), buffer.arrayOffset() + index, length);
        } else {
            byte[] tmp = ByteBufUtil.threadLocalTempArray(length);
            int readBytes = in.read(tmp, 0, length);
            if (readBytes <= 0) {
                return readBytes;
            }
            ByteBuffer tmpBuf = internalNioBuffer();
            tmpBuf.clear().position(index);
            tmpBuf.put(tmp, 0, readBytes);
            return readBytes;
        }
    }

    @Override
    public ByteBuf copy(int index, int length) {
        ensureAccessible();
        ByteBuffer src;
        try {
            src = _internalNioBuffer(index, length, true);
        } catch (IllegalArgumentException ignored) {
            throw new IndexOutOfBoundsException("Too many bytes to read - Need " + (index + length));
        }

        return alloc().directBuffer(length, maxCapacity()).writeBytes(src);
    }

    @Override
    ByteBuffer newInternalNioBuffer() {
        return buffer.duplicate();
    }

    @Override
    protected void deallocate() {
        ByteBuffer buffer = this.buffer;
        if (buffer == null) {
            return;
        }

        this.buffer = null;

        if (!doNotFree) {
            freeDirect(buffer);
        }
    }
}
