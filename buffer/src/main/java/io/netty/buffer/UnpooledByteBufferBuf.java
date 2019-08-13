/*
 * Copyright 2019 The Netty Project
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * A NIO {@link ByteBuffer} based buffer.
 */
abstract class UnpooledByteBufferBuf extends UnpooledByteBuf {

    ByteBuffer buffer;

    UnpooledByteBufferBuf(ByteBufAllocator alloc, int maxCapacity) {
        super(alloc, maxCapacity);
    }

    @Override
    public int capacity() {
        return buffer.capacity();
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
        return getByte(index)      & 0xff        |
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
        }
        byte[] tmp = ByteBufUtil.threadLocalTempArray(length);
        int readBytes = in.read(tmp, 0, length);
        if (readBytes > 0) {
            ByteBuffer tmpBuf = internalNioBuffer();
            tmpBuf.clear().position(index);
            tmpBuf.put(tmp, 0, readBytes);
        }
        return readBytes;
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

        ByteBuf dst = src.isDirect() ? alloc().directBuffer(length, maxCapacity())
                : alloc().heapBuffer(length, maxCapacity());
        return dst.writeBytes(src);
    }

    @Override
    public boolean isReadOnly() {
        return buffer.isReadOnly();
    }

    @Override
    public boolean isDirect() {
        return buffer.isDirect();
    }

    @Override
    public boolean hasArray() {
        return buffer.hasArray();
    }

    @Override
    public byte[] array() {
        return buffer.array();
    }

    @Override
    public int arrayOffset() {
        return buffer.arrayOffset();
    }

    @Override
    ByteBuffer newInternalNioBuffer() {
        return buffer.duplicate();
    }

    @Override
    protected void deallocate() {
        this.buffer = null;
    }
}
