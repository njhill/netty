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

import io.netty.util.Recycler;

import java.nio.ByteBuffer;

final class PooledDirectByteBuf extends PooledByteBuf<ByteBuffer> {

    private static final Recycler<PooledDirectByteBuf> RECYCLER = new Recycler<PooledDirectByteBuf>() {
        @Override
        protected PooledDirectByteBuf newObject(Handle<PooledDirectByteBuf> handle) {
            return new PooledDirectByteBuf(handle, 0);
        }
    };

    static PooledDirectByteBuf newInstance(int maxCapacity) {
        PooledDirectByteBuf buf = RECYCLER.get();
        buf.reuse(maxCapacity);
        return buf;
    }

    private PooledDirectByteBuf(Recycler.Handle<PooledDirectByteBuf> recyclerHandle, int maxCapacity) {
        super(recyclerHandle, maxCapacity);
    }

    @Override
    protected ByteBuffer newInternalNioBuffer(ByteBuffer memory) {
        return memory.duplicate();
    }

    @Override
    public boolean isDirect() {
        return true;
    }

    @Override
    protected byte _getByte(int index) {
        return memory.get(idx(index));
    }

    @Override
    protected short _getShort(int index) {
        return memory.getShort(idx(index));
    }

    @Override
    protected short _getShortLE(int index) {
        return ByteBufUtil.swapShort(_getShort(index));
    }

    @Override
    protected int _getUnsignedMedium(int index) {
        index = idx(index);
        return (memory.get(index) & 0xff)     << 16 |
               (memory.get(index + 1) & 0xff) << 8  |
               memory.get(index + 2) & 0xff;
    }

    @Override
    protected int _getUnsignedMediumLE(int index) {
        index = idx(index);
        return memory.get(index)      & 0xff        |
               (memory.get(index + 1) & 0xff) << 8  |
               (memory.get(index + 2) & 0xff) << 16;
    }

    @Override
    protected int _getInt(int index) {
        return memory.getInt(idx(index));
    }

    @Override
    protected int _getIntLE(int index) {
        return ByteBufUtil.swapInt(_getInt(index));
    }

    @Override
    protected long _getLong(int index) {
        return memory.getLong(idx(index));
    }

    @Override
    protected long _getLongLE(int index) {
        return ByteBufUtil.swapLong(_getLong(index));
    }

    @Override
    protected void _setByte(int index, int value) {
        memory.put(idx(index), (byte) value);
    }

    @Override
    protected void _setShort(int index, int value) {
        memory.putShort(idx(index), (short) value);
    }

    @Override
    protected void _setShortLE(int index, int value) {
        _setShort(index, ByteBufUtil.swapShort((short) value));
    }

    @Override
    protected void _setMedium(int index, int value) {
        index = idx(index);
        memory.put(index, (byte) (value >>> 16));
        memory.put(index + 1, (byte) (value >>> 8));
        memory.put(index + 2, (byte) value);
    }

    @Override
    protected void _setMediumLE(int index, int value) {
        index = idx(index);
        memory.put(index, (byte) value);
        memory.put(index + 1, (byte) (value >>> 8));
        memory.put(index + 2, (byte) (value >>> 16));
    }

    @Override
    protected void _setInt(int index, int value) {
        memory.putInt(idx(index), value);
    }

    @Override
    protected void _setIntLE(int index, int value) {
        _setInt(index, ByteBufUtil.swapInt(value));
    }

    @Override
    protected void _setLong(int index, long value) {
        memory.putLong(idx(index), value);
    }

    @Override
    protected void _setLongLE(int index, long value) {
        _setLong(index, ByteBufUtil.swapLong(value));
    }

    @Override
    public ByteBuf copy(int index, int length) {
        checkIndex(index, length);
        ByteBuf copy = alloc().directBuffer(length, maxCapacity());
        return copy.writeBytes(this, index, length);
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
    public boolean hasMemoryAddress() {
        return false;
    }

    @Override
    public long memoryAddress() {
        throw new UnsupportedOperationException();
    }
}
