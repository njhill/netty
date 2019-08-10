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

import static io.netty.util.internal.ObjectUtil.checkNotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

abstract class UnpooledByteBuf extends AbstractReferenceCountedByteBuf {

    private final ByteBufAllocator alloc;
    private ByteBuffer tmpNioBuf;

    UnpooledByteBuf(ByteBufAllocator alloc, int maxCapacity) {
        super(maxCapacity);
        this.alloc = checkNotNull(alloc, "alloc");
    }

    @Override
    public ByteBufAllocator alloc() {
        return alloc;
    }

    @Override
    public ByteOrder order() {
        return ByteOrder.BIG_ENDIAN;
    }

    @Override
    public boolean hasMemoryAddress() {
        return false;
    }

    @Override
    public long memoryAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf capacity(int newCapacity) {
        checkNewCapacity(newCapacity);
        int oldCapacity = capacity();
        if (newCapacity == oldCapacity) {
            return this;
        }
        int bytesToCopy;
        if (newCapacity > oldCapacity) {
            bytesToCopy = oldCapacity;
        } else {
            trimIndicesToCapacity(newCapacity);
            bytesToCopy = newCapacity;
        }
        replaceBuffer(newCapacity, bytesToCopy);
        tmpNioBuf = null;
        return this;
    }

    abstract void replaceBuffer(int newCapacity, int bytesToCopy);

    @Override
    public byte getByte(int index) {
        ensureAccessible();
        return _getByte(index);
    }

    @Override
    public short getShort(int index) {
        ensureAccessible();
        return _getShort(index);
    }

    @Override
    public int getUnsignedMedium(int index) {
        ensureAccessible();
        return _getUnsignedMedium(index);
    }

    @Override
    public int getInt(int index) {
        ensureAccessible();
        return _getInt(index);
    }

    @Override
    public int getIntLE(int index) {
        ensureAccessible();
        return _getIntLE(index);
    }

    @Override
    public long getLong(int index) {
        ensureAccessible();
        return _getLong(index);
    }

    @Override
    public long getLongLE(int index) {
        ensureAccessible();
        return _getLongLE(index);
    }

    @Override
    public ByteBuf setByte(int index, int value) {
        ensureAccessible();
        _setByte(index, value);
        return this;
    }

    @Override
    public ByteBuf setShort(int index, int value) {
        ensureAccessible();
        _setShort(index, value);
        return this;
    }

    @Override
    public ByteBuf setShortLE(int index, int value) {
        ensureAccessible();
        _setShortLE(index, value);
        return this;
    }

    @Override
    public ByteBuf setMedium(int index, int   value) {
        ensureAccessible();
        _setMedium(index, value);
        return this;
    }

    @Override
    public ByteBuf setMediumLE(int index, int   value) {
        ensureAccessible();
        _setMediumLE(index, value);
        return this;
    }

    @Override
    public ByteBuf setInt(int index, int   value) {
        ensureAccessible();
        _setInt(index, value);
        return this;
    }

    @Override
    public ByteBuf setIntLE(int index, int   value) {
        ensureAccessible();
        _setIntLE(index, value);
        return this;
    }

    @Override
    public ByteBuf setLong(int index, long  value) {
        ensureAccessible();
        _setLong(index, value);
        return this;
    }

    @Override
    public ByteBuf setLongLE(int index, long  value) {
        ensureAccessible();
        _setLongLE(index, value);
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
        checkDstIndex(index, length, dstIndex, checkNotNull(dst, "dst").capacity());
        _getBytes(index, dst, dstIndex, length, false);
        return this;
    }

    @Override
    public ByteBuf readBytes(ByteBuf dst, int dstIndex, int length) {
        checkReadableBytes(length);
        _getBytes(readerIndex, dst, dstIndex, length, true);
        readerIndex += length;
        return this;
    }

    void _getBytes(int index, ByteBuf dst, int dstIndex, int length, boolean internal) {
        if (dst.hasArray()) {
            _getBytes(index, dst.array(), dst.arrayOffset() + dstIndex, length, internal);
        } else if (dst.nioBufferCount() > 0) {
            for (ByteBuffer bb: dst.nioBuffers(dstIndex, length)) {
                int bbLen = bb.remaining();
                _getBytes(index, bb, internal);
                index += bbLen;
            }
        } else {
            dst.setBytes(dstIndex, this, index, length);
        }
    }

    @Override
    public ByteBuf getBytes(int index, OutputStream out, int length) throws IOException {
        checkIndex(index, length);
        _getBytes(index, checkNotNull(out, "out"), length, false);
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
        checkDstIndex(index, length, dstIndex, checkNotNull(dst, "dst").length);
        _getBytes(index, dst, dstIndex, length, false);
        return this;
    }

    @Override
    public ByteBuf readBytes(byte[] dst, int dstIndex, int length) {
        checkReadableBytes(length);
        _getBytes(readerIndex, dst, dstIndex, length, true);
        readerIndex += length;
        return this;
    }

    void _getBytes(int index, byte[] dst, int dstIndex, int length, boolean internal) {
        _internalNioBuffer(index, length, !internal).get(dst, dstIndex, length);
    }

    @Override
    public ByteBuf getBytes(int index, ByteBuffer dst) {
        int length = dst.remaining();
        checkIndex(index, length);
        _getBytes(index, dst, false);
        return this;
    }

    @Override
    public ByteBuf readBytes(ByteBuffer dst) {
        int length = dst.remaining();
        checkReadableBytes(length);
        _getBytes(readerIndex, dst, true);
        readerIndex += length;
        return this;
    }

    void _getBytes(int index, ByteBuffer dst, boolean internal) {
        dst.put(_internalNioBuffer(index, dst.remaining(), !internal));
    }

    @Override
    public ByteBuf readBytes(OutputStream out, int length) throws IOException {
        checkReadableBytes(length);
        _getBytes(readerIndex, out, length, true);
        readerIndex += length;
        return this;
    }

    void _getBytes(int index, OutputStream out, int length, boolean internal) throws IOException {
        if (length != 0) {
            ByteBufUtil.readBytes(alloc(),
                    internal ? internalNioBuffer() : newInternalNioBuffer(), index, length, out);
        }
    }

    @Override
    public int readBytes(FileChannel out, long position, int length) throws IOException {
        checkReadableBytes(length);
        int readBytes = out.write(internalNioBuffer(readerIndex, length), position);
        readerIndex += readBytes;
        return readBytes;
    }

    @Override
    public int readBytes(GatheringByteChannel out, int length) throws IOException {
        checkReadableBytes(length);
        int readBytes = out.write(internalNioBuffer(readerIndex, length));
        readerIndex += readBytes;
        return readBytes;
    }

    @Override
    public int getBytes(int index, FileChannel out, long position, int length) throws IOException {
        ensureAccessible();
        return out.write(_internalNioBuffer(index, length, true), position);
    }

    @Override
    public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
        ensureAccessible();
        return out.write(_internalNioBuffer(index, length, true));
    }

    @Override
    public int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
        try {
            return in.read(internalNioBuffer(index, length));
        } catch (ClosedChannelException ignored) {
            return -1;
        }
    }

    @Override
    public int setBytes(int index, FileChannel in, long position, int length) throws IOException {
        try {
            return in.read(internalNioBuffer(index, length), position);
        } catch (ClosedChannelException ignored) {
            return -1;
        }
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
        checkSrcIndex(index, length, srcIndex, src.capacity());
        if (src.nioBufferCount() > 0) {
            for (ByteBuffer bb: src.nioBuffers(srcIndex, length)) {
                int bbLen = bb.remaining();
                setBytes(index, bb);
                index += bbLen;
            }
        } else {
            src.getBytes(srcIndex, this, index, length);
        }
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
        checkSrcIndex(index, length, srcIndex, src.length);
        _internalNioBuffer(index, length, false).put(src, srcIndex, length);
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuffer src) {
        ensureAccessible();
        ByteBuffer tmpBuf = internalNioBuffer();
        if (src == tmpBuf) {
            src = src.duplicate();
        }

        tmpBuf.clear().position(index).limit(index + src.remaining());
        tmpBuf.put(src);
        return this;
    }

    final ByteBuffer _internalNioBuffer(int index, int length, boolean duplicate) {
        ByteBuffer buffer = duplicate ? newInternalNioBuffer() : internalNioBuffer();
        buffer.limit(index + length).position(index);
        return buffer;
    }

    abstract ByteBuffer newInternalNioBuffer();

    ByteBuffer internalNioBuffer() {
        ByteBuffer tmpNioBuf = this.tmpNioBuf;
        if (tmpNioBuf == null) {
            this.tmpNioBuf = tmpNioBuf = newInternalNioBuffer();
        }
        return tmpNioBuf;
    }

    @Override
    public ByteBuffer internalNioBuffer(int index, int length) {
        checkIndex(index, length);
        return _internalNioBuffer(index, length, false);
    }

    @Override
    public int nioBufferCount() {
        return 1;
    }

    @Override
    public ByteBuffer[] nioBuffers(int index, int length) {
        return new ByteBuffer[] { nioBuffer(index, length) };
    }

    @Override
    public ByteBuffer nioBuffer(int index, int length) {
        checkIndex(index, length);
        return _internalNioBuffer(index, length, true).slice();
    }

    @Override
    public ByteBuf unwrap() {
        return null;
    }
}
