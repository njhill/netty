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
package io.netty.handler.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ByteProcessor;

import java.util.List;

/**
 * A decoder that splits the received {@link ByteBuf}s on line endings.
 * <p>
 * Both {@code "\n"} and {@code "\r\n"} are handled.
 * <p>
 * The byte stream is expected to be in UTF-8 character encoding or ASCII. The current implementation
 * uses direct {@code byte} to {@code char} cast and then compares that {@code char} to a few low range
 * ASCII characters like {@code '\n'} or {@code '\r'}. UTF-8 is not using low range [0..0x7F]
 * byte values for multibyte codepoint representations therefore fully supported by this implementation.
 * <p>
 * For a more general delimiter-based decoder, see {@link DelimiterBasedFrameDecoder}.
 */
public class LineBasedFrameDecoder extends ByteToMessageDecoder {

    /** Maximum length of a frame we're willing to decode.  */
    private final int maxLength;
    /** Whether or not to throw an exception as soon as we exceed maxLength. */
    private final boolean failFast;
    private final boolean stripDelimiter;

    /** True if we're discarding input because we're already over maxLength.  */
    private boolean discarding;
    private int discardedBytes;

    /** Last scan position. */
    private int offset;

    /**
     * Creates a new decoder.
     * @param maxLength  the maximum length of the decoded frame.
     *                   A {@link TooLongFrameException} is thrown if
     *                   the length of the frame exceeds this value.
     */
    public LineBasedFrameDecoder(final int maxLength) {
        this(maxLength, true, false);
    }

    /**
     * Creates a new decoder.
     * @param maxLength  the maximum length of the decoded frame.
     *                   A {@link TooLongFrameException} is thrown if
     *                   the length of the frame exceeds this value.
     * @param stripDelimiter  whether the decoded frame should strip out the
     *                        delimiter or not
     * @param failFast  If <tt>true</tt>, a {@link TooLongFrameException} is
     *                  thrown as soon as the decoder notices the length of the
     *                  frame will exceed <tt>maxFrameLength</tt> regardless of
     *                  whether the entire frame has been read.
     *                  If <tt>false</tt>, a {@link TooLongFrameException} is
     *                  thrown after the entire frame that exceeds
     *                  <tt>maxFrameLength</tt> has been read.
     */
    public LineBasedFrameDecoder(final int maxLength, final boolean stripDelimiter, final boolean failFast) {
        this.maxLength = maxLength;
        this.failFast = failFast;
        this.stripDelimiter = stripDelimiter;
        setByteProcessor(ByteProcessor.FIND_LF, false);
    }

    @Override
    protected final void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        Object decoded = decode(ctx, in);
        if (decoded != null) {
            out.add(decoded);
        }
        setByteProcessor(ByteProcessor.FIND_LF, discarding);
    }

    /**
     * Create a frame out of the {@link ByteBuf} and return it.
     *
     * @param   ctx             the {@link ChannelHandlerContext} which this {@link ByteToMessageDecoder} belongs to
     * @param   buffer          the {@link ByteBuf} from which to read data
     * @return  frame           the {@link ByteBuf} which represent the frame or {@code null} if no frame could
     *                          be created.
     */
    protected Object decode(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
        int eol = getLastProcessedByteIndex(); // findEndOfLine(buffer);
        int delimLength = 1;
        //TODO optimize
        if (eol > buffer.readerIndex() && buffer.getByte(eol - 1) == '\r') {
            eol--;
            delimLength = 2;
        }
        if (!discarding) {
            if (eol >= 0) {
                final ByteBuf frame;
                final int length = eol - buffer.readerIndex();

                if (length > maxLength) {
                    buffer.readerIndex(eol + delimLength);
                    fail(ctx, length);
                    return null;
                }

                if (stripDelimiter) {
                    frame = buffer.readRetainedSlice(length);
                    buffer.skipBytes(delimLength);
                } else {
                    frame = buffer.readRetainedSlice(length + delimLength);
                }
                return frame;
            }
            assert failFast;
            
            //TODO here search in remainder of available buffer and
            // report/advance accordingly
            
            final int length = buffer.readableBytes();
            if (length > maxLength) {
                //TODO need change here
                discardedBytes = length;
                buffer.readerIndex(buffer.writerIndex());
                discarding = true;
                offset = 0;
                fail(ctx, "over " + discardedBytes);
            }
            return null;
        }
        assert eol >= 0;
        
        //TODO we don't know discardedBytes here without custom ByteProcessor
         // which counts (it should exclude `\r` chars from count tho)
        
        final int length = discardedBytes + eol - buffer.readerIndex();
        buffer.readerIndex(eol + delimLength);
        discardedBytes = 0;
        discarding = false;
        if (!failFast) {
            fail(ctx, length);
        }

        return null;
    }

    private void fail(final ChannelHandlerContext ctx, int length) {
        fail(ctx, String.valueOf(length));
    }

    private void fail(final ChannelHandlerContext ctx, String length) {
        ctx.fireExceptionCaught(
                new TooLongFrameException(
                        "frame length (" + length + ") exceeds the allowed maximum (" + maxLength + ')'));
    }

    /**
     * Returns the index in the buffer of the end of line found.
     * Returns -1 if no end of line was found in the buffer.
     */
    private int findEndOfLine(final ByteBuf buffer) {
        int totalLength = buffer.readableBytes();
        int i = buffer.forEachByte(buffer.readerIndex() + offset, totalLength - offset, ByteProcessor.FIND_LF);
        if (i >= 0) {
            offset = 0;
            if (i > 0 && buffer.getByte(i - 1) == '\r') {
                i--;
            }
        } else {
            offset = totalLength;
        }
        return i;
    }
}
