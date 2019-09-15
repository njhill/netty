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
package io.netty.handler.codec.http;

import io.netty.buffer.ByteBufUtil;
import io.netty.util.CharsetUtil;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.StringUtil;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;

/**
 * Creates an URL-encoded URI from a path string and key-value parameter pairs.
 * This encoder is for one time use only.  Create a new instance for each URI.
 *
 * <pre>
 * {@link QueryStringEncoder} encoder = new {@link QueryStringEncoder}("/hello");
 * encoder.addParam("recipient", "world");
 * assert encoder.toString().equals("/hello?recipient=world");
 * </pre>
 * @see QueryStringDecoder
 */
public class QueryStringEncoder {

    private final String charsetName;
    private final StringBuilder uriBuilder;
    private boolean hasParams;

    /**
     * Creates a new encoder that encodes a URI that starts with the specified
     * path string.  The encoder will encode the URI in UTF-8.
     */
    public QueryStringEncoder(String uri) {
        this(uri, HttpConstants.DEFAULT_CHARSET);
    }

    /**
     * Creates a new encoder that encodes a URI that starts with the specified
     * path string in the specified charset.
     */
    public QueryStringEncoder(String uri, Charset charset) {
        uriBuilder = new StringBuilder(uri);
        charsetName = charset == CharsetUtil.UTF_8 ? null : charset.name();
    }

    /**
     * Adds a parameter with the specified name and value to this encoder.
     */
    public void addParam(String name, String value) {
        ObjectUtil.checkNotNull(name, "name");
        if (hasParams) {
            uriBuilder.append('&');
        } else {
            uriBuilder.append('?');
            hasParams = true;
        }
        if (charsetName == null) {
            appendComponentUtf8(name, uriBuilder);
            if (value != null) {
                appendComponentUtf8(value, uriBuilder.append('='));
            }
        } else {
            appendComponent(name, charsetName, uriBuilder);
            if (value != null) {
                appendComponent(value, charsetName, uriBuilder.append('='));
            }
        }
    }

    /**
     * Returns the URL-encoded URI object which was created from the path string
     * specified in the constructor and the parameters added by
     * {@link #addParam(String, String)} method.
     */
    public URI toUri() throws URISyntaxException {
        return new URI(toString());
    }

    /**
     * Returns the URL-encoded URI which was created from the path string
     * specified in the constructor and the parameters added by
     * {@link #addParam(String, String)} method.
     */
    @Override
    public String toString() {
        return uriBuilder.toString();
    }

    private static void appendComponent(String s, String charset, StringBuilder sb) {
        try {
            s = URLEncoder.encode(s, charset);
        } catch (UnsupportedEncodingException ignored) {
            throw new UnsupportedCharsetException(charset);
        }
        // replace all '+' with "%20"
        int idx = s.indexOf('+');
        if (idx == -1) {
            sb.append(s);
            return;
        }
        sb.append(s, 0, idx).append("%20");
        int size = s.length();
        idx++;
        for (; idx < size; idx++) {
            char c = s.charAt(idx);
            if (c != '+') {
                sb.append(c);
            } else {
                sb.append("%20");
            }
        }
    }

    private static final byte WRITE_UTF_UNKNOWN = (byte) '?';

    private static void appendEncoded(StringBuilder uriBuilder, int b) {
        uriBuilder.append('%').append(hexNibble(b >> 4)).append(hexNibble(b));
    }

    private static char hexNibble(int b) {
        b &= 0xf;
        return (char) (b < 10 ? b + 48 : b + 55);
    }

    /**
     * @see {@link ByteBufUtil#writeUtf8(io.netty.buffer.ByteBuf, CharSequence, int, int)}
     */
    private static void appendComponentUtf8(String seq, StringBuilder sb) {
        for (int i = 0, l = seq.length(); i < l; i++) {
            char c = seq.charAt(i);
            if (c < 0x80) {
                if ((c >= 'a' && c <= 'z') || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
                        || c == '-' || c == '_' || c == '.' || c == '*') {
                    sb.append(c);
                } else {
                    appendEncoded(sb, c);
                }
            } else if (c < 0x800) {
                appendEncoded(sb, 0xc0 | (c >> 6));
                appendEncoded(sb, 0x80 | (c & 0x3f));
            } else if (StringUtil.isSurrogate(c)) {
                if (!Character.isHighSurrogate(c)) {
                    appendEncoded(sb, WRITE_UTF_UNKNOWN);
                    continue;
                }
                // Surrogate Pair consumes 2 characters.
                if (++i == l) {
                    appendEncoded(sb, WRITE_UTF_UNKNOWN);
                    break;
                }
                // Extra method to allow inlining the rest of writeUtf8 which is the most likely code path.
                writeUtf8Surrogate(c, seq.charAt(i), sb);
            } else {
                appendEncoded(sb, 0xe0 | (c >> 12));
                appendEncoded(sb, 0x80 | ((c >> 6) & 0x3f));
                appendEncoded(sb, 0x80 | (c & 0x3f));
            }
        }
    }

    private static void writeUtf8Surrogate(char c, char c2, StringBuilder sb) {
        if (!Character.isLowSurrogate(c2)) {
            appendEncoded(sb, WRITE_UTF_UNKNOWN);
            appendEncoded(sb, Character.isHighSurrogate(c2) ? WRITE_UTF_UNKNOWN : c2);
            return;
        }
        int codePoint = Character.toCodePoint(c, c2);
        // See http://www.unicode.org/versions/Unicode7.0.0/ch03.pdf#G2630.
        appendEncoded(sb, 0xf0 | (codePoint >> 18));
        appendEncoded(sb, 0x80 | ((codePoint >> 12) & 0x3f));
        appendEncoded(sb, 0x80 | ((codePoint >> 6) & 0x3f));
        appendEncoded(sb, 0x80 | (codePoint & 0x3f));
    }
}
