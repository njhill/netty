/*
 * Copyright 2014 The Netty Project
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
package io.netty.channel.epoll;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.unix.IovArray;
import io.netty.channel.unix.Limits;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import static io.netty.channel.unix.Limits.UIO_MAX_IOV;
import static io.netty.channel.unix.NativeInetAddress.copyIpv4MappedIpv6Address;

/**
 * Support <a href="http://linux.die.net/man/2/sendmmsg">sendmmsg(...)</a> on linux with GLIBC 2.14+
 */
final class NativeDatagramPacketArray implements ChannelOutboundBuffer.MessageProcessor {

    // Use UIO_MAX_IOV as this is the maximum number we can write with one sendmmsg(...) call.
    private final NativeDatagramPacket[] packets = new NativeDatagramPacket[UIO_MAX_IOV];

    // We share one IovArray for all NativeDatagramPackets to reduce memory overhead. This will allow us to write
    // up to IOV_MAX iovec across all messages in one sendmmsg(...) call.
    private final IovArray iovArray = new IovArray();
    private int count;

    NativeDatagramPacketArray() {
        for (int i = 0; i < packets.length; i++) {
            packets[i] = new NativeDatagramPacket();
        }
    }

    /**
     * Try to add the given {@link DatagramPacket}. Returns {@code true} on success,
     * {@code false} otherwise.
     */
    private boolean addReadable(DatagramPacket packet) {
        if (count == packets.length) {
            // We already filled up to UIO_MAX_IOV messages. This is the max allowed per sendmmsg(...) call, we will
            // try again later.
            return false;
        }
        ByteBuf content = packet.content();
        int len = content.readableBytes();
        if (len == 0) {
            return true;
        }
        NativeDatagramPacket p = packets[count];
        InetSocketAddress recipient = packet.recipient();

        int offset = iovArray.count();
        if (!iovArray.addReadable(content)) {
            // Not enough space to hold the whole content, we will try again later.
            return false;
        }
        p.init(iovArray.memoryAddress(offset), iovArray.count() - offset, recipient);

        count++;
        return true;
    }

    boolean addWritable(long address, int len) {
        if (count == packets.length) {
            // We already filled up to UIO_MAX_IOV messages. This is the max allowed per recvmmsg(...) call, we will
            // try again later.
            return false;
        }
        if (len == 0) {
            return true;
        }
        NativeDatagramPacket p = packets[count];

        int offset = iovArray.count();
        if (offset == Limits.IOV_MAX || !iovArray.add(address, len)) {
            // Not enough space to hold the whole content, we will try again later.
            return false;
        }
        p.init(iovArray.memoryAddress(offset), iovArray.count() - offset);
        count++;
        return true;
    }

    @Override
    public boolean processMessage(Object msg) {
        return msg instanceof DatagramPacket && addReadable((DatagramPacket) msg);
    }

    /**
     * Returns the count
     */
    int count() {
        return count;
    }

    /**
     * Returns an array with {@link #count()} {@link NativeDatagramPacket}s filled.
     */
    NativeDatagramPacket[] packets() {
        return packets;
    }

    void clear() {
        this.count = 0;
        this.iovArray.clear();
    }

    void release() {
        iovArray.release();
    }

    /**
     * Used to pass needed data to JNI.
     */
    @SuppressWarnings("unused")
    static final class NativeDatagramPacket {

        // This is the actual struct iovec*
        private long memoryAddress;
        private int count;

        private final byte[] addr = new byte[16];
        private int addrLen;
        private int scopeId;
        private int port;

        private void init(long memoryAddress, int count, InetSocketAddress recipient) {
            init(memoryAddress, count);

            InetAddress address = recipient.getAddress();
            if (address instanceof Inet6Address) {
                System.arraycopy(address.getAddress(), 0, addr, 0, 16);
                addrLen = 16;
                scopeId = ((Inet6Address) address).getScopeId();
            } else {
                copyIpv4MappedIpv6Address(address.getAddress(), addr);
                addrLen = 16;
                scopeId = 0;
            }
            addrLen = addr.length;
            port = recipient.getPort();
        }

        private void init(long memoryAddress, int count) {
            this.memoryAddress = memoryAddress;
            this.count = count;
            this.scopeId = 0;
            this.port = 0;
            this.addrLen = 0;
        }

        DatagramPacket newDatagramPacket(ByteBuf buffer, InetSocketAddress localAddress) throws UnknownHostException {
            final InetAddress address;
            if (scopeId != 0) {
                address = Inet6Address.getByAddress(null, addr, scopeId);
            } else {
                address = InetAddress.getByAddress(addr);
            }
            return new DatagramPacket(buffer.writerIndex(count), localAddress, new InetSocketAddress(address, port));
        }
    }
}
