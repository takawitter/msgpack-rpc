//
// MessagePack-RPC for Java
//
// Copyright (C) 2010 FURUHASHI Sadayuki
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//
package org.msgpack.rpc.loop.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.nio.channels.Channels;
import java.util.Map;

import org.msgpack.rpc.Session;
import org.msgpack.rpc.config.TcpClientConfig;
import org.msgpack.rpc.transport.PooledStreamClientTransport;
import org.msgpack.rpc.transport.RpcMessageHandler;

class NettyTcpClientTransport extends PooledStreamClientTransport<Channel, ByteBufOutputStream> {
    private static final String TCP_NO_DELAY = "tcpNoDelay";

    private final Bootstrap bootstrap;

    NettyTcpClientTransport(TcpClientConfig config, Session session,
            NettyEventLoop loop) {
        // TODO check session.getAddress() instanceof IPAddress
        super(config, session);

        RpcMessageHandler handler = new RpcMessageHandler(session);

        bootstrap = new Bootstrap();
        bootstrap.loop.getClientFactory());
        bootstrap.setPipelineFactory(new StreamPipelineFactory(loop.getMessagePack(), handler));
        Map<String, Object> options = config.getOptions();
        setIfNotPresent(options, TCP_NO_DELAY, Boolean.TRUE, bootstrap);
        bootstrap.setOptions(options);
    }

    private final ChannelFutureListener connectListener = new ChannelFutureListener() {
        public void operationComplete(ChannelFuture future) throws Exception {
            if (!future.isSuccess()) {
                onConnectFailed(future.getChannel(), future.getCause());
                return;
            }
            Channel c = future.getChannel();
            c.getCloseFuture().addListener(closeListener);
            onConnected(c);
        }
    };

    private final ChannelFutureListener closeListener = new ChannelFutureListener() {
        public void operationComplete(ChannelFuture future) throws Exception {
            Channel c = future.getChannel();
            onClosed(c);
        }
    };

    @Override
    protected void startConnection() {
        ChannelFuture f = bootstrap.connect(session.getAddress().getSocketAddress());
        f.addListener(connectListener);
    }

    @Override
    protected ChannelBufferOutputStream newPendingBuffer() {
        return new ChannelBufferOutputStream(
                ChannelBuffers.dynamicBuffer(HeapChannelBufferFactory.getInstance()));
    }

    @Override
    protected void resetPendingBuffer(ChannelBufferOutputStream b) {
        b.buffer().clear();
    }

    @Override
    protected void flushPendingBuffer(ChannelBufferOutputStream b, Channel c) {
        Channels.write(c, b.buffer());
        b.buffer().clear();
    }

    @Override
    protected void closePendingBuffer(ChannelBufferOutputStream b) {
        b.buffer().clear();
    }

    @Override
    protected void sendMessageChannel(Channel c, Object msg) {
        Channels.write(c, msg);
    }

    @Override
    protected void closeChannel(Channel c) {
        c.close();
    }

    private static void setIfNotPresent(Map<String, Object> options,
            String key, Object value, Bootstrap bootstrap) {
        if (!options.containsKey(key)) {
            bootstrap.setOption(key, value);
        }
    }
}
