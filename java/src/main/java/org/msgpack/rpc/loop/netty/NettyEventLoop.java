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

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import org.msgpack.MessagePack;
import org.msgpack.rpc.Server;
import org.msgpack.rpc.Session;
import org.msgpack.rpc.config.TcpClientConfig;
import org.msgpack.rpc.config.TcpServerConfig;
import org.msgpack.rpc.loop.EventLoop;
import org.msgpack.rpc.transport.ClientTransport;
import org.msgpack.rpc.transport.ServerTransport;

public class NettyEventLoop extends EventLoop {
    public NettyEventLoop(ExecutorService workerExecutor,
            ExecutorService ioExecutor,
            ScheduledExecutorService scheduledExecutor, MessagePack messagePack) {
        super(workerExecutor, ioExecutor, scheduledExecutor, messagePack);
    }

    private EventLoopGroup clientGroup = null;
    private EventLoopGroup serverParentGroup = null;
    private EventLoopGroup serverChildGroup = null;


    public synchronized EventLoopGroup getClientEventLoopGroup() {
        if (clientGroup == null) {
            clientGroup = new NioEventLoopGroup(); // TODO: workerCount
        }
        return clientGroup;
    }

    public synchronized EventLoopGroup getServerParentGroup() {
        if (serverParentGroup == null) {
            serverParentGroup = new NioEventLoopGroup(); // TODO: workerCount
            // messages will be dispatched to worker thread on server.
            // see useThread(true) in NettyTcpClientTransport().
        }
        return serverParentGroup;
    }

    public synchronized EventLoopGroup getServerChildGroup() {
        if (serverChildGroup == null) {
            serverChildGroup = new NioEventLoopGroup(); // TODO: workerCount
            // messages will be dispatched to worker thread on server.
            // see useThread(true) in NettyTcpClientTransport().
        }
        return serverChildGroup;
    }

    protected ClientTransport openTcpTransport(TcpClientConfig config,
            Session session) {
        return new NettyTcpClientTransport(config, session, this);
    }

    protected ServerTransport listenTcpTransport(TcpServerConfig config,
            Server server) {
        return new NettyTcpServerTransport(config, server, this);
    }
}
