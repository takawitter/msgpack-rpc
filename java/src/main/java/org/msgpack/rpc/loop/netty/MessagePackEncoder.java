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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.MessageBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import org.msgpack.MessagePack;
import org.msgpack.type.Value;

public class MessagePackEncoder extends MessageToMessageEncoder<Value> {
    private final int estimatedLength;

    private MessagePack messagePack;

    public MessagePackEncoder(MessagePack messagePack) {
        this(1024, messagePack);
    }

    public MessagePackEncoder(int estimatedLength, MessagePack messagePack) {
        this.estimatedLength = estimatedLength;
        this.messagePack = messagePack;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Value msg,
            MessageBuf<Object> out) throws Exception {
        ByteBuf buf = ctx.alloc().buffer(estimatedLength);
        messagePack.write(new ByteBufOutputStream(buf), msg);
        out.add(buf);
    }
}
