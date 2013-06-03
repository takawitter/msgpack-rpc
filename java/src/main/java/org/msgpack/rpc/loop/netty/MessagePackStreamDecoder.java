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
import io.netty.buffer.MessageBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.nio.ByteBuffer;

import org.msgpack.MessagePack;
import org.msgpack.type.Value;
import org.msgpack.unpacker.Unpacker;

public class MessagePackStreamDecoder extends ByteToMessageDecoder {
    protected MessagePack msgpack;

    public MessagePackStreamDecoder(MessagePack msgpack) {
        super();
        this.msgpack = msgpack;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in,
            MessageBuf<Object> out) throws Exception {
        // TODO #MN will modify the body with MessagePackBufferUnpacker.
        if (!buffer.hasRemaining()) {
            return;
        }
        source.markReaderIndex();

        byte[] bytes = buffer.array(); // FIXME buffer must has array
        int offset = buffer.arrayOffset() + buffer.position();
        int length = buffer.arrayOffset() + buffer.limit();
        ByteArrayInputStream stream = new ByteArrayInputStream(bytes, offset,
                length);
        int startAvailable = stream.available();
        try{
            Unpacker unpacker = msgpack.createUnpacker(stream);
            Value v = unpacker.readValue();
            source.skipBytes(startAvailable - stream.available());
            return v;
        }catch( EOFException e ){
            // not enough buffers.
            // So retry reading
            source.resetReaderIndex();
            return null;
        }
    }
}
