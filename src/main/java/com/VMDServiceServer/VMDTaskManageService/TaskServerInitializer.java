package com.VMDServiceServer.VMDTaskManageService;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

public class TaskServerInitializer extends ChannelInitializer<SocketChannel> {
    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        socketChannel.pipeline()
                .addLast(new TaskServerHandler());
    }
}
