package com.VMDServiceServer.VMDIndexManageService;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

public class IndexServerInitializer extends ChannelInitializer<SocketChannel> {
    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        socketChannel.pipeline()
                .addLast(new IndexServerHandler());
    }
}
