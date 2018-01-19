package com.VMDServiceClientAPI;

import com.UtilClass.Service.Command;
import com.UtilClass.Service.Results;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

public class ClientInitializer extends ChannelInitializer<SocketChannel> {
	public Command command;
	public Results results;
	public ClientInitializer(Command command,Results results){
		this.command=command;
		this.results=results;
	}
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new ClientHandler(command,results));
    }
}
