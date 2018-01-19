package com.VMDServiceClientAPI;

import com.UtilClass.Service.Command;
import com.UtilClass.Service.Results;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

public class ClientManageInitializer extends ChannelInitializer<SocketChannel> {
	public Results results;
	public Command command;
	public ClientManageInitializer(Command command,Results results){
		this.results=results;
		this.command=command;
	}
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new ClientManageHandler(results,command));
    }
}