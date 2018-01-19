package com.VMDServiceServer.VMDTaskManageService;

import com.UtilClass.Service.Command;
import com.UtilClass.Service.Results;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

public class TaskServerMessageInitializer extends ChannelInitializer<SocketChannel> {
	public Results results;
	public Command command;
	public TaskServerMessageInitializer(Command command,Results results){
		this.results=results;
		this.command=command;
	}
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new TaskServerMessageHandler(results,command));
    }
}
