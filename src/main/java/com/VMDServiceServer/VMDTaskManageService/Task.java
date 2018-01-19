package com.VMDServiceServer.VMDTaskManageService;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import com.UtilClass.Service.Command;

public class Task {
	private SocketAddress socketAddress;
	private Command command;
	private String TaskStatu = "waiting";
	public Task(SocketAddress socketAddress,Command command){
		this.socketAddress = socketAddress;
		this.command = command;
	}
	public Task(String logstring){
		String[] logInfo=logstring.split("-");
		String[] socketAddressInfo=logInfo[0].split(":");
		String socketAddressIp=socketAddressInfo[0].substring(1);
		String socketAddressPort=socketAddressInfo[1];
		this.socketAddress = new InetSocketAddress(socketAddressIp,Integer.valueOf(socketAddressPort));
		//String[] commandInfo=logInfo[1].split(":");
		String Type=logInfo[1];
		int commandType=0;
		switch (Type) {
		case "VideoUPLOAD":
			commandType = Command.VideoUPLOAD;
			break;
		case "VMDGENERATE":
			commandType = Command.VMDGENERATE;
			break;
		case "VMDDELETE":
			commandType = Command.VMDDELETE;
			break;
		case "VMDGET":
			commandType = Command.VMDGET;
			break;
		case "VMDLS":
			commandType = Command.VMDLS;
			break;
		case "GET_FRAME_Index":
			commandType = Command.GET_FRAME_Index;
			break;
		case "GET_FRAME":
			commandType = Command.GET_FRAME;
			break;
		case "GET_ALL_RedisInfo":
			commandType = Command.GET_ALL_RedisInfo;
			break;
		case "GET_IndexServerInfo":
			commandType = Command.GET_IndexServerInfo;
			break;
		case "GET_TaskServerInfo":
			commandType = Command.GET_TaskServerInfo;
			break;
		case "GET_ALL_NodeInfo":
			commandType = Command.GET_ALL_NodeInfo;
			break;
		case "GET_Task_Info":
			commandType = Command.GET_Task_Info;
			break;
		case "ADD_VMDRE":
			commandType = Command.ADD_VMDRE;
			break;
		case "DET_VMDRE":
			commandType = Command.DET_VMDRE;
			break;
		}
		String[] commandArgs=new String[logInfo.length-3];
		for(int i=0;i<commandArgs.length;i++){
			commandArgs[i]=logInfo[i+2];
		}
		this.command = new Command(commandType,commandArgs);
		this.setTaskStatu(logInfo[logInfo.length-1]);
	}
	public SocketAddress getClientIP() {
		return socketAddress;
	}
	public Command getCommand() {
		return command;
	}
	public String getTaskStatu() {
		return TaskStatu;
	}
	public void setTaskStatu(String taskStatu) {
		TaskStatu = taskStatu;
	}
	public String toLogString(){
		String logstring="";
		logstring+=socketAddress.toString();
		logstring+="-"+command.toString();
		logstring+="-"+TaskStatu;
		return logstring;
	}
	public static void main(String[]args){
		Task task=new Task(new InetSocketAddress("172.16.10.101",8000),new Command(Command.GET_FRAME,"localhostfile","100","2"));
		String logString=task.toLogString();
		System.out.println(logString);
		System.out.println(new Task(logString).toLogString());
		System.out.println(new Task(logString).getClientIP());
		System.out.println(new Task(logString).getCommand());
		System.out.println(new Task(logString).getTaskStatu());
	}
}
