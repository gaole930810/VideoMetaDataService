package com.UtilClass.Service;

import java.io.UnsupportedEncodingException;

import io.netty.buffer.ByteBuf;

public class Command {
	public int Type;
	public String[] args;
	
	public final static int VideoUPLOAD=00;
	
	public final static int VMDGENERATE=01;
	public final static int VMDDELETE=02;
	public final static int VMDGET=03;
	public final static int VMDLS=04;
	
	
	public final static int GET_FRAME_Index=11;	
	public final static int GET_FRAME=12;
	
	public final static int GET_ALL_RedisInfo=21;
	public final static int GET_IndexServerInfo=22;
	public final static int GET_TaskServerInfo=23;
	public final static int GET_ALL_NodeInfo = 24;
	public final static int GET_Task_Info=25;
	
	public final static int ADD_VMDRE=31;
	public final static int DET_VMDRE=32;
	
	
	

	public Command(int Type, String... args) {
		this.Type = Type;
		this.args = args;
	}
	public Command(ByteBuf msg) throws UnsupportedEncodingException {
		byte[] con = new byte[msg.readableBytes()];
		// 将ByteByf信息写出到字节数组
		msg.readBytes(con);
		String[] s = new String(con, "UTF-8").split("\\+");
		this.Type = Integer.parseInt(s[0]);
		if (s.length > 1)
			this.args = new String[s.length - 1];
		for (int i = 1; i < s.length; i++) {
			this.args[i - 1] = s[i];
		}
	}

	public int getCommand() {
		return Type;
	}

	public void setCommand(int command) {
		this.Type = command;
	}

	public String[] getArgs() {
		return args;
	}

	public void setArgs(String[] args) {
		this.args = args;
	}
	public String toString(){
		String commandToString = "";
		switch (Type) {
		case 00:
			commandToString = "VideoUPLOAD";
			break;
		case 01:
			commandToString = "VMDGENERATE";
			break;
		case 02:
			commandToString = "VMDDELETE";
			break;
		case 03:
			commandToString = "VMDGET";
			break;
		case 04:
			commandToString = "VMDLS";
			break;
		case 11:
			commandToString = "GET_FRAME_Index_By_FrameNo";
			break;
		case 12:
			commandToString = "GET_FRAME_By_FrameNo";
			break;
		case 13:
			commandToString = "GET_FRAME_Index_By_FrameTime";
			break;
		case 14:
			commandToString = "GET_FRAME_By_FrameTime";
			break;
		case 21:
			commandToString = "GET_ALL_RedisInfo";
			break;
		case 22:
			commandToString = "GET_IndexServerInfo";
			break;
		case 23:
			commandToString = "GET_TaskServerInfo";
			break;
		case 24:
			commandToString = "GET_ALL_NodeInfo";
			break;
		case 25:
			commandToString = "GET_Task_Info";
			break;
		case 31:
			commandToString = "ADD_VMDRE";
			break;
		case 32:
			commandToString = "DET_VMDRE";
			break;
		}
		for(String arg:args){
			commandToString+="-"+arg;
		}
		return commandToString;
	}
}
