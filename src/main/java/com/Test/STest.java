package com.Test;

import com.VMDServiceServer.VMDTaskManageService.TaskServer;

public class STest {
	public static void main(String[] args){
		new TaskServer(8000,args[0],args[1],args[2],args[3]).bind();
		return;
	}

}
