package com.Test;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import com.VMDServiceServer.VMDIndexManageService.IndexServer;


public class SMTest {
	public static void main(String[] args){
		new IndexServer(8000,args[0],args[1],args[2]).bind();
		return;
	}

}
