package com.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.VMDServiceServer.VMDTaskManageService.Task;


public class FileIOTest {

	private String TaskLogPath="F:\\picture\\";
	public static void main(String[]args){
		new FileIOTest().InitialTaskQueue();
	}

	private void InitialTaskQueue() {
		ConcurrentLinkedQueue<Task> newTaskQueue = new ConcurrentLinkedQueue<Task>();
		try {
			if(!new File(TaskLogPath+"TaskLog.txt").exists())
				return;
			BufferedReader br = new BufferedReader(new FileReader(TaskLogPath+"TaskLog.txt"));
			String tempString = null; 
            while ((tempString = br.readLine()) != null) {
            	 Task task=new Task(tempString);
            	 if(task.getTaskStatu().equals("completed")||task.getTaskStatu().equals("failed")){
            		 newTaskQueue.poll();      		 
            	 }else{
            		 newTaskQueue.add(task);
            	 }
            }  
            br.close();			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(newTaskQueue);
		String oldfilePath=TaskLogPath+"TaskLog.txt";  
		Date date = new Date(); 
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
		String CurrentTime = format.format(date);
		String newfilepath=TaskLogPath+"TaskLog"+CurrentTime+".txt";  
		File oldfile=new File(oldfilePath);  
		File newfile=new File(newfilepath);  
		if(oldfile.exists()){  
			oldfile.renameTo(newfile);  
		}
		try {
			FileWriter fw =new FileWriter(oldfilePath,true);
			for(Task tempTask:newTaskQueue){
				fw.write(tempTask.toLogString()+"\r\n");
				fw.flush();
			}
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//
	}
}
