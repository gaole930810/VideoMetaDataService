package com.Experiments;

import java.io.IOException;
import org.apache.hadoop.fs.Path;

import com.VMDServiceClientAPI.ClientAPI;


public class VMD_VideoUploadAndVmdGenerate {
	public static void main(String[] args) throws IOException {
		String src="C:\\Users\\gaole\\Desktop\\780copy.mp4";
		String des="hdfs://vm1:9000/yty/video/";
		
		ClientAPI.VideoUPLOAD(src,des).print();
		
	}
}
