package com.Experiments;

import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.fs.Path;

public class Experiment41 {

	public static void main(String[] args) throws IllegalArgumentException, IOException{
		FileWriter fw;
        //fw = new FileWriter("F:\\result\\"+"Experiment11-"+new Path(Path).getName() + ".txt",true);
        fw = new FileWriter("/home/b8311/Experiment/ExperimentResult/"+"Experiment41-"+new Path(args[0]).getName() + ".txt",true);
        
		StopWatch sw = new StopWatch();
		sw.start();
		HDFS_VideoUpload.uploadViaUtils(new Path(args[0]),new Path("hdfs://vm1:9000/yty/video/"));
		sw.stop();
		Long t=sw.getTime();
        System.out.println("total time:"+" "+t);
        fw.write("total time:"+" "+t+"\r\n");
        fw.flush();
        fw.close();
	}
}
