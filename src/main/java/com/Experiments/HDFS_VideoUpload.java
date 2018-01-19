package com.Experiments;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.UtilClass.Service.ConfUtil;

public class HDFS_VideoUpload {
	private final static Log LOG = LogFactory.getLog(HDFS_VideoUpload.class);
    private static int bufferSize = 10 * 1024 * 1024;

    /**
     * 利用FSDataInputStream上传文件
     *
     * @param src 文件本地路径
     * @param des hdfs目标路径
     */
    public static void upload(Path src, Path des) {
        try {
            String uri = "hdfs://vm1:9000";
            FileSystem hdfs = FileSystem.get(URI.create(uri), ConfUtil.generate());
            FSDataOutputStream fsout = hdfs.create(des, true);
            File file = new File(src.toString());
            Long fileLength = file.length();
            InputStream in = new BufferedInputStream(new FileInputStream(file));
            byte[] buffer = new byte[bufferSize];
            Long curLength = 0L;
            int cur;
            while ((cur = in.read(buffer)) > 0) {
                fsout.write(buffer);
                curLength += cur;
                LOG.debug((curLength * 100 / fileLength) + "% has been upload ");
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    /**
     * 利用hdfs提供的客户端方法copyFromLocalFile上传文件
     *
     * @param src 本地路径
     * @param des hdfs目标路径
     */
    public static void uploadViaUtils(Path src, Path des) {
        try {
            FileSystem hdfs = FileSystem.get(ConfUtil.generate());
            LOG.debug("Start Uploading");
            hdfs.copyFromLocalFile(false, true, src, des);
            LOG.debug("Uploading Finished");
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
	public static void main(String[] args){
		uploadViaUtils(new Path("C:\\Users\\gaole\\Desktop\\780copy.mp4"),new Path("hdfs://vm1:9000/yty/video/"));
	}

}
