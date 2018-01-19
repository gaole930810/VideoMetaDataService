package com.UtilClass.Service;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import java.io.*;
import java.net.URI;

/**
 * 关于上传文件的相关方法。
 * Created by yty on 2016/11/29.
 */
public class UploadFile {
    private final static Log LOG = LogFactory.getLog(UploadFile.class);
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

    /**
     * 生成文件摘要
     * 先读取一个规模为10MB的字节数组。
     * 使用 DigestUtils 类对其生成SHA256散列值作为文件摘要。
     *
     * @param src 文件路径
     * @return 文件摘要
     */
    public static String generateSummary(Path src) {
        String summary = "";
        long start;
        long end;
        try {
            FileSystem hdfs = FileSystem.get(ConfUtil.generate());
            FSDataInputStream fsin = hdfs.open(src);
            BufferedInputStream bin = new BufferedInputStream(fsin);
            byte[] buffer = new byte[bufferSize];
            int r = bin.read(buffer);
            if (r < 0)
                throw new IOException("Can not read the file");
            summary = DigestUtils.sha256Hex(buffer);
            LOG.debug(summary);
            fsin.close();

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return summary;
    }

    public static String generateSummary(File src) {
        String summary = "";
        long start;
        long end;
        try {
            File file = src;
            FileInputStream fin = new FileInputStream(file);
            BufferedInputStream bin = new BufferedInputStream(fin);
            byte[] buffer = new byte[bufferSize];
            start = System.nanoTime();
            int r = bin.read(buffer);
            end = System.nanoTime();
            LOG.debug("reading buffer using " + (end - start) + " nanoSeconds");
            if (r < 0)
                throw new IOException("Can not read the file");
            start = System.currentTimeMillis();
            summary = DigestUtils.sha256Hex(buffer);
            end = System.currentTimeMillis();
            LOG.debug("computing buffer SHA256 , using " + (end - start) + " milliSeconds");
            LOG.debug(summary);
            fin.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return summary;
    }
}
