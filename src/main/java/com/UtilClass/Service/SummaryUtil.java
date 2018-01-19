package com.UtilClass.Service;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.ProtocolException;

/**
 * 这是一个关于摘要生成的Util类
 * Created by yuan on 17-6-12.
 */
public class SummaryUtil {
    private final static Log LOG = LogFactory.getLog(SummaryUtil.class);
    private final static int bufferSize = 10 * 1024 * 1024;

    /**
     * 这个方法用来生成文件的摘要，采用前10MB的数据，使用SHA-256散列算法生成摘要。
     *
     * @param path     文件的路径
     * @param protocol 协议，如果是本地路径就是file，如果是hdfs路径就是hdfs
     * @param hdfs     如果是hdfs协议，直接提供一个FileSystem的实例。
     * @return 文件的摘要字符串
     */
    public static String generate(String path, String protocol, FileSystem hdfs) {
        if (protocol.equals("hdfs") && hdfs == null)
            throw new IllegalArgumentException("You cannot access HDFS without FileSystem instance!");
        LOG.info("Using " + protocol + " protocol, the file path is " + path);
        String summary = "";
        try {
            InputStream in;
            switch (protocol) {
                case "file":
                    in = new FileInputStream(new File(path));
                    break;
                case "hdfs":
                    in = hdfs.open(new Path(path));
                    break;
                default:
                    throw new ProtocolException("Can not identify the protocol name");
            }
            BufferedInputStream bin = new BufferedInputStream(in);
            byte[] buffer = new byte[bufferSize];
            int r = bin.read(buffer);
            if (r < 0)
                throw new IOException("Can not read the file");
            summary = DigestUtils.sha256Hex(buffer);
            LOG.info("The SHA-256 file summary is " + summary);
            bin.close();
            in.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            LOG.error("File doesn't exists!");
        } catch (IOException ioe) {
            ioe.printStackTrace();
            LOG.error("Can not open the file in HDFS!");
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Can not identify the protocol name!");
        }
        return summary;
    }
}
