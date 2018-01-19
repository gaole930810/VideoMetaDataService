package com.Test;


import com.UtilClass.Service.UploadFile;
import com.UtilClass.VMD.VMDProtoUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.*;

import java.io.File;

/**
 * Created by yty on 2016/11/29.
 */
public class FullTest {
    public static final Log LOG = LogFactory.getLog(FullTest.class);

    public static void main(String[] args) {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.INFO);
        if (args.length != 2) {
            throw new IllegalArgumentException("you need a input file path and HDFS output file path!");
        }
        Path src = new Path(args[0]);

        long start;
        long end;
        File file = new File(src.toString());
        Path des = new Path(args[1] + file.getName());
        LOG.info("file name is " + file.getName());
        LOG.info("file length is " + (file.length()/1024/1024) + "MB");
        LOG.info("start uploading file......");
        start = System.currentTimeMillis();
        UploadFile.upload(src, des);
        end = System.currentTimeMillis();
        LOG.info("uploading file finished ,using " + ((end - start) / 1000) + " seconds");
/*        LOG.info("generate Secondary MetaData from local file system ......");
        start = System.currentTimeMillis();
        VMDProtoUtil.writeMeta(src);
        end = System.currentTimeMillis();
        LOG.info("generating Secondary MetaData file finished ,using " + ((end - start)) + " milliseconds");*/
        LOG.info("generate Secondary MetaData from HDFS ......");
        start = System.currentTimeMillis();
        VMDProtoUtil.writeMeta(des);
        end = System.currentTimeMillis();
        LOG.info("generating Secondary MetaData file finished ,using " + ((end - start)) + " milliseconds");
    }
}
