package com.Test;

import com.Proto.SecondaryMetaClass;
import com.UtilClass.Service.ConfUtil;
import com.UtilClass.Service.UploadFile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 测试读取性能。
 * 1. HDFS原生API下载时间性能测试。
 * 2. 生成摘要-得到二级元数据-读取数据。
 * Created by yty on 2016/12/13.
 */
public class ExpThree {
    private static final Log LOG = LogFactory.getLog(ExpThree.class);
    private static int count = 1;
    public List<File> fileList = new ArrayList<>();
    private void getAllFiles(File directory) {
        if(directory == null)
            return ;
        if (directory.isDirectory()) {
            for (File file : directory.listFiles()) {
                getAllFiles(file);
            }
        } else {
            fileList.add(directory);
        }
    }
    public static void main(String[] args) throws IOException {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.INFO);
        ExpThree exp = new ExpThree();
        exp.getAllFiles(new File(args[0]));
        List<List<Integer>> list = new ArrayList<>();
        int hdfsAPIDownloadTime = 0;
        int generateVideoSummaryTime = 0;
        int getMetaDataFileTime = 0;
        int TotalTime = 0;
        for (File file : exp.fileList) {
            list.add(calc(file.getName()));
        }
        for (List<Integer> res : list) {
            hdfsAPIDownloadTime += res.get(0);
            generateVideoSummaryTime += res.get(1);
            getMetaDataFileTime += res.get(2);
            TotalTime += res.get(3);
        }
        LOG.info("-----------------------------------------------");
        LOG.info("average using hdfs api downloading time is " + (hdfsAPIDownloadTime / exp.fileList.size()) + " ms");
        LOG.info("average generating video summary from hdfs time is " + (generateVideoSummaryTime / exp.fileList.size()) + " ms");
        LOG.info("average getting meta data file time is " + (getMetaDataFileTime / exp.fileList.size()) + " ms");
        LOG.info("average total time with meta is " + (TotalTime / exp.fileList.size()) + " ms");
        LOG.info("-----------------------------------------------");
    }

    public static List<Integer> calc(String name) throws IOException {
        List<Integer> result = new ArrayList<>();
        int hdfsAPIDownloadTime = 0;
        int generateVideoSummaryTime = 0;
        int getMetaDataFileTime = 0;
        int TotalTime = 0;
        long start;
        long end;
        String base = "hdfs://vm1:9000/yty/video/";
        String metabase = "hdfs://vm1:9000/yty/meta/";
        // 1. 统计HDFS原生API的下载时间
        FileSystem hdfs = FileSystem.get(ConfUtil.generate());
        start = System.currentTimeMillis();
        hdfs.copyToLocalFile(new Path(base + name), new Path("tt.mkv"));
        end = System.currentTimeMillis();
        hdfsAPIDownloadTime += end - start;
        File file = new File("tt.mkv");
        file.delete();
        result.add(hdfsAPIDownloadTime);

        // 2. 统计基于二级元数据的下载时间

        start = System.currentTimeMillis();
        long ss = start;
        String summary = UploadFile.generateSummary(new Path(base + name));
        end = System.currentTimeMillis();
        generateVideoSummaryTime += end - start;
        result.add(generateVideoSummaryTime);
        start = System.currentTimeMillis();
        SecondaryMetaClass.SecondaryMeta SM = SecondaryMetaClass.SecondaryMeta.parseFrom(hdfs.open(new Path(metabase + summary)));
        end = System.currentTimeMillis();
        getMetaDataFileTime += end - start;
        result.add(getMetaDataFileTime);
        hdfs.copyToLocalFile(new Path(base + name), new Path("tt.mkv"));
        end = System.currentTimeMillis();
        TotalTime += end - ss;
        result.add(TotalTime);
        file = new File("tt.mkv");
        file.delete();
        LOG.info("-----------------------------------------------");
        LOG.info("No." + count++);
        LOG.info(result);
        LOG.info("-----------------------------------------------");
        return result;
    }
}
