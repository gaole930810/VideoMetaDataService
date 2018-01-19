package com.Test;

import com.UtilClass.Service.UploadFile;
import com.UtilClass.VMD.VMDProtoUtil;
import com.Proto.SecondaryMetaClass;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * 这个实验是用来对大规模文件进行上传测试的。（上传video-服务端生成元数据文件-上传元数据文件）
 * 记录内容包括（均为avg）：
 * 1.单独上传时间（单位毫秒）
 * 2.基于二级元数据的上传时间（单位毫秒）
 * 3.二级元数据采样时间（单位毫秒）
 * 4.I帧采样率和I帧数量。
 * 5.平均文件时长。
 * 6.I帧字典规模，总体规模。
 * Created by yty on 2016/12/10.
 */
public class ExpOne {
    private List<File> fileList = new ArrayList<>();
    private static final Log LOG = LogFactory.getLog(ExpOne.class);

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

    public static void main(String[] args) {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.INFO);
        String path = args[0];
        long uploadedFileNum = 1;
        long uploadedTimeMilliSeconds = 0;
        long genMetaTimeMilliSeconds = 0;
        long keyPacketDictSize = 0;
        long uploadWithMetaMilliSeconds = 0;
        long fileDuration = 0;
        long keyPacketNum = 0;
        double keyPacketSampler = 0.0;
        long FileLength = 0;
        long metaLength = 0;
        ExpOne one = new ExpOne();
        File dir = new File(path);
        one.getAllFiles(dir);
        long start;
        long end;
        String basicDes = "hdfs://vm1:9000/yty/video/";
        for (File file : one.fileList) {
            FileLength = FileLength + file.length();
            // 1. 上传文件并统计时间
            start = System.currentTimeMillis();
            UploadFile.uploadViaUtils(new Path(file.toString()), new Path(basicDes + file.getName()));
            end = System.currentTimeMillis();
            LOG.info("No." + uploadedFileNum + " file upload success , using " + (end - start) + " milliseconds");
            uploadedTimeMilliSeconds += end - start;
            // 2. 基于二级元数据的上传方式，分别统计总体用时和生成二级元数据用时
            start = System.currentTimeMillis();
            UploadFile.uploadViaUtils(new Path(file.toString()), new Path(basicDes + file.getName()));
            long ss = System.currentTimeMillis();
            SecondaryMetaClass.SecondaryMeta sm = VMDProtoUtil.writeMeta(new Path(file.toString()));
            end = System.currentTimeMillis();
            metaLength = metaLength + (long) sm.getSerializedSize();
            LOG.info("No." + uploadedFileNum + " file upload with meta success , using " + (end - start) + " milliseconds, "
                    + "using " + (end - ss) + " milliseconds to generate and upload Meta file");
            genMetaTimeMilliSeconds += (end - ss);
            uploadWithMetaMilliSeconds += (end - start);
            // 3. 根据元数据统计keyPacket采样率
            keyPacketSampler = keyPacketSampler + ((double) sm.getFrameMetaInfoCount() / (double) sm.getFrameNumber());
            // 4. 统计二级元数据中key packet字典的规模和数量
            keyPacketDictSize += sm.getFrameMetaInfoList().stream()
                    .map(SecondaryMetaClass.SecondaryMeta.FrameInfoGroup::getSerializedSize)
                    .reduce(0, Integer::sum);
            keyPacketNum += sm.getFrameMetaInfoCount();
            // 5. 统计文件的平均时长
            fileDuration += sm.getTimestamp();
            uploadedFileNum++;
        }
        uploadedFileNum--;
        LOG.info("------------------------------");
        LOG.info("There are total " + uploadedFileNum + " files uploaded");
        LOG.info("Average file size is " + (FileLength / uploadedFileNum / 1024 / 1024) + " MB");
        LOG.info("Average uploading time is " + (uploadedTimeMilliSeconds / uploadedFileNum) + " milliseconds");
        LOG.info("Average video secondary meta size is  " + (metaLength / uploadedFileNum) + " B");
        LOG.info("Average key packet dictionary size is  " + (keyPacketDictSize / uploadedFileNum ) + " B");
        LOG.info("Average I frame number is " + (keyPacketNum / uploadedFileNum));
        LOG.info("Average uploading with meta time is " + (uploadWithMetaMilliSeconds / uploadedFileNum) + " milliSeconds");
        LOG.info("Average generating vsm time is " + (genMetaTimeMilliSeconds / uploadedFileNum) + " milliseconds");
        LOG.info("Average KeyPacket sample rate is " + (keyPacketSampler / uploadedFileNum));
        LOG.info("Average file duration is " + (fileDuration / uploadedFileNum));
        LOG.info("------------------------------");
    }
}
