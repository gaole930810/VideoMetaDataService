package com.Test;

import com.UtilClass.Service.ConfUtil;
import com.UtilClass.VMD.HDFSProtocolHandlerFactory;
import com.UtilClass.VMD.VMDProtoUtil;
import com.UtilClass.VMD.XugglerDecompressor;
import com.UtilClass.VMD.XugglerInputStream;
import com.Proto.SecondaryMetaClass;
import com.xuggle.xuggler.*;
import com.xuggle.xuggler.io.URLProtocolManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * 这个实验主要是验证利用视频元数据来测试分布式读取的相关性能
 * 针对某一个视频：
 * 1. 得到元数据字典。
 * 2. 使用HDFSProtocolHandler来实现分布式读取。
 * 3. 测试的方法是对于每一个key Packet位置，解码50帧，记录总共的时间消耗情况。
 * Created by yty on 2016/12/11.
 */
public class ExpTwo {
    private static final Log LOG = LogFactory.getLog(ExpTwo.class);
    public String filename;

    public ExpTwo(String filename) {
        this.filename = filename;
    }

    public static void main(String[] args) {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.INFO);
        ExpTwo exp = new ExpTwo("Test.mkv");
        SecondaryMetaClass.SecondaryMeta sm = VMDProtoUtil.getMeta(new Path("hdfs://vm1:9000/yty/video/" + exp.filename));
        List<SecondaryMetaClass.SecondaryMeta.FrameInfoGroup> fig = sm.getFrameMetaInfoList();
        XugglerDecompressor dec = new XugglerDecompressor(fig);
        int times = 0;
        List<Long> xis = new ArrayList<>();
        List<Long> handler = new ArrayList<>();
        for (double i = 0.0; i <= 1.0; i += 0.01) {
            SecondaryMetaClass.SecondaryMeta.FrameInfoGroup f = fig.get((int) (fig.size() * i));
            long pos = f.getStartIndex();
            LOG.info(pos);
            LOG.info("-----------------------------------------------");
            xis.add(FindPosAndDecode50FramesWithInputStream(pos, exp.filename, dec, times));
            handler.add(FindPosAndDecode50FramesWithHandler(pos, exp.filename, times));
            LOG.info("-----------------------------------------------");
            times++;
        }
        LOG.info("-----------------------------------------------");
        LOG.info("xis average: " + xis);
        LOG.info("handler average: " + handler);
        LOG.info("-----------------------------------------------");

    }

    public static long FindPosAndDecode50FramesWithInputStream(long pos, String filename, XugglerDecompressor dec, int times) {
        long ss = 0;
        long ee = 0;
        Path path = new Path("hdfs://vm1:9000/yty/video/" + filename);
        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(ConfUtil.generate());
        } catch (IOException e) {
            e.printStackTrace();
        }

        try (FSDataInputStream fileIn = hdfs.open(path)) {
            long end = hdfs.getFileStatus(path).getLen();
            long start = pos;
            ss = System.currentTimeMillis();
            XugglerInputStream xis = new XugglerInputStream(fileIn, start, end, 0L, dec);
            int count = 0;
            ee = System.currentTimeMillis();
            while (count < 50) {
                if (xis.readVideoFrame() == null)
                    continue;
                count++;
            }

            LOG.info("No." + times + " xis: " + (ee - ss) + " ms");
            xis.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ee - ss;
    }

    public static long FindPosAndDecode50FramesWithHandler(long pos, String filename, int times) {
        long start = 0L;
        long end = 0L;
        long ee = 0L;
        boolean isLocated = false;
        start = System.currentTimeMillis();
        URLProtocolManager mgr = URLProtocolManager.getManager();
        mgr.registerFactory("hdfs", new HDFSProtocolHandlerFactory());
        IContainer container = IContainer.make();
        String path = "hdfs://vm1:9000/yty/video/" + filename;
        container.open(path, IContainer.Type.READ, null);
        int decodedFrameNum = 0;
        for (int i = 0; i < container.getNumStreams(); i++) {
            IStream stream = container.getStream(i);
            IStreamCoder coder = stream.getStreamCoder();
            if (coder.getCodecType() != ICodec.Type.CODEC_TYPE_VIDEO)
                continue;
            IPacket packet = IPacket.make();
            coder.open();
            IVideoPicture picture = IVideoPicture.make(coder.getPixelType(),
                    coder.getWidth(), coder.getHeight());
            container.seekKeyFrame(i, pos, pos, pos, IContainer.SEEK_FLAG_BYTE);
            while (container.readNextPacket(packet) >= 0) {
                if (packet.getStreamIndex() == i) {
                    if (packet.getPosition() < pos)
                        continue;
                    if (!isLocated) {
                        ee = System.currentTimeMillis();
                        isLocated = true;
                    }
                    int offset = 0;
                    while (offset < packet.getSize()) {
                        int bytesDecoded = coder.decodeVideo(picture, packet, offset);
                        if (bytesDecoded < 0)
                            throw new RuntimeException("got error decoding video in: " + packet.getPosition());
                        offset += bytesDecoded;
                        if (picture.isComplete()) {
                            Utils.videoPictureToImage(picture);
                            decodedFrameNum++;
                        }
                    }
                    if (decodedFrameNum == 50) {
                        break;
                    }
                }
            }
            coder.close();
        }
        end = System.currentTimeMillis();
        LOG.info("No." + times + " handler: " + (end - start) + " ms");
        LOG.info("No." + times + " handler , finding pos using : " + (ee - start) + " ms");
        container.close();
        return end - start;
    }
}
