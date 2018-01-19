package com.Test;

import com.UtilClass.Service.ConfUtil;
import com.UtilClass.Service.UploadFile;
import com.UtilClass.VMD.XugglerDecompressor;
import com.UtilClass.VMD.XugglerInputStream;
import com.Proto.SecondaryMetaClass;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.*;

import javax.imageio.ImageIO;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import java.awt.image.BufferedImage;


/**
 * 元数据利用测试
 * Created by yty on 2016/12/2.
 */
public class MetaUtilizationTest {
    private static final Log LOG = LogFactory.getLog(MetaUtilizationTest.class);

    /*
       1.如果没有元数据，则先生成元数据
       2.提取出元数据中的帧号和偏移量。
       3.根据偏移量读取出视频帧并生成图片。
    */
    public static void main(String[] args) throws IOException {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.INFO);
        Path path = new Path(args[1]);
        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(ConfUtil.generate());
            if (!hdfs.exists(path)) {
                throw new FileNotFoundException("the file doesn't exist !");
            }
            if (hdfs.isDirectory(path)) {
                throw new FileNotFoundException("it is not a file URL");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        String summary = UploadFile.generateSummary(path);
        LOG.info(summary);
//        try {
//            if (!hdfs.exists(new Path(ConfUtil.defaultFS + "/yty/meta/" + summary))) {
//                VMDProtoUtil.writeMeta(path);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        SecondaryMetaClass.SecondaryMeta sm = SecondaryMetaClass.SecondaryMeta.parseFrom(hdfs.open(new Path(ConfUtil.defaultFS + "/yty/meta/" + summary)));
        List<SecondaryMetaClass.SecondaryMeta.FrameInfoGroup> fig = sm.getFrameMetaInfoList();
        LOG.info(fig.size());
        XugglerDecompressor dec = new XugglerDecompressor(fig);
        FSDataInputStream fileIn = hdfs.open(path);
        long test_start = fig.get(Integer.parseInt(args[0])).getStartIndex();
        LOG.info("start : " + test_start);
        long test_end = fig.get(fig.size() - 1).getStartIndex();
        LOG.info("end : " + test_end);
        XugglerInputStream xis = new XugglerInputStream(fileIn, test_start, test_end, 0L, dec);
        BufferedImage bi;
        int count = 0;
        long start = System.currentTimeMillis();
        while (true) {
            bi = xis.readVideoFrame();
            if (bi == null)
                continue;
            File file = new File("Test/" + count + ".jpg");
            ImageIO.write(bi, "jpg", file);
            count++;
            LOG.info(count);
            if (count == 200) break;
        }
        long end = System.currentTimeMillis();
        LOG.info("total cost " + (end - start) / 1000 + " seconds");
        xis.close();
//        int count = 0;
//        URLProtocolManager mgr = URLProtocolManager.getManager();
//        mgr.registerFactory("hdfs", new HDFSProtocolHandlerFactory());
//        IContainer container = IContainer.make();
//        container.open(path.toString(), IContainer.Type.READ, null);
//        for (int i = 0; i < container.getNumStreams(); i++) {
//            IStream stream = container.getStream(i);
//            IStreamCoder coder = stream.getStreamCoder();
//            if (coder.getCodecType() != ICodec.Type.CODEC_TYPE_VIDEO)
//                continue;
//            IPacket packet = IPacket.make();
//            coder.open();
//            LOG.info("有多少个入口点： " + stream.getNumIndexEntries());
//            long start = System.currentTimeMillis();
//            while (container.readNextPacket(packet) >= 0) {
//                if (packet.getStreamIndex() == i) {
//                    IVideoPicture picture = IVideoPicture.make(coder.getPixelType(),
//                            coder.getWidth(), coder.getHeight());
//                    int offset = 0;
//                    //LOG.info(packet.getSize());
//                    while (offset < packet.getSize()) {
//                        int bytesDecoded = coder.decodeVideo(picture, packet, offset);
//                        //LOG.info("decoded size : " + bytesDecoded);
//                        if (bytesDecoded < 0)
//                            throw new RuntimeException("got error decoding video in: "
//                                    + packet.getPosition());
//                        offset += bytesDecoded;
//                        if (picture.isComplete()) {
//                            LOG.info("picture is complated, No." + count);
//                            IVideoPicture newPic = picture;
//                            if (coder.getCodecType() != ICodec.Type.CODEC_TYPE_VIDEO)
//                                return;
//                            IVideoResampler resampler = null;
//                            if (coder.getPixelType() != IPixelFormat.Type.BGR24) {
//                                resampler = IVideoResampler.make(coder.getWidth(),
//                                        coder.getHeight(), IPixelFormat.Type.BGR24,
//                                        coder.getWidth(), coder.getHeight(),
//                                        coder.getPixelType());
//                                if (resampler == null)
//                                    throw new RuntimeException("could not create color space "
//                                            + "resampler for: " + coder);
//                            }
//                            if (resampler != null) {
//                                newPic = IVideoPicture.make(
//                                        resampler.getOutputPixelFormat(),
//                                        picture.getWidth(), picture.getHeight());
//                                if (resampler.resample(newPic, picture) < 0)
//                                    throw new RuntimeException(
//                                            "could not resample video from: "
//                                                    + packet.getPosition());
//                            }
//
//                            BufferedImage bi = Utils.videoPictureToImage(newPic);
//                            ImageIO.write(bi, "jpg", new File(count++ + ".jpg"));
//                        }
//                    }
//                }
//            }
//            long end = System.currentTimeMillis();
//            LOG.info("totally cost " + (end-start)/1000 + " seconds");
    }
}

