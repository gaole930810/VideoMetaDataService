package com.Test;

import com.UtilClass.Service.ConfUtil;
import com.UtilClass.VMD.HDFSProtocolHandlerFactory;
import com.xuggle.xuggler.*;
import com.xuggle.xuggler.io.URLProtocolManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by yty on 2016/12/8.
 */
public class KeyFrameTest {
    private static final Log LOG = LogFactory.getLog(KeyFrameTest.class);

    public static void main(String[] args) {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.INFO);
        Path path = new Path("hdfs://vm1:9000/gl/780.mp4");
        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(ConfUtil.generate());
            if (!hdfs.exists(path)) {
                throw new FileNotFoundException("the file doesn't exist !");
            }
            if (hdfs.isDirectory(path)) {
                throw new FileNotFoundException("it is not a file URL");
            }
            System.out.println("已连接");
        } catch (IOException e) {
            e.printStackTrace();
        }
        int count = 0;
        URLProtocolManager mgr = URLProtocolManager.getManager();
        mgr.registerFactory("hdfs", new HDFSProtocolHandlerFactory());
        IContainer container = IContainer.make();
        container.open(path.toString(), IContainer.Type.READ, null);
        for (int i = 0; i < container.getNumStreams(); i++) {
            IStream stream = container.getStream(i);
            IStreamCoder coder = stream.getStreamCoder();
            if (coder.getCodecType() != ICodec.Type.CODEC_TYPE_VIDEO)
                continue;
            IPacket packet = IPacket.make();
            coder.open();
            LOG.info("有多少个入口点： " + stream.getNumIndexEntries());
            long start = System.currentTimeMillis();
            while (container.readNextPacket(packet) >= 0) {
                if (packet.getStreamIndex() == i) {
                    if(packet.isKeyPacket()){
                        LOG.info("No."+ count + " is key packet");
                    }
                    count++;
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
//                            LOG.info("picture is complated, No." + count++);
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
//                            BufferedImage bi = Utils.videoPictureToImage(newPic);
//                        }
//                    }
                }
            }
        }
    }
}
