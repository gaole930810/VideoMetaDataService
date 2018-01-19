package com.Test;

import com.UtilClass.Service.ConfUtil;
import com.UtilClass.VMD.HDFSProtocolHandlerFactory;
import com.UtilClass.VMD.VMDProtoUtil;
import com.Proto.SecondaryMetaClass;
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

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 测试通过元数据来解码
 * Created by yty on 2016/12/13.
 */
public class ExpFour {
    private static final Log LOG = LogFactory.getLog(ExpFour.class);

    public static void main(String[] args) throws IOException {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.INFO);
        List<Long> t1 = new ArrayList<>();
        String base = "hdfs://vm1:9000/yty/video/";
        String filename = "test.mkv";
        FileSystem hdfs = FileSystem.get(ConfUtil.generate());
        Path path = new Path(base + filename);
        SecondaryMetaClass.SecondaryMeta sm = VMDProtoUtil.getMeta(path);
        List<SecondaryMetaClass.SecondaryMeta.FrameInfoGroup> fig = sm.getFrameMetaInfoList();
        URLProtocolManager mgr = URLProtocolManager.getManager();
        mgr.registerFactory("hdfs", new HDFSProtocolHandlerFactory());
        IContainer container = IContainer.make();
        container.open(path.toString(), IContainer.Type.READ, null);
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
            IVideoResampler resampler = null;
            if (coder.getPixelType() != IPixelFormat.Type.BGR24) {
                resampler = IVideoResampler.make(coder.getWidth(),
                        coder.getHeight(), IPixelFormat.Type.BGR24,
                        coder.getWidth(), coder.getHeight(),
                        coder.getPixelType());
                if (resampler == null)
                    throw new RuntimeException("could not create color space "
                            + "resampler for: " + coder);
            }
            for (double j = 0.01; j <= 1.0; j += 0.01) {
                long pos = fig.get((int) (fig.size() * j)).getStartIndex();
                container.seekKeyFrame(i, pos, pos, pos, IContainer.SEEK_FLAG_BYTE);
                LOG.info("---------------  " + j + "  --------------");
                long start = System.currentTimeMillis();
                decodedFrameNum = 0;
                while (container.readNextPacket(packet) >= 0) {
                    if (packet.getStreamIndex() == i) {
                        if (packet.getPosition() < pos)
                            continue;
                        int offset = 0;
                        while (offset < packet.getSize()) {
                            int bytesDecoded = coder.decodeVideo(picture, packet, offset);
                            if (bytesDecoded < 0)
                                throw new RuntimeException("got error decoding video in: " + packet.getPosition());
                            offset += bytesDecoded;
                            if (picture.isComplete()) {
                                IVideoPicture newPic = picture;
                                newPic = IVideoPicture.make(
                                        resampler.getOutputPixelFormat(),
                                        picture.getWidth(),
                                        picture.getHeight());
                                if (resampler.resample(newPic, picture) < 0)
                                    throw new RuntimeException("could not resample video from: " + packet.getPosition());
                                BufferedImage bi = Utils.videoPictureToImage(newPic);
                                decodedFrameNum++;
                            }
                        }
                        if (decodedFrameNum == 50) {
                            break;
                        }
                    }
                }
                long end = System.currentTimeMillis();
                t1.add(end - start);
            }
            coder.close();
        }
        container.close();
        LOG.info(t1);
    }
}
