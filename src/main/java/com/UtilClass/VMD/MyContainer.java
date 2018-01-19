package com.UtilClass.VMD;

import com.xuggle.xuggler.*;

import java.awt.image.BufferedImage;


/**
 * 基于本地文件封装的IContainer
 * Created by yty on 2016/12/15.
 */
public class MyContainer {
    private IContainer container;
    private IStreamCoder coder;
    private IStream stream;
    private IPacket packet;
    private IVideoResampler resampler;
    private int streamIndex;

    private String filepath;

    public MyContainer(String filepath) {
        this.filepath = filepath;
        container = IContainer.make();
        packet = IPacket.make();
    }

    public void start() {
        container.open(filepath, IContainer.Type.READ, null);
        for (int i = 0; i < container.getNumStreams(); i++) {
            stream = container.getStream(i);
            coder = stream.getStreamCoder();
            streamIndex = i;
            if (coder.getCodecType() == ICodec.Type.CODEC_TYPE_VIDEO)
                break;
        }
        setResampler();
    }

    public void stop() {
        if (container.isOpened()) {
            container.close();
        }
        if (coder != null && coder.isOpen()) {
            coder.close();
        }
    }

    public IPacket getVideoPacket() {
        while (container.readNextPacket(packet) >= 0) {
            if (packet.getStreamIndex() == streamIndex) {
                return packet;
            }
        }
        return null;
    }

    public BufferedImage decodeFrame(IPacket pac) {
        BufferedImage bi = null;
        if (!this.coder.isOpen()) {
            coder.open();
        }
        IVideoPicture picture = IVideoPicture.make(coder.getPixelType(), coder.getWidth(), coder.getHeight());
        int offset = 0;
        while (offset < pac.getSize()) {
            int bytesDecoded = coder.decodeVideo(picture, pac, offset);
            if (bytesDecoded < 0)
                throw new RuntimeException("got error decoding video in: " + pac.getPosition());
            offset += bytesDecoded;
            if (picture.isComplete()) {
                IVideoPicture newPic = picture;
                if (this.resampler != null) {
                    newPic = IVideoPicture.make(
                            resampler.getOutputPixelFormat(),
                            picture.getWidth(),
                            picture.getHeight());
                    if (resampler.resample(newPic, picture) < 0)
                        throw new RuntimeException("could not resample video from: " + pac.getPosition());
                }
                bi = Utils.videoPictureToImage(newPic);
            }
        }
        return bi;
    }

    private void setResampler() {
        if (coder == null || this.coder.getCodecType() != ICodec.Type.CODEC_TYPE_VIDEO)
            return;
        resampler = null;
        if (coder.getPixelType() != IPixelFormat.Type.BGR24) {
            resampler = IVideoResampler.make(coder.getWidth(),
                    coder.getHeight(),
                    IPixelFormat.Type.BGR24,
                    coder.getWidth(),
                    coder.getHeight(),
                    coder.getPixelType());
            if (resampler == null)
                throw new RuntimeException("could not create color space " + "resampler for: " + coder);
        }
    }


}
