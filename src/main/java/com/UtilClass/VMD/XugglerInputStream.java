package com.UtilClass.VMD;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;

import com.xuggle.xuggler.Global;
import com.xuggle.xuggler.ICodec;
import com.xuggle.xuggler.ICodec.Type;
import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IError;
import com.xuggle.xuggler.IPacket;
import com.xuggle.xuggler.IPixelFormat;
import com.xuggle.xuggler.IStream;
import com.xuggle.xuggler.IStreamCoder;
import com.xuggle.xuggler.IVideoPicture;
import com.xuggle.xuggler.IVideoResampler;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;

/**
 * Created by yty on 2016/12/7.
 */
public class XugglerInputStream extends SplitCompressionInputStream {
    private static final Log LOG = LogFactory.getLog(XugglerInputStream.class);
    // 视频元数据中的字典。
    private XugglerDecompressor decompressor;

    private long frame_offset;

    private IContainer container;

    private IStream[] streams;

    private int videoStreamId;

    private IStreamCoder coder[];

    private IVideoResampler resampler;

    private XugglerFSDataInputStream xfsdis;

    private Type[] streamTypes;

    private boolean finished;

    public XugglerInputStream(InputStream in, long start, long end, long frame_offset, XugglerDecompressor dec)
            throws IOException {
        super(in, start, end);

        this.frame_offset = frame_offset;

        decompressor = dec;

        FSDataInputStream fIn = (FSDataInputStream) in;

        int header_size = (int) dec.getHeaderSize();
        byte[] header = new byte[header_size];

        // 读取视频头
        fIn.seek(0);
        int b = fIn.read(header, 0, header_size);
        LOG.debug("header read, count bytes: " + b);
        LOG.debug("after header reading, fIn.pos = " + fIn.getPos());

        xfsdis = new XugglerFSDataInputStream(in, start, end);

        xfsdis.setHeader(header);

        this.container = IContainer.make();

        int r = this.container.open(xfsdis, null);

        LOG.info("container.open = " + r);

        if (r < 0) {
            IError error = IError.make(r);
            LOG.debug("error: " + error.getDescription());
            throw new IOException("Could not create Container from given InputStream");
        }

        // 存储一些容器信息，比如多少条流，什么编码器，总之就是各种解码可能需要的信息。
        int numStreams = this.container.getNumStreams();
        this.streams = new IStream[numStreams];
        this.coder = new IStreamCoder[numStreams];
        this.streamTypes = new Type[numStreams];


        for (int s = 0; s < this.streams.length; s++) {
            this.streams[s] = this.container.getStream(s);
//            if (this.streams[s].getStreamCoder().getCodecType() == Type.CODEC_TYPE_VIDEO) {
//                int er = this.container.seekKeyFrame(s, start, start, end, IContainer.SEEK_FLAG_BYTE);
//                if (er < 0) {
//                    throw new RuntimeException("can't seek to the position");
//                }
//            }
            printStreamInfo(streams[s]);
            this.coder[s] = this.streams[s] != null ? this.streams[s].getStreamCoder() : null;
            this.streamTypes[s] = this.coder[s] != null ? this.coder[s].getCodecType() : null;
        }
    }

    /**
     * 由于这里XugglerInputStream主要实现的是高层封装，就是直接读取packet
     * 底层读取字节流的任务是由XugglerFSDataInputStream实现的。
     * 因此这里需要实现的三个方法就不用实现了。
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void resetState() throws IOException {
        // TODO Auto-generated method stub
    }

    @Override
    public int read() throws IOException {
        return 0;
    }

    public void close() throws IOException{
        this.container.close();
        super.close();
    }
    /**
     * 读取Packet的方法。
     */
    public XugglerPacket readPacket() {
        IPacket packet = IPacket.make();
        XugglerPacket xpacket = null;

        if (this.container.readNextPacket(packet) >= 0) {
            printPacketInfo(packet);
            xpacket = new XugglerPacket(packet,
                    streamTypes[packet.getStreamIndex()]);
        } else
            finished = true;

        if (xpacket != null) {
            // 计算出这个Packet相对于整个视频文件的位置。
            long pos = xpacket.getPosition() - decompressor.getHeaderSize()
                    + getAdjustedStart();
            xpacket.setPosition(pos);
            xpacket.setFrameNo(xpacket.getFrameNo() + getFrameOffset());
            LOG.debug("readPacket, frame no " + xpacket.getFrameNo());
        }
        return xpacket;
    }

    /**
     * 这个方法提供了一个直接得到BuffedImage形式的视频帧的方法。
     * 而不需要再根据Packet得到IVideoPicture。
     */
    public BufferedImage readVideoFrame() {
        XugglerPacket xpacket = readPacket();
        //LOG.debug(xpacket.getStreamType());
        // 首先找到视频Packet
        while (xpacket != null && xpacket.getStreamType() != XugglerPacket.StreamType.VIDEO)
            xpacket = readPacket();

        // 如果流已经结束了或者到头了，那就无法读取视频帧了。
        if (xpacket == null && finished()) return null;

        // 如果一直没有类型为Video的packet，只能返回null。
        if (xpacket.getStreamType() != XugglerPacket.StreamType.VIDEO) return null;

        // 如果还没解码，先解码。
        if (!xpacket.isDecoded())
            decode(xpacket);

        return xpacket.getBufferedImage();
    }

    public long getFrameOffset() {
        return frame_offset;
    }

    public void decode(XugglerPacket packet) {
        IPacket p = (IPacket) packet.getPacket();
        packet.setDecodedObject(decodePacket(p));
    }

    public Object decodePacket(IPacket packet) {

        LOG.debug("decodePacket");

        if (streamTypes[packet.getStreamIndex()] == ICodec.Type.CODEC_TYPE_VIDEO)
            return decodeVideoPacket(packet);
        if (streamTypes[packet.getStreamIndex()] == ICodec.Type.CODEC_TYPE_AUDIO)
            return decodeAudioPacket(packet);

        return null;

    }

    private Object decodeAudioPacket(IPacket packet) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * 解码视频帧
     * 基本就是按照xuggler官方的操作，读取IPacket对象，然后根据IStreamCoder转换成IVideoPicture对象。
     */
    private IVideoPicture decodeVideoPacket(IPacket packet) {
        LOG.debug("start decoding ...");
        int c;
        for (c = 0; c < coder.length; c++)
            if (coder[c] != null
                    && coder[c].getCodecType() == ICodec.Type.CODEC_TYPE_VIDEO)
                break;

        if (c >= coder.length)
            return null;

        IStreamCoder videoCoder = coder[c];

        /*
         * 按照VideoCoder给定的宽和高生成IVideoPicture对象。
         */
        IVideoPicture picture = IVideoPicture.make(videoCoder.getPixelType(),
                videoCoder.getWidth(), videoCoder.getHeight());

        if (!videoCoder.isOpen()) {
            int o = videoCoder.open();
            if (o < 0)
                LOG.debug("error on open VideoCoder = " + o);
        }

        int offset = 0;
        while (offset < packet.getSize()) {
            int bytesDecoded = videoCoder.decodeVideo(picture, packet, offset);
            LOG.debug("decoded size : " + bytesDecoded);
            if (bytesDecoded < 0)
                throw new RuntimeException("got error decoding video in: "
                        + packet.getPosition());
            offset += bytesDecoded;
            if (picture.isComplete()) {
                LOG.debug("picture is complated");
                IVideoPicture newPic = picture;
                setResampler(videoCoder);
                if (resampler != null) {
                    newPic = IVideoPicture.make(
                            resampler.getOutputPixelFormat(),
                            picture.getWidth(), picture.getHeight());
                    if (resampler.resample(newPic, picture) < 0)
                        throw new RuntimeException(
                                "could not resample video from: "
                                        + packet.getPosition());
                }
                videoCoder.close();
                return newPic;
            }
        }
        videoCoder.close();
        return null;
    }

    /**
     * 由于图片文件都是采用RGB分量来表示的，但是视频帧很多不是用RGB来表示
     * 因此需要用xuggler的重采样器来将不是BGR24类型（比如目前广泛应用的YUV系列）的视频帧重新采样
     */
    private void setResampler(IStreamCoder videoCoder) {
        if (videoCoder.getCodecType() != ICodec.Type.CODEC_TYPE_VIDEO)
            return;

        resampler = null;
        if (videoCoder.getPixelType() != IPixelFormat.Type.BGR24) {
            resampler = IVideoResampler.make(videoCoder.getWidth(),
                    videoCoder.getHeight(), IPixelFormat.Type.BGR24,
                    videoCoder.getWidth(), videoCoder.getHeight(),
                    videoCoder.getPixelType());
            if (resampler == null)
                throw new RuntimeException("could not create color space "
                        + "resampler for: " + videoCoder);
        }
    }

    public boolean finished() {
        return finished;
    }

    /**
     * 打印流信息。
     *
     * @param stream
     */
    private void printStreamInfo(IStream stream) {
        IStreamCoder coder = stream.getStreamCoder();
        IContainer container = stream.getContainer();
        String info = "";

        info += (String.format("type: %s; ", coder.getCodecType()));
        info += (String.format("codec: %s; ", coder.getCodecID()));
        info += String.format(
                "duration: %s; ",
                stream.getDuration() == Global.NO_PTS ? "unknown" : ""
                        + stream.getDuration());
        info += String.format("start time: %s; ",
                container.getStartTime() == Global.NO_PTS ? "unknown" : ""
                        + stream.getStartTime());
        info += String
                .format("language: %s; ",
                        stream.getLanguage() == null ? "unknown" : stream
                                .getLanguage());
        info += String.format("timebase: %d/%d; ", stream.getTimeBase()
                .getNumerator(), stream.getTimeBase().getDenominator());
        info += String.format("coder tb: %d/%d; ", coder.getTimeBase()
                .getNumerator(), coder.getTimeBase().getDenominator());

        if (coder.getCodecType() == ICodec.Type.CODEC_TYPE_AUDIO) {
            info += String.format("sample rate: %d; ", coder.getSampleRate());
            info += String.format("channels: %d; ", coder.getChannels());
            info += String.format("format: %s", coder.getSampleFormat());
        } else if (coder.getCodecType() == ICodec.Type.CODEC_TYPE_VIDEO) {
            info += String.format("width: %d; ", coder.getWidth());
            info += String.format("height: %d; ", coder.getHeight());
            info += String.format("format: %s; ", coder.getPixelType());
            info += String.format("frame-rate: %5.2f; ", coder.getFrameRate()
                    .getDouble());
        }
        LOG.debug(info);
    }

    private void printPacketInfo(IPacket packet) {
        String packet_info = "";
        packet_info += "Packet: ";
        packet_info += String.format("position = %d;", packet.getPosition());
        packet_info += String.format("isKey = %s;", packet.isKey());
        packet_info += String.format("stream_index = %d;",
                packet.getStreamIndex());
        packet_info += String.format("size = %d;", packet.getSize());
        LOG.debug(packet_info);
    }

    @Override
    public long getPos() {
        return xfsdis.getPos() - decompressor.getHeaderSize() + getAdjustedStart();
    }
}
