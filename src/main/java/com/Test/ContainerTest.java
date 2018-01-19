package com.Test;

import com.UtilClass.Service.ConfUtil;
import com.xuggle.xuggler.*;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by yty on 2016/12/13.
 */
public class ContainerTest {
    public static void main(String [] args) throws IOException{
        IContainer container = IContainer.make();
        FileSystem fs = FileSystem.get(ConfUtil.generate());
        FSDataInputStream fin = fs.open(new Path("hdfs://vm1:9000/yty/video/Test.mkv"));
        fin.seek(0L);
        byte[] header = new byte[5554];
        fin.read(header,0,5554);
        fin.seek(240708802);
        int r = container.open((InputStream) fin,null);
        System.out.println(r);
        int count = 0;
        for (int i = 0; i < container.getNumStreams(); i++) {
            IStream stream = container.getStream(i);
            IStreamCoder coder = stream.getStreamCoder();
            if (coder.getCodecType() != ICodec.Type.CODEC_TYPE_VIDEO)
                continue;
            IPacket packet = IPacket.make();
            coder.open();
            IVideoPicture picture = IVideoPicture.make(coder.getPixelType(),
                    coder.getWidth(), coder.getHeight());
            while (container.readNextPacket(packet) >= 0) {
                if (packet.getStreamIndex() == i) {
                    System.out.println(packet.getPosition());
                    int offset = 0;
                    while (offset < packet.getSize()) {
                        int bytesDecoded = coder.decodeVideo(picture, packet, offset);
                        if (bytesDecoded < 0)
                            throw new RuntimeException("got error decoding video in: " + packet.getPosition());
                        offset += bytesDecoded;
                        if (picture.isComplete()) {
                            Utils.videoPictureToImage(picture);
                            count ++;
                        }
                    }
                    if (count == 50) {
                        break;
                    }
                }
            }
            coder.close();
        }

    }
}
