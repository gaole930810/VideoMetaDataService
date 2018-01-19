package com.Test;


import com.Proto.SecondaryMetaClass;
import com.UtilClass.Service.ConfUtil;
import com.UtilClass.VMD.HDFSProtocolHandlerFactory;
import com.UtilClass.VMD.VideoMetaData;
import com.xuggle.xuggler.*;
import com.xuggle.xuggler.io.URLProtocolManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * 这个测试用来检测能否使用VMD读取某一帧视频内容。
 */
public class GetFrameWithVideoTimeTest {
    public static final Log LOG = LogFactory.getLog(GetFrameWithVideoTimeTest.class);

    public static void main(String[] args) throws IOException {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.INFO);

        //首先获取某一个视频的视频元数据对象
        //String Path = "hdfs://vm1:9000/yty/video/785.mp4";
        String Path = "hdfs://vm1:9000/yty/video/785.mp4";
        double timeoftargetFrame = 17.807;
        
      //创建一个Container来打开视频，如果path是hdfs的路径，则使用HDFSProtocolHandler
        URLProtocolManager mgr = URLProtocolManager.getManager();
        if (Path.startsWith("hdfs://"))
            mgr.registerFactory("hdfs", new HDFSProtocolHandlerFactory(ConfUtil.generate()));
        IContainer container = IContainer.make();
        int r = container.open(Path, IContainer.Type.READ, null);
        if (r < 0) throw new IllegalArgumentException("Can not open the video container of path {" + Path + "}");
        LOG.info("Xuggle video container opens");
        

        //获取视频流编号
        IStreamCoder coder = null;
        double fps = 0;
        long framenum=0;
        int VideoStreamIndex = -1;
        for (int i = 0; i < container.getNumStreams(); i++) {
            if (container.getStream(i).getStreamCoder().getCodecType() == ICodec.Type.CODEC_TYPE_VIDEO) {
                VideoStreamIndex = i;
                coder = container.getStream(i).getStreamCoder();
                fps=container.getStream(i).getFrameRate().getDouble();
                framenum=container.getStream(i).getNumFrames();
                break;
            }
        }        
        int targetFrame = (int) Math.floor(timeoftargetFrame*fps);
        System.out.println(framenum+"   "+fps+"  "+targetFrame);
        //potplayer播放器会在视频开头自动添加一帧作为第0帧，所以在该播放器中视频的总帧数会比视频实际帧数多1帧，
        //因此播放器中的第n帧对应的是视频中的第n-1帧，因此如果此处要与播放器的视频帧相匹配则targetFrame需要-1；
        targetFrame--;        
        try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        SecondaryMetaClass.SecondaryMeta vmd = VideoMetaData.generateViaKeyPacket(Path, ConfUtil.generate());
        VideoMetaData.print(vmd);
        LOG.info(vmd.getFrameMetaInfoList().size());
        List<SecondaryMetaClass.SecondaryMeta.FrameInfoGroup> IDict = vmd.getFrameMetaInfoList();
        


        
        
        if (VideoStreamIndex == -1) throw new IllegalArgumentException("this container doesn't hava a video stream.");
        LOG.info("the video stream id is " + VideoStreamIndex);

        //获取距离target最近的关键帧的时间偏移量，记录为before
        IStream videoStream = container.getStream(VideoStreamIndex);
        Long before = 0L;
        Long framebefore = 0L;
        for(SecondaryMetaClass.SecondaryMeta.FrameInfoGroup item : IDict){
            if(item.getStartFrameNo() > targetFrame){
                break;
            }
            before = item.getStartIndex();
            framebefore = item.getStartFrameNo();
            
        }
        System.out.println(before);
        System.out.println(framebefore);

        //定位至目标TimeStamp，这里会自动调用之前写的HDFSProtocolHandler
        //*******************************重要方法************************
        //  参数分别是（流编号，时间偏移量，seek标志{0就是从起始位置开始seek}）
        container.seekKeyFrame(VideoStreamIndex,before,0);
        //*************************************************************
        //decode，下面就是标准的使用xuggler进行视频解码的操作了。
        int number = 0;
        IVideoResampler resampler = null;
        if(coder.getPixelType() != IPixelFormat.Type.BGR24){
            resampler = IVideoResampler.make(
                    coder.getWidth(),coder.getHeight(), IPixelFormat.Type.BGR24,
                    coder.getWidth(),coder.getHeight(),coder.getPixelType());
            if(resampler == null) throw new RuntimeException("Can not resample the video stream");
        }

        IPacket packet = IPacket.make();

        if(coder.open() < 0) throw new RuntimeException("can not open the video coder");
        
    	int firstwhile=0;
    	int secondwhile=0;
    	System.out.println(firstwhile++);
    	int count=1;

        while(container.readNextPacket(packet) >= 0){
        	System.out.println(firstwhile++);
            if(packet.getStreamIndex() == VideoStreamIndex){
                IVideoPicture picture = IVideoPicture.make(coder.getPixelType(),coder.getWidth(),coder.getHeight());
                int offset = 0;
                while(offset < packet.getSize()){
                	System.out.println(secondwhile++);
                    int decoded = coder.decodeVideo(picture,packet,offset);
                    if(decoded < 0) throw new RuntimeException("ERROR");
                    offset+=decoded;
                    if(picture.isComplete()){
                        IVideoPicture newpic = picture;
                        if(resampler !=null){
                            newpic = IVideoPicture.make(
                                    resampler.getOutputPixelFormat(),picture.getWidth(),picture.getHeight()
                            );
                            if(resampler.resample(newpic,picture)<0)
                                throw new RuntimeException("can not resample the picture");
                        }
                        if(count>targetFrame-framebefore){
                        	BufferedImage bi = Utils.videoPictureToImage(newpic);
                        	ImageIO.write(bi,"jpg",new File("F:\\picture\\"+number + ".jpg"));
                        	number++;
                        }
                        count++;
                        //ImageIO.write(bi,"jpg",new File("/home/b8311/yty/picture/"+number + ".jpg"));
                    }
                }
                System.out.println(number);
                //这里我只想测试15张图片。
                if(number == 2) break;
            }
        }

        //千万别忘记将这两个对象close了
        coder.close();
        container.close();
    }
}

