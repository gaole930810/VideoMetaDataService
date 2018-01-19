package com.Experiments;


import com.Proto.SecondaryMetaClass;
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

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * 这个方法使用HDFS中的vmd读取第n帧。
 *
 */
public class VMD_HardDisk_ReadSingleFrame {
    public static final Log LOG = LogFactory.getLog(VMD_HardDisk_ReadSingleFrame.class);

    public static void main(String[] args) throws IOException {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.INFO);

        //首先获取某一个视频的视频元数据对象
        String Path = "hdfs://vm1:9000/yty/video/780.mp4";
        
        int targetFrame = 1250;
        
        


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
        int VideoStreamIndex = -1;
        for (int i = 0; i < container.getNumStreams(); i++) {
            if (container.getStream(i).getStreamCoder().getCodecType() == ICodec.Type.CODEC_TYPE_VIDEO) {
                VideoStreamIndex = i;
                coder = container.getStream(i).getStreamCoder();
                break;
            }
        }
        if (VideoStreamIndex == -1) throw new IllegalArgumentException("this container doesn't hava a video stream.");
        LOG.info("the video stream id is " + VideoStreamIndex);

        //获取距离target最近的关键帧的时间偏移量，记录为before
        IStream videoStream = container.getStream(VideoStreamIndex);
        Long before = 0L;
        Long framebefore = 0L;
        FileSystem hdfs = FileSystem.get(ConfUtil.generate());
        SecondaryMetaClass.SecondaryMeta SM = SecondaryMetaClass.SecondaryMeta.parseFrom(hdfs.open(new Path(Path + ".vmd")));
        List<SecondaryMetaClass.SecondaryMeta.FrameInfoGroup> fig = SM.getFrameMetaInfoList();
		
        LOG.debug(fig.size());
        if(targetFrame<fig.get(0).getStartFrameNo()){
        	before=0L;
        	framebefore=0L;
		}
        int start = findStart(targetFrame,0,fig.size()-1,fig); 
        before = fig.get(start).getStartIndex();
        framebefore = fig.get(start).getStartFrameNo();
        
        System.out.println(before);
        System.out.println(framebefore);

        //定位至目标TimeStamp，这里会自动调用之前写的HDFSProtocolHandler
        //*******************************重要方法************************
        //  参数分别是（流编号，时间偏移量，seek标志{0就是从起始位置开始seek}）
        container.seekKeyFrame(VideoStreamIndex,before,0);
        //*************************************************************
        //decode，下面就是标准的使用xuggler进行视频解码的操作了。
        Long number = framebefore;
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
                        if(number>=targetFrame){
                        	BufferedImage bi = Utils.videoPictureToImage(newpic);
                        	ImageIO.write(bi,"jpg",new File("F:\\picture\\"+number + ".jpg"));
                        	
                        }
                        
                        number++;
                        //ImageIO.write(bi,"jpg",new File("/home/b8311/yty/picture/"+number + ".jpg"));
                    }
                }
                System.out.println(number);
                //这里我只想测试15张图片。
                if(number == targetFrame+15) break;
            }
        }

        //千万别忘记将这两个对象close了
        coder.close();
        container.close();
    }
    public static int findStart(int FrameNo,int s,int e,List<SecondaryMetaClass.SecondaryMeta.FrameInfoGroup> fig){
		int start=0;
		SecondaryMetaClass.SecondaryMeta.FrameInfoGroup temp=null;
		Iterator<SecondaryMetaClass.SecondaryMeta.FrameInfoGroup> iter=fig.iterator();
		while(iter.hasNext()){
			temp=iter.next();
			if(temp.getStartFrameNo()>FrameNo){
				start--;
				break;
			}
			start++;
		}
		return start;
	}
}

