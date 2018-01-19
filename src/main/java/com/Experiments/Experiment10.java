package com.Experiments;


import com.Proto.SecondaryMetaClass;
import com.UtilClass.Service.ConfUtil;
import com.UtilClass.VMD.HDFSProtocolHandlerFactory;
import com.xuggle.xuggler.*;
import com.xuggle.xuggler.io.URLProtocolManager;

import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * 这个方法用原始的解码帧方法每隔1000帧读取1帧。
 *
 */
public class Experiment10 {
    public static final Log LOG = LogFactory.getLog(Experiment10.class);

    public static void main(String[] args) throws IOException {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.ERROR);

        String[] Paths={
        		//5组样本，每组一个视频
            	//首先获取某一个视频的视频元数据对象
        		
        		"hdfs://vm1:9000/yty/video/780.mp4",
        		"hdfs://vm1:9000/yty/video/781.mp4",
        		"hdfs://vm1:9000/yty/video/782.mp4",
        		"hdfs://vm1:9000/yty/video/783.mp4",
        		"hdfs://vm1:9000/yty/video/784.mp4"
        };
      //创建一个Container来打开视频，如果path是hdfs的路径，则使用HDFSProtocolHandler
        URLProtocolManager mgr = URLProtocolManager.getManager();
        mgr.registerFactory("hdfs", new HDFSProtocolHandlerFactory(ConfUtil.generate()));
        IContainer container = IContainer.make();
        int sizePerTime=1;
        Long t=0L;
        for(int j=0;j<5;j++){
            String Path = Paths[j];
            FileWriter fw;
            //fw = new FileWriter("F:\\result\\"+"Experiment10-"+new Path(Path).getName() + ".txt",true);
            fw = new FileWriter("/home/b8311/Experiment/ExperimentResult/"+"Experiment10-"+new Path(Path).getName() + ".txt",true);
            for(int k=0;k<10;k++){
            	int targetFrame = k*1000;
            	
                StopWatch sw = new StopWatch();
                StopWatch sw0=new StopWatch();
                StopWatch sw1=new StopWatch();
                sw.start();

                
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
                //定位至目标TimeStamp，这里会自动调用之前写的HDFSProtocolHandler
                //*******************************重要方法************************
                //  参数分别是（流编号，时间偏移量，seek标志{0就是从起始位置开始seek}）
                //container.seekKeyFrame(VideoStreamIndex,before,0);
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
                sw0.start();
                while(container.readNextPacket(packet) >= 0){
                    if(packet.getStreamIndex() == VideoStreamIndex){
                        IVideoPicture picture = IVideoPicture.make(coder.getPixelType(),coder.getWidth(),coder.getHeight());
                        int offset = 0;
                        while(offset < packet.getSize()){
                            int decoded = coder.decodeVideo(picture,packet,offset);
                            if(decoded < 0) throw new RuntimeException("ERROR");
                            offset+=decoded;
                            if(picture.isComplete()){     
                                if(number>=targetFrame){
                                	sw0.stop();                                	
                                	sw1.start();
                                	IVideoPicture newpic = picture;
                                	if(resampler !=null){
                                        newpic = IVideoPicture.make(
                                                resampler.getOutputPixelFormat(),picture.getWidth(),picture.getHeight()
                                        );
                                        if(resampler.resample(newpic,picture)<0)
                                            throw new RuntimeException("can not resample the picture");
                                    }
                                	BufferedImage bi = Utils.videoPictureToImage(newpic);
                                	sw1.stop();
                                	//ImageIO.write(bi,"jpg",new File("F:\\picture\\"+number + ".jpg"));                        
                                	ImageIO.write(bi,"jpg",new File("/home/b8311/Experiment/ExperimentResult/picture/"+new Path(Path).getName()+"-"+number + ".jpg"));
                                	
                                	t=sw0.getTime();
                                    System.out.println(k+" "+"Seek Time:"+" "+t);
                                    fw.write(k+" "+"Seek Time:"+" "+t);
                                    t=sw1.getTime();
                                    System.out.println(k+" "+"GeneratePicture Time:"+" "+t);
                                    fw.write(" "+"GeneratePicture Time:"+" "+t);
                                    fw.flush();
                                }
                                number++;	
                                
                            }
                        }
                        //这里每次解码sizePerTime张图片。
                        if(number == targetFrame+sizePerTime) break;
                    }
                }

                //千万别忘记将这两个对象close了
                coder.close();
                container.close();
                sw.stop();
                t=sw.getTime();
                
                
                System.out.println(k+" "+"Total Time:"+" "+t);
                fw.write(" "+"Total Time:"+" "+t+"\r\n");
                fw.flush();                
            }
            fw.close();
        }
        
        
        
    }
}

