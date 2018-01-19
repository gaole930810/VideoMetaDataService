package com.VMDServiceClientAPI;

import com.UtilClass.Service.Command;
import com.UtilClass.Service.ConfUtil;
import com.UtilClass.Service.Results;
import com.UtilClass.VMD.HDFSProtocolHandlerFactory;
import com.xuggle.xuggler.ICodec;
import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IStreamCoder;
import com.xuggle.xuggler.io.URLProtocolManager;

public class ClientAPI {
	public static Results VideoUPLOAD(String src,String des){
		Command command=new Command(Command.VideoUPLOAD,src,des);		
		return new Client("172.16.10.101:2181","/sgroup").connect(command);
	}
	public static Results VMDGENERATE(String url){
		Command command=new Command(Command.VMDGENERATE,url);		
		return new Client("172.16.10.101:2181","/sgroup").connect(command);
	}
	public static Results VMDDELETE(String url){
		Command command=new Command(Command.VMDDELETE,url);		
		return new Client("172.16.10.101:2181","/sgroup").connect(command);
	}
	public static Results VMDGET(String url){
		Command command=new Command(Command.VMDGET,url);		
		return new Client("172.16.10.101:2181","/sgroup").connect(command);
	}	
	public static Results VMDLS(String url){
		Command command=new Command(Command.VMDLS,url);		
		return new Client("172.16.10.101:2181","/sgroup").connect(command);
	}
	public static Results GET_FRAME_Index_By_FrameNo(String url,String startFrameNo){
		Command command=new Command(Command.GET_FRAME_Index,url,startFrameNo);		
		return new Client("172.16.10.101:2181","/sgroup").connect(command);
	}
	public static Results GET_FRAME_By_FrameNo(String url,String startFrameNo,String Size){
		Command command=new Command(Command.GET_FRAME,url,startFrameNo,Size);		
		return new Client("172.16.10.101:2181","/sgroup").connect(command);
	}
	public static Results GET_FRAME_Index_By_FrameTime(String url,String startFrameTime){
		Command command=new Command(Command.GET_FRAME_Index,fraseFrameTimetoFrameNo(url,startFrameTime));
		return new Client("172.16.10.101:2181","/sgroup").connect(command);
	}
	public static Results GET_FRAME_By_FrameTime(String url,String startFrameTime,String endFrameTime){
		Command command=new Command(Command.GET_FRAME,fraseFrameTimetoFrameNo(url,startFrameTime,endFrameTime));
		return new Client("172.16.10.101:2181","/sgroup").connect(command);
	}
	
	public static Results GET_Task_Info(){
		Command command=new Command(Command.GET_Task_Info);		
		return new Client("172.16.10.101:2181","/sgroup").connect(command);
	}
	public static Results GET_ALL_RedisInfo(){
		Command command=new Command(Command.GET_ALL_RedisInfo);		
		return new Client("172.16.10.101:2181","/sgroup").connect(command);
	}
	public static Results GET_IndexServerInfo(){
		Command command=new Command(Command.GET_IndexServerInfo);		
		return new Client("172.16.10.101:2181","/sgroup").connect(command);
	}
	public static Results GET_TaskServerInfo(){
		Command command=new Command(Command.GET_TaskServerInfo);		
		return new Client("172.16.10.101:2181","/sgroup").connect(command);
	}
	
	public static Results GET_ALL_NodeInfo(){
		Command command=new Command(Command.GET_ALL_NodeInfo);		
		return new Client("172.16.10.101:2181","/sgroup").connect(command);
	}
	
	private static String[] fraseFrameTimetoFrameNo(String Path, String... targetFrameTimes) {
		// TODO Auto-generated method stub
		URLProtocolManager mgr = URLProtocolManager.getManager();
        if (Path.startsWith("hdfs://"))
            mgr.registerFactory("hdfs", new HDFSProtocolHandlerFactory(ConfUtil.generate()));
        IContainer container = IContainer.make();
        int r = container.open(Path, IContainer.Type.READ, null);
        if (r < 0) throw new IllegalArgumentException("Can not open the video container of path {" + Path + "}");
        
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
        if(targetFrameTimes.length==2){
        	long startFrameNo=(long) Math.floor(Double.valueOf(targetFrameTimes[0])*fps);
        	long endFrameNo=(long) Math.floor(Double.valueOf(targetFrameTimes[1])*fps);
            long size=endFrameNo-startFrameNo+1;
          //potplayer播放器会在视频开头自动添加一帧作为第0帧，所以在该播放器中视频的总帧数会比视频实际帧数多1帧，
            //因此播放器中的第n帧对应的是视频中的第n-1帧，因此如果此处要与播放器的视频帧相匹配则targetFrame需要-1；
            startFrameNo--;
            return new String[]{Path,String.valueOf(startFrameNo),String.valueOf(size)};
        }else{
        	long startFrameNo=(long) Math.floor(Double.valueOf(targetFrameTimes[0])*fps);
        	startFrameNo--;
        	return new String[]{Path,String.valueOf(startFrameNo)};
        }	
	}
}

