package com.Test;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.Set;

import javax.imageio.ImageIO;

import org.mortbay.log.Log;

import com.VMDServiceClientAPI.ClientAPI;
import com.xuggle.xuggler.Utils;

public class CTest {
	private static String url="hdfs://vm1:9000/yty/video/Test4.rmvb";
	public static void main(String[] args){
		
		switch(args[0]){
		case "VideoUPLOAD"://上传视频
			url=args[1];
			ClientAPI.VideoUPLOAD(url,args[2]).print();
			break;
		case "VMDDELETE":
			url=args[1];
			ClientAPI.VMDDELETE(url).print();
			break;
		case "VMDGENERATE":
			url=args[1];
			ClientAPI.VMDGENERATE(url).print();
		    break;
		case "GET_Task_Info"://列出目标VMD的所有信息
			ClientAPI.GET_Task_Info().print();
			break;
		case "GET_FRAME_By_FrameNo":
			url=args[1];
			String size1 = "";
			if(args.length==4){
				size1 = args[3];
			}
			else if(args.length==3){
				size1 = "1";
			}
			else
				break;
			Set<BufferedImage> imageSet1=(Set<BufferedImage>)ClientAPI.GET_FRAME_By_FrameNo(url,args[2],size1).getResults();
			int number1=1;
			for(BufferedImage bi:imageSet1){
            	try {
            		
					ImageIO.write(bi,"jpg",new File("F:\\picture\\"+number1+ ".jpg"));
					Log.info("生成图片："+"F:\\picture\\"+(number1)+ ".jpg");
					System.out.println("生成图片："+"F:\\picture\\"+(number1)+ ".jpg");
					number1++;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			break;
		case "GET_FRAME_By_FrameTime":
			url=args[1];
			String size2 = "";
			if(args.length==4){
				size2 = args[3];
			}
			else if(args.length==3){
				size2 = "1";
			}
			else
				break;
			Set<BufferedImage> imageSet2=(Set<BufferedImage>)ClientAPI.GET_FRAME_By_FrameTime(url,args[2],size2).getResults();
			int number2=1;
			for(BufferedImage bi:imageSet2){
            	try {
            		
					ImageIO.write(bi,"jpg",new File("F:\\picture\\"+number2+ ".jpg"));
					Log.info("生成图片："+"F:\\picture\\"+(number2)+ ".jpg");
					System.out.println("生成图片："+"F:\\picture\\"+(number2)+ ".jpg");
					number2++;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			break;
		case "GET_FRAME_Index_By_FrameNo":
			url=args[1];
			ClientAPI.GET_FRAME_Index_By_FrameNo(url,args[2]).printFRAMEIndex();
			break;
		case "GET_FRAME_Index_By_FrameTime":
			url=args[1];
			ClientAPI.GET_FRAME_Index_By_FrameTime(url,args[2]).printFRAMEIndex();
			break;
		case "VMDLS"://列出所有节点的所有VMD，及其I帧字典规模
			if(args.length==2)
				url=args[1];
			else if(args.length==1)
				url="hdfs://vm1:9000/yty/video/";
			else
				break;
			ClientAPI.VMDLS(url).printVMDMap();
			break;
		case "VMDGET"://列出目标VMD的所有信息
			url=args[1];
			ClientAPI.VMDGET(url).printVMDInfo();
			break;
		case "GET_ALL_RedisInfo"://列出目标VMD的所有信息
			ClientAPI.GET_ALL_RedisInfo().printNodeMap();
			break;
		case "GET_IndexServerInfo"://列出目标VMD的所有信息
			ClientAPI.GET_IndexServerInfo().printNodeMap();
			break;
		case "GET_TaskServerInfo"://列出目标VMD的所有信息
			ClientAPI.GET_TaskServerInfo().printNodeMap();
			break;
		case "GET_ALL_NodeInfo"://列出目标VMD的所有信息
			ClientAPI.GET_ALL_NodeInfo().printNodeMap();
			break;
		    default:
		    	break;
		}
    }
}
