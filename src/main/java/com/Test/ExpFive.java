package com.Test;

import com.UtilClass.VMD.MyContainer;
import com.xuggle.xuggler.IPacket;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

/**
 * 验证MyContainer中的解码功能是否正确。
 * Created by yty on 2016/12/18.
 */
public class ExpFive {
    public static void main(String [] args )throws IOException{
        MyContainer mc = new MyContainer("D:\\test.mkv");
        mc.start();
        BufferedImage bi;
        int count = 0;
        IPacket packet;
        while((packet = mc.getVideoPacket())!=null){
            if((bi = mc.decodeFrame(packet))==null){
                System.out.println(count);
            }
            //ImageIO.write(bi,"jpg",new File(count +".jpg"));
            count ++;
        }
        mc.stop();
    }
}
