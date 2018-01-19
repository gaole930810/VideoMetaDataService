package com.VMDServiceClientAPI;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.*;

import com.UtilClass.Service.Command;
import com.UtilClass.Service.Results;

import java.io.UnsupportedEncodingException;

public class ClientManageHandler extends ChannelInboundHandlerAdapter {
    /**
     * 日志
     */
	public static final Log LOG = LogFactory.getLog(ClientManageHandler.class);
	public Results results;
	public Command command ;
    public ClientManageHandler(Results results,Command command){
    	this.results=results;
    	this.command=command;
    }
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
    	/*Logger root = Logger.getRootLogger();
    	root.addAppender(new ConsoleAppender(
                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
    	root.setLevel(Level.INFO);*/
    	LOG.debug("客户端连接上了服务端master");
        //System.out.println("客户端连接上了服务端master");        

        //发送请求
        ByteBuf reqBuf = getReq(command);

        ctx.writeAndFlush(reqBuf);
        reqBuf.clear();
    }

    /**
     * 将字符串包装成ByteBuf
     * @param s
     * @return
     */
    private ByteBuf getReq(Command command) throws UnsupportedEncodingException {
    	String s=String.valueOf(command.Type);
    	if(command.args!=null){
    		for(int i=0;i<command.args.length;i++){
    			s+="+"+command.args[i];
    		}
    	}    	
        byte[] data = s.getBytes("UTF-8");
        ByteBuf reqBuf = Unpooled.buffer(data.length);
        reqBuf.writeBytes(data);
        return reqBuf;
    }
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf byteBuf = (ByteBuf) msg;
        String resStr = getRes(byteBuf);
        byteBuf.clear();
        results.results=resStr;
        if(command.Type==Command.GET_FRAME_Index){
        	LOG.debug("客户端收到:"+resStr);
            //System.out.println("客户端收到:"+resStr); 
        }else if(command.Type==Command.GET_FRAME){
        	LOG.debug("客户端收到:"+resStr);
            //System.out.println("客户端收到:"+resStr);
        }else if(command.Type==Command.VMDGET){
        	LOG.debug("客户端收到:"+resStr);
            //System.out.println("客户端收到:"+resStr);
        }       
        
    }

    private String getRes(ByteBuf buf) {
        byte[] con = new byte[buf.readableBytes()];
        buf.readBytes(con);
        try {
            return new String(con, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
    }
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        //将消息发送队列中的消息写入到SocketChannel中发送给对方
    	LOG.debug("channelReadComplete");
        ctx.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.close();
    }
}