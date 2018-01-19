package com.VMDServiceServer.VMDTaskManageService;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.*;
//import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.Proto.SecondaryMetaClass.SecondaryMeta;
import com.UtilClass.Service.Command;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Handler主要用于对网络事件进行读写操作,是真正的业务类 通常只需要关注 channelRead 和 exceptionCaught 方法
 */
public class TaskServerHandler extends ChannelInboundHandlerAdapter {

	/**
	 * 日志
	 */
	public static final Log LOG  = LogFactory.getLog(TaskServer.class);
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		/*Logger root = Logger.getRootLogger();
	    root.addAppender(new ConsoleAppender(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
		root.setLevel(Level.INFO);*/
		// ByteBuf,类似于NIO中的ByteBuffer,但是更强大
		ByteBuf reqBuf = (ByteBuf) msg; // msg： 命令+url
		Command command = new Command(reqBuf);
		// 获取请求字符串
		// String req = getReq(reqBuf);

		LOG.debug("From:" + ctx.channel().remoteAddress());
		//System.out.println("From:" + ctx.channel().remoteAddress());

		LOG.debug("服务端收到:" + command.Type);
		//System.out.println("服务端收到:" + command.Type);
		String resStr;
		ByteBuf resBuf = null;
		String url;
		switch (command.Type) {
		case Command.VMDDELETE:
			if (command.args.length != 1) {
				LOG.info("args error!");
			}
			// 提交任务
			resStr = TaskServer.submit(command,ctx.channel().remoteAddress());;
			resBuf = getRes(resStr);
			LOG.debug("服务端应答数据:\n" + resStr);
			//System.out.println("服务端应答数据:\n" + resStr);
			ctx.write(resBuf);
			break;
		case Command.VMDGENERATE:
			if (command.args.length != 1) {
				LOG.info("args error!");
			}
			// 提交任务
			resStr = TaskServer.submit(command,ctx.channel().remoteAddress());
			resBuf = getRes(resStr);
			LOG.debug("服务端应答数据:\n" + resStr);
			//System.out.println("服务端应答数据:\n" + resStr);
			ctx.write(resBuf);
			break;
		case Command.GET_Task_Info:
			// 提交任务
			resStr = TaskServer.getTaskInfo(ctx.channel().remoteAddress());;
			resBuf = getRes(resStr);
			LOG.debug("服务端应答数据:\n" + resStr);
			//System.out.println("服务端应答数据:\n" + resStr);
			ctx.write(resBuf);
			break;
		default:
			// 丢弃
			LOG.debug("丢弃");
			//System.out.println("丢弃");
			ReferenceCountUtil.release(msg);
			break;
		}
	}

	/**
	 * 获取发送给客户端的数据
	 *
	 * @param resStr
	 * @return
	 */
	private ByteBuf getRes(String resStr) throws UnsupportedEncodingException {
		byte[] req = resStr.getBytes("UTF-8");
		ByteBuf pingMessage = Unpooled.buffer(req.length);
		// 将字节数组信息写入到ByteBuf
		pingMessage.writeBytes(req);

		return pingMessage;
	}

	/**
	 * 获取请求字符串
	 *
	 * @param buf
	 * @return
	 */
	private String getReq(ByteBuf buf) {
		byte[] con = new byte[buf.readableBytes()];
		// 将ByteByf信息写出到字节数组
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
		// 将消息发送队列中的消息写入到SocketChannel中发送给对方
		LOG.debug("channelReadComplete");
		ctx.flush();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		// 发生异常时,关闭 ChannelHandlerContext,释放ChannelHandlerContext 相关的句柄等资源
		LOG.error("exceptionCaught");
		ctx.close();
	}
}
