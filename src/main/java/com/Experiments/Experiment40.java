package com.Experiments;

import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.UtilClass.Service.Command;
import com.UtilClass.Service.ConfUtil;
import com.UtilClass.Service.Results;
import com.VMDServiceClientAPI.ClientInitializer;
import com.VMDServiceClientAPI.ClientManageInitializer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class Experiment40 {

	public static void main(String[] args) throws IOException {
		String src=args[0];
		String des="hdfs://vm1:9000/yty/video/";
		Command command=new Command(Command.VideoUPLOAD,src,des);
		Experiment40 exp=new Experiment40();
		exp.connect(command).print();
		
	}
	public static final Log LOG = LogFactory.getLog(Experiment40.class);
	public  Results results=new Results();
    private String HOST="127.0.0.1";
    private int PORT=8000;
    /*private String[] serversIP={
    	"172.16.10.101",
    	"172.16.10.102",
    	"172.16.10.103"
    };*/

    public Results connect(Command command) throws IOException{
    	if(command.getCommand()==Command.VideoUPLOAD){
    		FileWriter fw;
            //fw = new FileWriter("F:\\result\\"+"Experiment11-"+new Path(Path).getName() + ".txt",true);
            fw = new FileWriter("/home/b8311/Experiment/ExperimentResult/"+"Experiment40-"+new Path(command.getArgs()[0]).getName() + ".txt",true);
            
    		StopWatch sw = new StopWatch();
    		StopWatch sw0 = new StopWatch();
    		StopWatch sw1 = new StopWatch();
    		Long t=0L;
            sw.start();
            sw0.start();
    		String src=command.getArgs()[0];
    		String des=command.getArgs()[1];
    		
    		uploadViaUtils(new Path(src),new Path(des));
    		sw0.stop();
    		sw1.start();
    		String url=des+new Path(src).getName();
    		System.out.println("GENERATE");
    		command=new Command(Command.VMDGENERATE,url);
    		HOST=getServer(command);
    		if(HOST.equals("VMD EXISTS")){
    			results.results="VMD EXISTS";
    			return results;
    		}
    		System.out.println(HOST);    		
            results=doAfterFind(command);
            sw1.stop();
            sw.stop();
            t=sw.getTime();
            System.out.println("total time:"+" "+t);
            fw.write("total time:"+" "+t+"\r\n");
            fw.flush();
            t=sw0.getTime();
            System.out.println("upload video:"+" "+t);
            fw.write("upload video:"+" "+t+"\r\n");
            fw.flush();
            t=sw1.getTime();
            System.out.println("generate vmd:"+" "+t);
            fw.write("generate vmd:"+" "+t+"\r\n");
            fw.flush();
            fw.close();
    		return results;
    	}
    	if(command.getCommand()==Command.VMDLS){
    		
    		results=getLs(command);
    		
    		return results;
    	}
    	if(command.getCommand()==Command.VMDDELETE||
    			command.getCommand()==Command.VMDGET||
    			command.getCommand()==Command.GET_FRAME
    			){
    		HOST=getHost(command);
        	if(HOST.equals("VMD NOT EXISTS")){
        		results.results="VMD NOT EXISTS";
        		return results;
        	}
        	return doAfterFind(command);
    	}
    	if(command.getCommand()==Command.VMDGENERATE){
    		System.out.println("GENERATE");
    		HOST=getServer(command);
    		if(HOST.equals("VMD EXISTS")){
    			results.results="VMD EXISTS";
    			return results;
    		}
    		System.out.println(HOST);
    		return doAfterFind(command);
    	}
    	return results;
    	
    }
    public Results doAfterFind(Command command){
    	/*Logger root = Logger.getRootLogger();
    	root.addAppender(new ConsoleAppender(
                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
    	root.setLevel(Level.INFO);*/
        //配置客户端NIO线程组
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(1024*1024))
                    .option(ChannelOption.TCP_NODELAY,true)
                    .handler(new ClientInitializer(command,results));
            //发起异步连接操作
            LOG.debug("发起异步连接操作 - start");
            //System.out.println("发起异步连接操作 - start");
            
            ChannelFuture channelFuture = bootstrap.connect(HOST,PORT).sync();
            
            LOG.debug("发起异步连接操作 - end");
            //System.out.println("发起异步连接操作 - end");
            
            
            //等待客户端链路关闭
            LOG.debug("等待客户端链路关闭 - start");
            //System.out.println("等待客户端链路关闭 - start");
            
            channelFuture.channel().closeFuture().sync();
            
            LOG.debug("等待客户端链路关闭 - end");
            //System.out.println("等待客户端链路关闭 - end");
            
        } catch (InterruptedException e) {
        	LOG.error(e.getMessage(),e);
            System.out.println(e.getMessage());
        }finally {
            //关闭
            eventLoopGroup.shutdownGracefully();
        }
        return results;
    }
    public void uploadViaUtils(Path src, Path des) {
        try {
            FileSystem hdfs = FileSystem.get(ConfUtil.generate());
            LOG.debug("Start Uploading");
            hdfs.copyFromLocalFile(false, true, src, des);
            LOG.debug("Uploading Finished");
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
    public Results getLs(Command command) {
		// TODO Auto-generated method stub
    	Results Ls=new Results();
    	String ServerMasterIP="172.16.10.110";
    	int ServerMasterPORT=8000;
    	 //配置客户端NIO线程组
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY,true)
                    .handler(new ClientManageInitializer(command,Ls));
            //发起异步连接操作
            LOG.debug("发起异步连接操作 - start");
            //System.out.println("发起异步连接操作 - start");
            
            ChannelFuture channelFuture = bootstrap.connect(ServerMasterIP,ServerMasterPORT).sync();
            
            LOG.debug("发起异步连接操作 - end");
            //System.out.println("发起异步连接操作 - end");
            
            
            //等待客户端链路关闭
            LOG.debug("等待客户端链路关闭 - start");
            //System.out.println("等待客户端链路关闭 - start");
            
            channelFuture.channel().closeFuture().sync();
            
            LOG.debug("等待客户端链路关闭 - end");
            //System.out.println("等待客户端链路关闭 - end");
            
        } catch (InterruptedException e) {
        	LOG.error(e.getMessage(),e);
            System.out.println(e.getMessage());
        }finally {
            //关闭
            eventLoopGroup.shutdownGracefully();
        }
        LOG.info("getLs from  serverMaster:SUCCESS");
    	return Ls;
	}

    public String getHost(Command command){
    	Results HostIP=new Results();
    	String ServerMasterIP="172.16.10.110";
    	int ServerMasterPORT=8000;
    	 //配置客户端NIO线程组
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY,true)
                    .handler(new ClientManageInitializer(command,HostIP));
            //发起异步连接操作
            LOG.debug("发起异步连接操作 - start");
            //System.out.println("发起异步连接操作 - start");
            
            ChannelFuture channelFuture = bootstrap.connect(ServerMasterIP,ServerMasterPORT).sync();
            
            LOG.debug("发起异步连接操作 - end");
            //System.out.println("发起异步连接操作 - end");
            
            
            //等待客户端链路关闭
            LOG.debug("等待客户端链路关闭 - start");
            //System.out.println("等待客户端链路关闭 - start");
            
            channelFuture.channel().closeFuture().sync();
            
            LOG.debug("等待客户端链路关闭 - end");
            //System.out.println("等待客户端链路关闭 - end");
            
        } catch (InterruptedException e) {
        	LOG.error(e.getMessage(),e);
            System.out.println(e.getMessage());
        }finally {
            //关闭
            eventLoopGroup.shutdownGracefully();
        }
        LOG.info("getHost from  serverMaster:SUCCESS");
    	return (String) HostIP.results;
    }
	public String getServer(Command command){
    	Results ServerIP=new Results();
    	Command GetServer=new Command(Command.GET_TaskServerInfo,command.args[0]);
    	String ServerMasterIP="172.16.10.110";
    	int ServerMasterPORT=8000;
    	 //配置客户端NIO线程组
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY,true)
                    .handler(new ClientManageInitializer(GetServer,ServerIP));
            //发起异步连接操作
            LOG.debug("发起异步连接操作 - start");
            //System.out.println("发起异步连接操作 - start");
            
            ChannelFuture channelFuture = bootstrap.connect(ServerMasterIP,ServerMasterPORT).sync();
            
            LOG.debug("发起异步连接操作 - end");
            //System.out.println("发起异步连接操作 - end");
            
            
            //等待客户端链路关闭
            LOG.debug("等待客户端链路关闭 - start");
            //System.out.println("等待客户端链路关闭 - start");
            
            channelFuture.channel().closeFuture().sync();
            
            LOG.debug("等待客户端链路关闭 - end");
            //System.out.println("等待客户端链路关闭 - end");
            
        } catch (InterruptedException e) {
        	LOG.error(e.getMessage(),e);
            System.out.println(e.getMessage());
        }finally {
            //关闭
            eventLoopGroup.shutdownGracefully();
        }
        LOG.info("getServer from  serverMaster:SUCCESS");
    	return (String) ServerIP.results;
    }
}
