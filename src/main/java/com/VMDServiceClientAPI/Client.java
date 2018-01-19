package com.VMDServiceClientAPI;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.Proto.SecondaryMetaClass;
import com.Proto.SecondaryMetaClass.SecondaryMeta;
import com.UtilClass.Service.Command;
import com.UtilClass.Service.ConfUtil;
import com.UtilClass.Service.RedisCluster;
import com.UtilClass.Service.Results;
import com.UtilClass.Service.SerializationUtil;
import com.UtilClass.VMD.HDFSProtocolHandlerFactory;
import com.UtilClass.VMD.VideoMetaData;
import com.xuggle.xuggler.ICodec;
import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IPacket;
import com.xuggle.xuggler.IPixelFormat;
import com.xuggle.xuggler.IStream;
import com.xuggle.xuggler.IStreamCoder;
import com.xuggle.xuggler.IVideoPicture;
import com.xuggle.xuggler.IVideoResampler;
import com.xuggle.xuggler.Utils;
import com.xuggle.xuggler.io.URLProtocolManager;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

public class Client {
    /**
     * 日志
     */
	public static final Log LOG = LogFactory.getLog(Client.class);
    

    public  Results results=new Results();
    private int PORT=8000;//所有Server默认端口号
    private String ZookeeperAddress;
    private String groupNode;

    public Client(String ZookeeperAddress,String groupNode) {  
    	this.ZookeeperAddress=ZookeeperAddress;
    	this.groupNode=groupNode;
    }

    public Results connect(Command command){
    	String vmdLocation;
    	String indexServer;
    	String taskServer;
    	if(command.getCommand()==Command.VideoUPLOAD){
    		String src=command.getArgs()[0];
    		String des=command.getArgs()[1];
    		uploadViaUtils(new Path(src),new Path(des));
    		String url=des+new Path(src).getName();
    		
    		System.out.println("GENERATE");
    		command=new Command(Command.VMDGENERATE,url);
    		
    		taskServer=getTaskServer();
    		if(taskServer.equals("no taskServer")){
    			results.results="no taskServer";
    			return results;
    		}
    		System.out.println(taskServer);
    		return submitTask(command,taskServer);
    	}
    	if(command.getCommand()==Command.VMDDELETE||command.getCommand()==Command.VMDGENERATE){
    		taskServer=getTaskServer();
    		if(taskServer.equals("no taskServer")){
    			results.results="no taskServer";
    			return results;
    		}
    		System.out.println(taskServer);
    		return submitTask(command,taskServer);
    	}
    	if(command.getCommand()==Command.GET_FRAME){
    		indexServer=getIndexServer();
    		vmdLocation=getVMDLocation(command,indexServer);
    		if(vmdLocation.equals("VMD NOT EXISTS")){
    			results.results="VMD NOT EXISTS";
    			return results;
    		}
    		results.results= getBufferedImage(command,vmdLocation);
    		return results;
    	}
    	if(command.getCommand()==Command.GET_FRAME_Index){
    		indexServer=getIndexServer();
    		vmdLocation=getVMDLocation(command,indexServer);
    		if(vmdLocation.equals("VMD NOT EXISTS")){
    			results.results="VMD NOT EXISTS";
    			return results;
    		}
    		results.results=getSeqAndIndex(command,vmdLocation);
    		return results;
    	}
    	if(command.getCommand()==Command.VMDGET){
    		indexServer=getIndexServer();
    		vmdLocation=getVMDLocation(command,indexServer);
    		if(vmdLocation.equals("VMD NOT EXISTS")){
    			results.results="VMD NOT EXISTS";
    			return results;
    		}
    		results.results=getVMD(command,vmdLocation);
    		return results;
    	}
    	if(command.getCommand()==Command.VMDLS){
    		results.results=getLs();
    		return results;
    	}
    	if(command.getCommand()==Command.GET_Task_Info){
    		taskServer=getTaskServer();
    		if(taskServer.equals("no taskServer")){
    			results.results="no taskServer";
    			return results;
    		}
    		LOG.info("向taskServer"+taskServer+"提交任务");
    		System.out.println("向taskServer"+taskServer+"提交任务");
    		return submitTask(command,taskServer);
    	}
    	if(command.getCommand()==Command.GET_ALL_RedisInfo){
    		results.results=get_ALL_RedisInfo();
    		return results;
    	}
    	if(command.getCommand()==Command.GET_IndexServerInfo){
    		results.results=get_IndexServerInfo();
    		return results;
    	}
    	if(command.getCommand()==Command.GET_TaskServerInfo){
    		results.results=get_TaskServerInfo();
    		return results;
    	}
    	if(command.getCommand()==Command.GET_ALL_NodeInfo){
    		results.results=get_ALL_NodeInfo();
    		return results;
    	}
    	return results;
    	
    }
    private Object get_ALL_RedisInfo() {
		// TODO Auto-generated method stub
    	ZooKeeper zooKeeper;
    	Map<String,String> re = new HashMap<String,String>();
    	try {
			zooKeeper = new ZooKeeper(ZookeeperAddress, 5000, new Watcher() {
			    @Override
			    public void process(WatchedEvent event) {
			        // 啥都不做

			    }
			});
			List<String> subList = zooKeeper.getChildren(groupNode, true);
			for (String subNode : subList) {
				if (subNode.startsWith("RedisUnit-")) {
					byte[] data = zooKeeper.getData(groupNode + "/" + subNode, true, new Stat());
					re.put(subNode, new String(data, "utf-8"));
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		return re;
	}

	private Object get_IndexServerInfo() {
		// TODO Auto-generated method stub
    	ZooKeeper zooKeeper;
    	Map<String,String> re = new HashMap<String,String>();
    	try {
			zooKeeper = new ZooKeeper(ZookeeperAddress, 5000, new Watcher() {
			    @Override
			    public void process(WatchedEvent event) {
			        // 啥都不做

			    }
			});
			List<String> subList = zooKeeper.getChildren(groupNode, true);
			for (String subNode : subList) {
				if (subNode.startsWith("IndexServer-")) {
					re.put(subNode, "no data");
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		return re;
	}

	private Object get_TaskServerInfo() {
		// TODO Auto-generated method stub
    	ZooKeeper zooKeeper;
    	Map<String,String> re = new HashMap<String,String>();
    	try {
			zooKeeper = new ZooKeeper(ZookeeperAddress, 5000, new Watcher() {
			    @Override
			    public void process(WatchedEvent event) {
			        // 啥都不做

			    }
			});
			List<String> subList = zooKeeper.getChildren(groupNode, true);
			for (String subNode : subList) {
				if (subNode.startsWith("TaskServer-")) {
					re.put(subNode, "no data");
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		return re;
	}

	private Map<String,String> get_ALL_NodeInfo() {
		// TODO Auto-generated method stub
    	ZooKeeper zooKeeper;
    	Map<String,String> re = new HashMap<String,String>();
    	try {
			zooKeeper = new ZooKeeper(ZookeeperAddress, 5000, new Watcher() {
			    @Override
			    public void process(WatchedEvent event) {
			        // 啥都不做

			    }
			});
			List<String> subList = zooKeeper.getChildren(groupNode, true);
			for (String subNode : subList) {
				if (subNode.startsWith("RedisUnit-")) {
					byte[] data = zooKeeper.getData(groupNode + "/" + subNode, true, new Stat());
					re.put(subNode, new String(data, "utf-8"));
				}
				else{
					re.put(subNode, "no data");
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		return re;
	}

	private Map<String,Map<String,Integer>> getLs() {
		// TODO Auto-generated method stub
    	ZooKeeper zooKeeper;
    	Map<String,Map<String,Integer>> re = new HashMap<String,Map<String,Integer>>();
		try {
			zooKeeper = new ZooKeeper(ZookeeperAddress, 5000, new Watcher() {
			    @Override
			    public void process(WatchedEvent event) {
			        // 啥都不做

			    }
			});
			List<String> subList = zooKeeper.getChildren(groupNode, true);
			for (String subNode : subList) {
				if (subNode.startsWith("RedisUnit-")) {
					byte[] data = zooKeeper.getData(groupNode + "/" + subNode, true, new Stat());
					String[] redisInfo= new String(data, "utf-8").split("-");
					if(redisInfo.length<4){
						LOG.error("解析RedisUntil失败："+new String(data, "utf-8"));
						break;
					}
					String ClusterName=redisInfo[0];
					Set<String> RedisClusterIPs = new HashSet<String>();
					for(int i=0;i<redisInfo.length-3;i++){
						RedisClusterIPs.add(redisInfo[1+i]);
					}				
					String ClusterPassport = redisInfo[redisInfo.length-2];
					String ValuedMemory=redisInfo[redisInfo.length-1];
					JedisSentinelPool jedisSentinelPool = new JedisSentinelPool(ClusterName,RedisClusterIPs,ClusterPassport);
					Map<String, Integer> map=new HashMap<String, Integer>();
					Jedis jedis = null;
					try {
						jedis = jedisSentinelPool.getResource();
						// jedis.set("key", "value");
						Set s = jedis.keys("*");
						Iterator it = s.iterator();

						while (it.hasNext()) {
							String url = (String) it.next();
							if (url.startsWith("hdfs://vm1:9000")) {
								SecondaryMeta temp=(SecondaryMeta)SerializationUtil.deserialize(jedis.get(url.getBytes()));
								map.put(url, temp.getFrameMetaInfoList().size());
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						jedisSentinelPool.returnBrokenResource(jedis);
					}
					re.put(ClusterName, map);
				} 
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		return re;
	}

	private SecondaryMetaClass.SecondaryMeta getVMD(Command command, String vmdLocation) {
		// TODO Auto-generated method stub
		String url = command.args[0];
		RedisCluster redisCluster = getRedisCluster(vmdLocation);

		JedisSentinelPool jedisSentinelPool = new JedisSentinelPool(redisCluster.ClusterName,
				redisCluster.RedisClusterIPs, redisCluster.ClusterPassport);
		SecondaryMeta SM = getVMDfromRedis(url,jedisSentinelPool);
		return SM;
	}

	private Long[] getSeqAndIndex(Command command, String vmdLocation) {
		// TODO Auto-generated method stub
		Long[] res = new Long[2];
		String url = command.args[0];
		int FrameNo = Integer.valueOf(command.args[1]);
		// System.out.println("正在处理：\n"+url+"\n"+FrameNo+" "+res[0]+" "+res[1]);
		// List<SecondaryMetaClass.SecondaryMeta.FrameInfoGroup> fig =
		// smf.get(url);
		RedisCluster redisCluster = getRedisCluster(vmdLocation);

		JedisSentinelPool jedisSentinelPool = new JedisSentinelPool(redisCluster.ClusterName,
				redisCluster.RedisClusterIPs, redisCluster.ClusterPassport);
		SecondaryMeta SM = getVMDfromRedis(url,jedisSentinelPool);
		if (SM == null) {
			LOG.error("no VMD of" + url + ",FAILED...");
			LOG.error("GETSEQANDINDEX FAILED");
			return res;
		}
		List<SecondaryMetaClass.SecondaryMeta.FrameInfoGroup> fig = SM.getFrameMetaInfoList();

		LOG.debug(fig.size());
		if (FrameNo < fig.get(0).getStartFrameNo()) {
			res[0] = 0L;
			res[1] = 0L;
			LOG.info("GETSEQANDINDEX SUCCESSED");
			return res;
		}
		int start = findStart(FrameNo, 0, fig.size() - 1, fig);
		res[0] = fig.get(start).getStartIndex();
		LOG.debug("test_startIndex : " + res[0]);
		res[1] = fig.get(start).getStartFrameNo();
		LOG.debug("test_StartFrameNo : " + res[1]);
		LOG.info("GETSEQANDINDEX SUCCESSED");
		return res;
	}

	private Set<BufferedImage> getBufferedImage(Command command, String vmdLocation) {
		// TODO Auto-generated method stub
		long[] res = new long[2];
		String url = command.args[0];
		int FrameNo = Integer.valueOf(command.args[1]);
		int FrameSize = Integer.valueOf(command.args[2]);
		// System.out.println("正在处理：\n"+url+"\n"+FrameNo+" "+res[0]+" "+res[1]);
		// List<SecondaryMetaClass.SecondaryMeta.FrameInfoGroup> fig =
		// smf.get(url);
		RedisCluster redisCluster = getRedisCluster(vmdLocation);

		JedisSentinelPool jedisSentinelPool = new JedisSentinelPool(redisCluster.ClusterName,
				redisCluster.RedisClusterIPs, redisCluster.ClusterPassport);
		SecondaryMeta SM = getVMDfromRedis(url,jedisSentinelPool);
		if (SM == null) {
			LOG.error("no VMD of" + url + ",FAILED...");
			LOG.error("GETSEQANDINDEX FAILED");
			return null;
		}
		List<SecondaryMetaClass.SecondaryMeta.FrameInfoGroup> fig = SM.getFrameMetaInfoList();

		LOG.debug(fig.size());
		if (FrameNo < fig.get(0).getStartFrameNo()) {
			res[0] = 0;
			res[1] = 0;
			LOG.info("GETSEQANDINDEX SUCCESSED");
		}else{
			int start = findStart(FrameNo, 0, fig.size() - 1, fig);
			res[0] = fig.get(start).getStartIndex();
			LOG.debug("test_startIndex : " + res[0]);
			res[1] = fig.get(start).getStartFrameNo();
			LOG.debug("test_StartFrameNo : " + res[1]);
			LOG.info("GETSEQANDINDEX SUCCESSED");
		}
		return GenerateBufferedImages(url,FrameNo,FrameSize,res);
	}
	
	private RedisCluster getRedisCluster(String vmdLocation) {
		// TODO Auto-generated method stub
		ZooKeeper zooKeeper;
		try {
			zooKeeper = new ZooKeeper(ZookeeperAddress, 5000, new Watcher() {
			    @Override
			    public void process(WatchedEvent event) {
			        // 啥都不做

			    }
			});

			byte[] data = zooKeeper.getData(groupNode + "/" + "RedisUnit-"+vmdLocation, true, new Stat());
			String[] redisInfo= new String(data, "utf-8").split("-");
			if(redisInfo.length<4){
				LOG.error("解析RedisUntil失败："+new String(data, "utf-8"));
				return null;
			}
			String ClusterName=redisInfo[0];
			Set<String> RedisClusterIPs = new HashSet<String>();
			for(int i=0;i<redisInfo.length-3;i++){
				RedisClusterIPs.add(redisInfo[1+i]);
			}				
			String ClusterPassport = redisInfo[redisInfo.length-2];
			String ValuedMemory=redisInfo[redisInfo.length-1];
			return new RedisCluster(ClusterName,RedisClusterIPs,ClusterPassport,ValuedMemory);	
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	private Set<BufferedImage> GenerateBufferedImages(String url, int frameNo, int frameSize, long[] res) {
		// TODO Auto-generated method stub
		Set<BufferedImage> re= new HashSet<BufferedImage>();
		String Path = url;
		URLProtocolManager mgr = URLProtocolManager.getManager();
        if (Path.startsWith("hdfs://"))
            mgr.registerFactory("hdfs", new HDFSProtocolHandlerFactory(ConfUtil.generate()));
        IContainer container = IContainer.make();
        int r = container.open(Path, IContainer.Type.READ, null);//782、784等转码得到的视频，使用时间作参数时，在这里会崩溃
        if (r < 0) throw new IllegalArgumentException("Can not open the video container of path {" + Path + "}");
        LOG.info("Xuggle video container opens");

        //获取视频流编号
        IStreamCoder coder = null;
        long framenum=0;
        int VideoStreamIndex = -1;
        for (int i = 0; i < container.getNumStreams(); i++) {
            if (container.getStream(i).getStreamCoder().getCodecType() == ICodec.Type.CODEC_TYPE_VIDEO) {
                VideoStreamIndex = i;
                coder = container.getStream(i).getStreamCoder();
                framenum=container.getStream(i).getNumFrames();
                break;
            }
        }        
        int targetFrame = frameNo;        
        if (VideoStreamIndex == -1) throw new IllegalArgumentException("this container doesn't hava a video stream.");
        LOG.info("the video stream id is " + VideoStreamIndex);

        //获取距离target最近的关键帧的时间偏移量，记录为before
        IStream videoStream = container.getStream(VideoStreamIndex);
        Long StartIndex = res[0];
        Long StartFrameNo = res[1];        

        //定位至目标TimeStamp，这里会自动调用之前写的HDFSProtocolHandler
        //*******************************重要方法************************
        //  参数分别是（流编号，时间偏移量，seek标志{0就是从起始位置开始seek}）
        container.seekKeyFrame(VideoStreamIndex,StartIndex,0);
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
        
    	int count=1;

        while(container.readNextPacket(packet) >= 0){
            if(packet.getStreamIndex() == VideoStreamIndex){
                IVideoPicture picture = IVideoPicture.make(coder.getPixelType(),coder.getWidth(),coder.getHeight());
                int offset = 0;
                while(offset < packet.getSize()){
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
                        if(count>targetFrame-StartFrameNo){
                        	BufferedImage bi = Utils.videoPictureToImage(newpic);
                        	re.add(bi);
                        	number++;
                        }
                        count++;
                        //ImageIO.write(bi,"jpg",new File("/home/b8311/yty/picture/"+number + ".jpg"));
                    }
                }
                //System.out.println(number);
                //保存frameSize张图片。
                if(number == frameSize) break;
            }
        }

        //千万别忘记将这两个对象close了
        coder.close();
        container.close();
    
		
		return re;
	}

	private String getTaskServer() {
		// TODO Auto-generated method stub
		ZooKeeper zooKeeper;
		try {
			zooKeeper = new ZooKeeper(ZookeeperAddress, 5000, new Watcher() {
			    @Override
			    public void process(WatchedEvent event) {
			        // 啥都不做

			    }
			});
			List<String> subList = zooKeeper.getChildren(groupNode, true);
			for (String subNode : subList) {
				if (subNode.startsWith("TaskServer-")) {
					return subNode.split("-")[1];
				} 
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return null;
	}
	private String getIndexServer() {
		// TODO Auto-generated method stub
		ZooKeeper zooKeeper;
		try {
			zooKeeper = new ZooKeeper(ZookeeperAddress, 5000, new Watcher() {
			    @Override
			    public void process(WatchedEvent event) {
			        // 啥都不做

			    }
			});
			List<String> subList = zooKeeper.getChildren(groupNode, true);
			for (String subNode : subList) {
				if (subNode.startsWith("IndexServer-")) {
					return subNode.split("-")[1];
				} 
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return null;
	}

	private String getVMDLocation(Command command, String indexServer) {
		// TODO Auto-generated method stub
		Results vmdLocation=new Results();
    	 //配置客户端NIO线程组
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY,true)
                    .handler(new ClientManageInitializer(command,vmdLocation));
            //发起异步连接操作
            LOG.debug("发起异步连接操作 - start");
            //System.out.println("发起异步连接操作 - start");
            
            ChannelFuture channelFuture = bootstrap.connect(indexServer,PORT).sync();
            
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
    	return (String)vmdLocation.results;
	}

	private Results submitTask(Command command, String taskServer) {
		// TODO Auto-generated method stub
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
            
            ChannelFuture channelFuture = bootstrap.connect(taskServer,PORT).sync();
            
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

	public static SecondaryMeta getVMDfromRedis(String url, JedisSentinelPool jedisSentinelPool) {
		Jedis jedis = null;
		SecondaryMeta re = null;
		try {
			jedis = jedisSentinelPool.getResource();
			re = (SecondaryMeta) SerializationUtil.deserialize(jedis.get(url.getBytes()));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			jedisSentinelPool.returnBrokenResource(jedis);
		}
		return re;
	}
	public static int findStart(int FrameNo, int s, int e, List<SecondaryMetaClass.SecondaryMeta.FrameInfoGroup> fig) {
		int start = 0;
		SecondaryMetaClass.SecondaryMeta.FrameInfoGroup temp = null;
		Iterator<SecondaryMetaClass.SecondaryMeta.FrameInfoGroup> iter = fig.iterator();
		while (iter.hasNext()) {
			temp = iter.next();
			if (temp.getStartFrameNo() > FrameNo) {
				start--;
				break;
			}
			start++;
		}
		return start;
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
}