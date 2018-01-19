package com.VMDServiceServer.VMDTaskManageService;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ReferenceCountUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.net.SocketAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.Proto.SecondaryMetaClass;
import com.Proto.SecondaryMetaClass.SecondaryMeta;
import com.Proto.SecondaryMetaClass.SecondaryMeta.FrameInfoGroup;
import com.UtilClass.Service.Command;
import com.UtilClass.Service.ConfUtil;
import com.UtilClass.Service.RedisCluster;
import com.UtilClass.Service.Results;
import com.UtilClass.Service.SerializationUtil;
import com.UtilClass.Service.UploadFile;
import com.UtilClass.VMD.VMDFileUtil;
import com.UtilClass.VMD.VideoMetaData;
import com.VMDServiceClientAPI.ClientManageInitializer;

public class TaskServer implements Watcher {
	/**
	 * 服务端绑定端口号
	 */
	private static int PORT;
	private static String LocalIP = "";
	private static Map<String, JedisSentinelPool> jedisSentinelPools = new ConcurrentHashMap<String, JedisSentinelPool>();

	/*
	 * zookeeper参数
	 */
	private static String ZookeeperAddress = "172.16.10.101:2181";
	private static String groupNode = "/sgroup";
	private static ZooKeeper zooKeeper;
	private static String TaskServerPath = "";// Task服务器创建的节点的路径
	private static String TaskLogPath = "/home/b8311/Experiment/ExperimentTaskLog/";

	private static String indexServer;
	// private static Map<String, RedisCluster> redisClusters = new
	// HashMap<String, RedisCluster>();
	private static volatile ConcurrentHashMap<String, Object> NodeAndInfo = new ConcurrentHashMap<String, Object>();
	private static volatile ConcurrentLinkedQueue<Task> TaskQueue = new ConcurrentLinkedQueue<Task>();

	public TaskServer(int PORT, String IP, String ZookeeperAddress, String groupNode, String TaskLogPath) {
		this.PORT = PORT;
		this.LocalIP = IP;
		this.ZookeeperAddress = ZookeeperAddress;
		this.groupNode = groupNode;
		this.TaskLogPath = TaskLogPath;
		InitialTaskQueue();

	}

	private void InitialTaskQueue() {
		if(!new File(TaskLogPath+"TaskLog.txt").exists())
			return;
		ConcurrentLinkedQueue<Task> newTaskQueue = new ConcurrentLinkedQueue<Task>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(TaskLogPath + "TaskLog.txt"));
			String tempString = null;
			while ((tempString = br.readLine()) != null) {
				Task task = new Task(tempString);
				if (task.getTaskStatu().equals("completed") || task.getTaskStatu().equals("failed")) {
					newTaskQueue.poll();
				} else {
					newTaskQueue.add(task);
				}
			}
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String oldfilePath = TaskLogPath + "TaskLog.txt";
		Date date = new Date();
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
		String CurrentTime = format.format(date);
		String newfilepath = TaskLogPath + "TaskLog" + CurrentTime + ".txt";
		File oldfile = new File(oldfilePath);
		File newfile = new File(newfilepath);
		if (oldfile.exists()) {
			oldfile.renameTo(newfile);
		}
		FileWriter fw = null;
		try {
			fw = new FileWriter(oldfilePath, true);
			for (Task tempTask : newTaskQueue) {
				fw.write(tempTask.toLogString() + "\n");
				fw.flush();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (fw != null)
				try {
					fw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		TaskQueue = newTaskQueue;
	}

	public TaskServer() {

	}

	/**
	 * 日志
	 */
	public static final Log LOG = LogFactory.getLog(TaskServer.class);

	public static String generateSMF(String url) throws IOException, KeeperException, InterruptedException {

		SecondaryMeta SM = VideoMetaData.generateViaIndexEntry(url, ConfUtil.generate("vm1", "9000", "vm1"));
		if(SM==null)
			return "GENERATE FAILED";
		// 获取VMD的存储位置
		String VMDLocation = getValuedRedis(VideoMetaData.getSize(SM));
		System.out.println("新生成的VMD将存储在RedisUnit：" + VMDLocation);
		JedisSentinelPool jedisSentinelPool = jedisSentinelPools.get(VMDLocation);
		if (existsKeyOnRedis(url, jedisSentinelPool)) {
			LOG.info("VMD of " + url + "exist,Generate End...");
			LOG.info("GENERATE EXISTS");
			return "GENERATE EXISTS";
		}
		if (!putVMDonRedis(url, SM, VMDLocation).equals("OK")) {
			LOG.error("putVMDonRedis:FAILED");
			LOG.error("GENERATE FAILED");
			return "GENERATE FAILED";
		} else {
			while (true) {
				Command command = new Command(Command.ADD_VMDRE, url, VMDLocation);
				Results results = uploadVMDInfo(command);
				LOG.debug("uploadVMDInfo:ADD_VMDRE:" + results.results);
				if (results.results.equals("addVMDRE SUCCESS")) {
					return "GENERATE SUCCESSED";
				}
			}
		}
	}

	private static String getValuedRedis(Long size) {
		// TODO Auto-generated method stub
		String valuedRedis = "";
		System.out.println("start to findRedis");
		for (String redisname : jedisSentinelPools.keySet()) {
			JedisSentinelPool jedisSentinelPool = jedisSentinelPools.get(redisname);
			Jedis jedis = null;
			redis.clients.jedis.Client jedisclient = null;
			try {
				jedis = jedisSentinelPool.getResource();
				String info = "";
				try {
					jedisclient = jedis.getClient();
					jedisclient.info();
					info = jedisclient.getBulkReply();
				} finally {
					// 关闭流
					jedisclient.close();
				}
				String[] strs = info.split("\n");
				Long usedMemory = 0L;
				Long maxMemory = 0L;
				for (int i = 0; i < strs.length; i++) {
					String s = strs[i];
					String[] detail = s.split(":");
					if (detail[0].equals("used_memory")) {
						usedMemory = getIntFromString(detail[1]);
						break;
					}
				}
				for (int i = 0; i < strs.length; i++) {
					String s = strs[i];
					String[] detail = s.split(":");
					if (detail[0].equals("maxmemory")) {
						maxMemory = getIntFromString(detail[1]);
						maxMemory = maxMemory > 0 ? maxMemory : 4L * 1024 * 1024 * 1024;// 默认4G最大内存
						break;
					}
				}
				if (maxMemory - usedMemory > size + 1024 * 1024) {
					System.out.println(redisname);
					return redisname;
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				jedisSentinelPool.returnBrokenResource(jedis);
			}
		}

		System.out.println(valuedRedis);
		return valuedRedis;
	}

	public static Long getIntFromString(String input) {
		// TODO Auto-generated method stub
		Long re = 0L;
		for (int i = 0; i < input.length(); i++) {
			char t = input.charAt(i);
			if (t >= '0' && t <= '9') {
				re = re * 10 + t - '0';
			}
		}
		return re;
	}

	public static String deleteSMF(String url) throws KeeperException, InterruptedException {
		// 获取VMD的存储位置
		String VMDLocation = getVMDLocation(url);
		System.out.println("待删除的VMD存储在RedisUnit：" + VMDLocation);
		// 删除数据
		JedisSentinelPool jedisSentinelPool = jedisSentinelPools.get(VMDLocation);
		if (!existsKeyOnRedis(url, jedisSentinelPool)) {
			LOG.info("no VMD of" + url + ",Delete End...");
			LOG.info("DELETE NOT EXISTS");
			return "DELETE NOT EXISTS";
		}
		Long d = delVMDfromRedis(url, VMDLocation);

		if (d != 1 && d != 0) {
			LOG.error("delVMDfromRedis:FAILED");
			LOG.error("DELETE FAILED");
			return "DELETE FAILED";
		} else {
			while (true) {
				Command command = new Command(Command.DET_VMDRE, url);
				Results results = uploadVMDInfo(command);
				LOG.debug("uploadVMDInfo:ADD_VMDRE:" + results.results);
				if (results.results.equals("delVMDRE SUCCESS")) {
					LOG.info("deleteSMF:SUCCESSED");
					System.out.println("deleteSMF:SUCCESSED");
					return "DELETE SUCCESSED";
				}
			}
		}
	}

	private static String getVMDLocation(String url) throws KeeperException, InterruptedException {
		// TODO Auto-generated method stub
		indexServer = getIndexServer();
		System.out.println("找到indexServer:" + indexServer);
		int indexServerPORT = 8000;
		Results vmdLocation = new Results();
		// 配置客户端NIO线程组
		EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
		try {
			Bootstrap bootstrap = new Bootstrap();
			bootstrap.group(eventLoopGroup).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true)
					.handler(new TaskServerMessageInitializer(new Command(Command.VMDDELETE, url), vmdLocation));
			// 发起异步连接操作
			LOG.debug("发起异步连接操作 - start");
			// System.out.println("发起异步连接操作 - start");

			ChannelFuture channelFuture = bootstrap.connect(indexServer, indexServerPORT).sync();

			LOG.debug("发起异步连接操作 - end");
			// System.out.println("发起异步连接操作 - end");

			// 等待客户端链路关闭
			LOG.debug("等待客户端链路关闭 - start");
			// System.out.println("等待客户端链路关闭 - start");

			channelFuture.channel().closeFuture().sync();

			LOG.debug("等待客户端链路关闭 - end");
			// System.out.println("等待客户端链路关闭 - end");

		} catch (InterruptedException e) {
			LOG.error(e.getMessage(), e);
			System.out.println(e.getMessage());
		} finally {
			// 关闭
			eventLoopGroup.shutdownGracefully();
		}
		LOG.info("getHost from  serverMaster:SUCCESS");
		return (String) vmdLocation.results;
	}

	private static String getIndexServer() throws KeeperException, InterruptedException {
		// TODO Auto-generated method stub
		List<String> subList = zooKeeper.getChildren(groupNode, true);
		for (String subNode : subList) {
			if (subNode.startsWith("IndexServer-")) {
				return subNode.split("-")[1];
			}
		}
		return null;
	}

	// redis上的数据操作
	public static Boolean existsKeyOnRedis(String url, JedisSentinelPool jedisSentinelPool) {
		Jedis jedis = null;
		Boolean re = null;
		try {
			jedis = jedisSentinelPool.getResource();
			re = jedis.exists(url.getBytes());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			jedisSentinelPool.returnBrokenResource(jedis);
		}
		return re;

	}

	public static String putVMDonRedis(String url, SecondaryMeta SM, String VMDLocation)
			throws KeeperException, InterruptedException {
		JedisSentinelPool jedisSentinelPool = jedisSentinelPools.get(VMDLocation);
		Jedis jedis = null;
		String re = "";
		try {
			jedis = jedisSentinelPool.getResource();
			re = jedis.set(url.getBytes(), SerializationUtil.serialize(SM));
			LOG.debug("jedis.set:" + re);
			return re;

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			jedisSentinelPool.returnBrokenResource(jedis);
		}
		return re;

		/*
		 * JedisSentinelPool jedisSentinelPool =
		 * jedisSentinelPools.get(VMDLocation); Command command = new
		 * Command(Command.ADD_VMDRE, url,VMDLocation); Results results =
		 * uploadVMDInfo(command); LOG.debug("uploadVMDInfo:ADD_VMDRE:" +
		 * results.results); Jedis jedis = null; String re = ""; try { jedis =
		 * jedisSentinelPool.getResource(); if (results.results.equals(
		 * "addVMDRE SUCCESS")) { re = jedis.set(url.getBytes(),
		 * SerializationUtil.serialize(SM)); LOG.debug("jedis.set:" + re); }
		 * else { re = "ServerMaster:" + results.results; LOG.debug(re); return
		 * re; } if (!re.equals("OK")) { LOG.debug("!re.equals:" + re); Command
		 * cm = new Command(Command.DET_VMDRE, url); re += uploadVMDInfo(cm);
		 * LOG.debug("uploadVMDInfo:DET_VMDRE:" + re); } } catch (Exception e) {
		 * e.printStackTrace(); } finally {
		 * jedisSentinelPool.returnBrokenResource(jedis); } return re;
		 */
	}

	public static Long delVMDfromRedis(String url, String VMDLocation) throws KeeperException, InterruptedException {
		JedisSentinelPool jedisSentinelPool = jedisSentinelPools.get(VMDLocation);
		Jedis jedis = null;
		Long re = -1L;
		try {
			jedis = jedisSentinelPool.getResource();
			re = jedis.del(url.getBytes());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			jedisSentinelPool.returnBrokenResource(jedis);
		}
		return re;

		/*
		 * JedisSentinelPool jedisSentinelPool =
		 * jedisSentinelPools.get(VMDLocation); Jedis jedis = null; Long re =
		 * -1L; try { jedis = jedisSentinelPool.getResource(); re =
		 * jedis.del(url.getBytes()); } catch (Exception e) {
		 * e.printStackTrace(); } finally {
		 * jedisSentinelPool.returnBrokenResource(jedis); } if (re != 1) {
		 * return re; } Command command = new Command(Command.DET_VMDRE, url);
		 * Results results = uploadVMDInfo(command);
		 * 
		 * LOG.info(results.results); return re;
		 */
	}

	public void bind() {
		// 调用Zookeeper服务

		try {
			connectZookeeper(ZookeeperAddress, "TaskServer-" + LocalIP);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (KeeperException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			doTask();
		} catch (KeeperException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		/*
		 * Logger root = Logger.getRootLogger(); root.addAppender(new
		 * ConsoleAppender( new
		 * PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
		 * root.setLevel(Level.INFO);
		 */
		/*
		 * NioEventLoopGroup是线程池组 包含了一组NIO线程,专门用于网络事件的处理 bossGroup:服务端,接收客户端连接
		 * workGroup:进行SocketChannel的网络读写
		 */
		EventLoopGroup bossGroup = new NioEventLoopGroup();
		EventLoopGroup workGroup = new NioEventLoopGroup();
		try {
			/*
			 * ServerBootstrap:用于启动NIO服务的辅助类,目的是降低服务端的开发复杂度
			 */
			ServerBootstrap serverBootstrap = new ServerBootstrap();
			serverBootstrap.group(bossGroup, workGroup).channel(NioServerSocketChannel.class)
					.option(ChannelOption.SO_SNDBUF, 1024 * 1024).option(ChannelOption.SO_BACKLOG, 1024 * 10)// 配置TCP参数,能够设置很多,这里就只设置了backlog=1024,
					.childHandler(new TaskServerInitializer());// 绑定I/O事件处理类
			LOG.debug("绑定端口号:" + PORT + ",等待同步成功");
			System.out.println("绑定端口号:" + PORT + ",等待同步成功");
			/*
			 * bind:绑定端口 sync:同步阻塞方法,等待绑定完成,完成后返回 ChannelFuture ,主要用于通知回调
			 */
			ChannelFuture channelFuture = serverBootstrap.bind(PORT).sync();
			LOG.debug("等待服务端监听窗口关闭");
			System.out.println("等待服务端监听窗口关闭");
			/*
			 * closeFuture().sync():为了阻塞,服务端链路关闭后才退出.也是一个同步阻塞方法
			 */
			channelFuture.channel().closeFuture().sync();
		} catch (InterruptedException e) {
			LOG.error(e.getMessage(), e);
			System.out.println(e.getMessage());
		} finally {
			LOG.debug("退出,释放线程池资源");
			// System.out.println("退出,释放线程池资源");
			bossGroup.shutdownGracefully();
			workGroup.shutdownGracefully();
		}
	}

	/**
	 * 连接zookeeper服务器，并在集群总结点下创建EPHEMERAL类型的子节点，把服务器名称存入子节点的数据
	 * 
	 * @param zookeeperServerHost
	 * @param serverName
	 * @throws IOException
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void connectZookeeper(String zookeeperServerHost, String TaskServerName)
			throws IOException, KeeperException, InterruptedException {
		zooKeeper = new ZooKeeper(zookeeperServerHost, 5000, this);
		// 先判断sgroup节点是否存在
		String groupNodePath = groupNode;
		Stat stat = zooKeeper.exists(groupNodePath, false);
		if (null == stat) {
			zooKeeper.create(groupNodePath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			LOG.info("creat a groupNode:SUCCESSED");
			System.out.println("creat a groupNode:SUCCESSED");

		}
		LOG.info("connectZookeeper:SUCCESSED");
		// 将server的地址数据关联到新创建的子节点上
		TaskServerPath = zooKeeper.create(groupNodePath + "/" + TaskServerName, null, Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL);
		LOG.info("创建了TaskServer节点：" + TaskServerPath);
		LOG.info("connectZookeeper:SUCCESSED ");
		System.out.println("connectZookeeper:SUCCESSED ");

		updateNodeAndInfo();
	}

	/**
	 * 更新NodeAndInfo信息
	 * 
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws UnsupportedEncodingException
	 */
	private synchronized void updateNodeAndInfo()
			throws KeeperException, InterruptedException, UnsupportedEncodingException {

		ConcurrentHashMap<String, Object> newNodeAndInfo = new ConcurrentHashMap<String, Object>();
		List<String> subList = zooKeeper.getChildren(groupNode, true);
		for (String subNode : subList) {
			// 获取每个子节点下关联的节点的信息
			byte[] data = zooKeeper.getData(groupNode + "/" + subNode, true, new Stat());
			if (subNode.startsWith("RedisUnit-")) {
				// 取出RedisUnit的数据信息
				String[] redisInfo = new String(data, "utf-8").split("-");
				if (redisInfo.length < 4) {
					LOG.error("新增RedisUntil失败：" + new String(data, "utf-8"));
					return;
				}
				String ClusterName = redisInfo[0];
				Set<String> RedisClusterIPs = new HashSet<String>();
				for (int i = 0; i < redisInfo.length - 3; i++) {
					RedisClusterIPs.add(redisInfo[1 + i]);
				}
				String ClusterPassport = redisInfo[redisInfo.length - 2];
				String ValuedMemory = redisInfo[redisInfo.length - 1];
				newNodeAndInfo.put(subNode,
						new RedisCluster(ClusterName, RedisClusterIPs, ClusterPassport, ValuedMemory));
			} else {
				// 其他类型Node中暂时未存入数据
				Object NodeData = "no data ";
				newNodeAndInfo.put(subNode, NodeData);
			}
		}
		updateJedisSentinelPools(newNodeAndInfo);
		// 替换NodeAndInfo列表
		NodeAndInfo = newNodeAndInfo;
		LOG.info("更新了NodeAndInfo列表:" + NodeAndInfo);
		System.out.println("更新了NodeAndInfo列表:" + NodeAndInfo);
	}

	private void updateJedisSentinelPools(ConcurrentHashMap<String, Object> newNodeAndInfo) {
		// 对比新旧NodeAndInfo，找到相关RedisUnit
		List<String> newOffline = new ArrayList<String>();
		List<String> newOnline = new ArrayList<String>();
		// 下线的RedisUnit
		for (String nodename : NodeAndInfo.keySet()) {
			if (nodename.startsWith("RedisUnit-") && (!newNodeAndInfo.keySet().contains(nodename))) {
				newOffline.add(nodename);
			}
		}
		// 删掉下线RedisUnit的jedisSentinelPool
		for (String nodename : newOffline) {
			RedisCluster redisCluster = (RedisCluster) NodeAndInfo.get(nodename);
			JedisSentinelPool jedisSentinelPool = jedisSentinelPools.get(redisCluster.ClusterName);
			jedisSentinelPool.close();
			jedisSentinelPools.remove(redisCluster.ClusterName);
		}
		// 新增的RedisUnit
		for (String nodename : newNodeAndInfo.keySet()) {
			if (nodename.startsWith("RedisUnit-") && (!NodeAndInfo.keySet().contains(nodename))) {
				newOnline.add(nodename);
			}
		}
		// 增加上线RedisUnit的jedisSentinelPool
		for (String nodename : newOnline) {

			RedisCluster redisCluster = (RedisCluster) newNodeAndInfo.get(nodename);
			JedisSentinelPool jedisSentinelPool = new JedisSentinelPool(redisCluster.ClusterName,
					redisCluster.RedisClusterIPs, redisCluster.ClusterPassport);
			jedisSentinelPools.put(redisCluster.ClusterName, jedisSentinelPool);
		}
	}

	@Override
	public void process(WatchedEvent event) {
		LOG.debug("监听到zookeeper事件-----eventType:" + event.getType() + ",path:" + event.getPath());
		LOG.info("监听到zookeeper事件-----eventType:" + event.getType() + ",path:" + event.getPath());
		// 集群总节点的子节点变化触发的事件
		if (event.getType() == EventType.NodeChildrenChanged && event.getPath().equals(groupNode)) {
			// 如果发生了"/sgroup"节点下的子节点变化事件,更新NodeAndInfo列表, 并重新注册监听
			try {
				updateNodeAndInfo();
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		if (event.getType() == EventType.NodeDataChanged
				&& event.getPath().startsWith(groupNode + "/" + "RedisUnit-")) {
			// 如果发生了RedisUnit节点数据变化事件, 更新NodeAndInfo列表, 并重新注册监听
			try {
				updateNodeAndInfo();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 关闭于zookeeper服务器的连接
	 * 
	 * @throws InterruptedException
	 */
	public void closeZookeeper() throws InterruptedException {
		if (null != zooKeeper) {
			zooKeeper.close();
		}
	}

	// 服务器端主动通知服务器Master更新路由表
	public static Results uploadVMDInfo(Command command) throws KeeperException, InterruptedException {
		Results results = new Results();
		String indexServer = getIndexServer();
		int indexServerPORT = 8000;
		// 配置客户端NIO线程组
		EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
		try {
			Bootstrap bootstrap = new Bootstrap();
			bootstrap.group(eventLoopGroup).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true)
					.handler(new TaskServerMessageInitializer(command, results));
			// 发起异步连接操作
			LOG.debug("发起异步连接操作 - start");
			// System.out.println("发起异步连接操作 - start");

			ChannelFuture channelFuture = bootstrap.connect(indexServer, indexServerPORT).sync();

			LOG.debug("发起异步连接操作 - end");
			// System.out.println("发起异步连接操作 - end");

			// 等待客户端链路关闭
			LOG.debug("等待客户端链路关闭 - start");
			// System.out.println("等待客户端链路关闭 - start");

			channelFuture.channel().closeFuture().sync();

			LOG.debug("等待客户端链路关闭 - end");
			// System.out.println("等待客户端链路关闭 - end");

		} catch (InterruptedException e) {
			LOG.error(e.getMessage(), e);
			System.out.println(e.getMessage());
		} finally {
			// 关闭
			eventLoopGroup.shutdownGracefully();
		}
		LOG.info("inform serverMaster the new VMD:" + results);
		// System.out.println("uploadVMDInfo - end");
		return results;
	}

	public static void doTask() throws KeeperException, InterruptedException, IOException {
		new Thread(new Runnable() {
			public void run() {
				while (true) {
					while (!TaskQueue.isEmpty()) {
						System.out.println("当前任务队列不为空！正在处理...");
						String taskResult = "";
						Task task = TaskQueue.peek();
						task.setTaskStatu("processing");
						FileWriter fw = null;
						switch (task.getCommand().Type) {
						case Command.VMDDELETE:
							try {
								taskResult = deleteSMF(task.getCommand().args[0]);
							} catch (KeeperException | InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							if (taskResult.equals("DELETE FAILED")) {
								LOG.error("doTask-deleteSMF:" + taskResult);
								task.setTaskStatu("failed");
							}else{
								task.setTaskStatu("completed");
							}
							TaskQueue.poll();
							try {
								fw = new FileWriter(TaskLogPath + "TaskLog.txt", true);
								fw.write(task.toLogString() + "\n");
								fw.flush();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} finally {
								if (fw != null)
									try {
										fw.close();
									} catch (IOException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
							}
							break;
						case Command.VMDGENERATE:
							try {
								taskResult = generateSMF(task.getCommand().args[0]);
							} catch (IOException | KeeperException | InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							if (taskResult.equals("GENERATE FAILED")) {
								LOG.error("doTask-generateSMF:" + taskResult);
								task.setTaskStatu("failed");
							}else{
								task.setTaskStatu("completed");
							}
							TaskQueue.poll();
							try {
								fw = new FileWriter(TaskLogPath + "TaskLog.txt", true);
								fw.write(task.toLogString() + "\n");
								fw.flush();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} finally {
								if (fw != null)
									try {
										fw.close();
									} catch (IOException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
							}
							break;
						default:
							break;
						}
					}
					while (TaskQueue.isEmpty()) {
						//System.out.println("当前任务队列为空！等待新任务提交...");
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
		}).start();

	}

	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		System.out.print("请输入服务器名称（如server001）:");
		Scanner scan = new Scanner(System.in);
		String serverName = scan.nextLine();
		TaskServer Server = new TaskServer();
		Server.connectZookeeper("172.16.10.101:2181", serverName);
		while (true) {
			System.out.println("请输入您的操作指令(exit 退出系统)：");
			String command = scan.nextLine();
			if ("exit".equals(command)) {
				System.out.println("服务器关闭中....");
				Server.zooKeeper.close();
				System.exit(0);
				break;
			} else {
				continue;
			}
		}
	}

	public static String submit(Command command, SocketAddress remoteAddress) {
		// TODO Auto-generated method stub
		Task task = new Task(remoteAddress, command);
		if (TaskQueue.add(task)) {
			FileWriter fw = null;
			try {
				fw = new FileWriter(TaskLogPath + "TaskLog.txt", true);
				fw.write(task.toLogString() + "\n");
				fw.flush();
				return "submit Success:"+task.toLogString();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				if (fw != null)
					try {
						fw.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}
		}
		return "submit Failed";
	}

	public static String getTaskInfo(SocketAddress remoteAddress) {
		// TODO Auto-generated method stub
		String remoteIP = remoteAddress.toString().split(":")[0].substring(1);
		System.out.println("来自" + remoteIP + "的getTaskInfo请求");
		String TaskInfo = "";
		ConcurrentLinkedQueue<Task> tempTaskQueue = new ConcurrentLinkedQueue<Task>(TaskQueue);
		while (!tempTaskQueue.isEmpty()) {
			Task task = tempTaskQueue.poll();
			if (task.getClientIP().toString().split(":")[0].substring(1).equals(remoteIP)) {
				TaskInfo += task.toLogString() + "\r\n";
			}
		}
		if (TaskInfo.equals("")) {
			TaskInfo = "no Task in TaskQueue!";
		}
		System.out.println("向" + remoteIP + "返回getTaskInfo结果：" + TaskInfo);
		return TaskInfo;
	}

}
