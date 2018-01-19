package com.VMDServiceServer.VMDIndexManageService;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import com.Proto.SecondaryMetaClass.SecondaryMeta;
import com.UtilClass.Service.RedisCluster;
import com.UtilClass.Service.SerializationUtil;

public class IndexServer implements Watcher {
	/**
	 * 服务端绑定端口号
	 */
	private static int PORT;
	private static String LocalIP = "";
	private static Map<String, JedisSentinelPool> jedisSentinelPools = new ConcurrentHashMap<String, JedisSentinelPool>();
	// private static Map<String, RedisCluster> redisClusters = new
	// HashMap<String, RedisCluster>();//服务器中保存的数据 redis的访问路径
	// private static volatile ConcurrentHashMap<String, String> redisAndServer
	// = new ConcurrentHashMap<String, String>();//Master服务器中保存的数据
	// server与redis的对照表
	// private static volatile Map<String, String> vmdRouter = new
	// ConcurrentHashMap<String, String>();//Master服务器中保存的数据 路由表
	// 路由信息，包含了VMD的url、redis的name；
	private static volatile ConcurrentHashMap<String, Object> NodeAndInfo = new ConcurrentHashMap<String, Object>();
	private static volatile ConcurrentHashMap<String, String> VMDIndex = new ConcurrentHashMap<String, String>();
	/*
	 * zookeeper参数
	 */
	private static String groupNode = "/sgroup";
	private static ZooKeeper zooKeeper;
	private static String ZookeeperAddress = "172.16.10.101:2181";
	private String IndexServerPath = "";// 索引服务器创建的节点的路径

	public IndexServer() {
	}

	public IndexServer(int PORT, String IP, String ZookeeperAddress, String groupNode) {
		this.PORT = PORT;
		this.LocalIP = IP;
		this.ZookeeperAddress = ZookeeperAddress;
		this.groupNode = groupNode;
		
	}

	

	/**
	 * 日志
	 */
	public static final Log LOG = LogFactory.getLog(IndexServer.class);

	public static Map<String, Map<String, Integer>> getls(String filepath) {
		// TODO Auto-generated method stub
		Map<String, Map<String, Integer>> re = new HashMap<String, Map<String, Integer>>();
		for (String Node : NodeAndInfo.keySet()) {
			if (Node.startsWith("RedisUnit-")) {
				RedisCluster redisCluster = (RedisCluster) NodeAndInfo.get(Node);
				Map<String, Integer> mapa = new HashMap<String, Integer>();
				re.put(redisCluster.ClusterName, mapa);
			}

		}
		for (String url : VMDIndex.keySet()) {
			if (url.startsWith(filepath)) {
				String redisname = VMDIndex.get(url);
				JedisSentinelPool jedisSentinelPool = jedisSentinelPools.get(redisname);
				Jedis jedis = null;
				try {
					jedis = jedisSentinelPool.getResource();
					byte[] urlbyte = jedis.get(url.getBytes());
					if (urlbyte != null) {
						SecondaryMeta temp = (SecondaryMeta) SerializationUtil.deserialize(urlbyte);
						re.get(redisname).put(url, temp.getFrameMetaInfoList().size());
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					jedisSentinelPool.returnBrokenResource(jedis);
				}
			}

		}
		return re;
	}

	public static String findRedis(String url) {
		String redisname = VMDIndex.get(url);
		if (redisname == null) {
			return "VMD NOT EXISTS";
		} else {
			return redisname;
		}
	}

	public static String addVMDRE(String url, String redisname)
			throws UnsupportedEncodingException, KeeperException, InterruptedException {
		// TODO Auto-generated method stub
		// 从Redis中获取该vmd信息，更新VMDIndex。
		RedisCluster redisCluster = getRedisCluster(redisname);
		JedisSentinelPool jedisSentinelPool = jedisSentinelPools.get(redisCluster.ClusterName);
		Jedis jedis = null;
		Boolean vmdExist = null;
		String re = null;
		try {
			jedis = jedisSentinelPool.getResource();
			vmdExist = jedis.exists(url.getBytes());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			jedisSentinelPool.returnBrokenResource(jedis);
		}
		if (vmdExist.booleanValue()) {
			re = VMDIndex.putIfAbsent(url, redisname);
		}
		return re == null ? "addVMDRE SUCCESS" : "addVMDRE FAILED EXISTED";
	}

	public static String delVMDRE(String url)
			throws UnsupportedEncodingException, KeeperException, InterruptedException {
		// TODO Auto-generated method stub
		String redisname = VMDIndex.get(url);
		RedisCluster redisCluster = getRedisCluster(redisname);
		JedisSentinelPool jedisSentinelPool = jedisSentinelPools.get(redisCluster.ClusterName);
		Jedis jedis = null;
		Boolean vmdExist = null;
		String re = null;
		try {
			jedis = jedisSentinelPool.getResource();
			vmdExist = jedis.exists(url.getBytes());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			jedisSentinelPool.returnBrokenResource(jedis);
		}
		if (!vmdExist.booleanValue()) {
			re = VMDIndex.remove(url);
		}
		return re == null ? "delVMDRE FAILD" : "delVMDRE SUCCESS";
	}

	private static RedisCluster getRedisCluster(String redisname)
			throws KeeperException, InterruptedException, UnsupportedEncodingException {
		// TODO Auto-generated method stub
		byte[] data = zooKeeper.getData(groupNode + "/" + "RedisUnit-" + redisname, true, new Stat());
		String[] redisInfo = new String(data, "utf-8").split("-");
		if (redisInfo.length < 4) {
			LOG.error("解析RedisUntil失败：" + new String(data, "utf-8"));
			return null;
		}
		String ClusterName = redisInfo[0];
		Set<String> RedisClusterIPs = new HashSet<String>();
		for (int i = 0; i < redisInfo.length - 3; i++) {
			RedisClusterIPs.add(redisInfo[1 + i]);
		}
		String ClusterPassport = redisInfo[redisInfo.length - 2];
		String ValuedMemory = redisInfo[redisInfo.length - 1];
		return new RedisCluster(ClusterName, RedisClusterIPs, ClusterPassport, ValuedMemory);
	}

	public void bind() {
		// 调用Zookeeper服务
		// ServerMaster ac=new ServerMaster();
		try {
			connectZookeeper(ZookeeperAddress, "IndexServer-" + LocalIP);
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
					.option(ChannelOption.SO_BACKLOG, 1024 * 10)// 配置TCP参数,能够设置很多,这里就只设置了backlog=1024,
					.childHandler(new IndexServerInitializer());// 绑定I/O事件处理类
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
			System.out.println("退出,释放线程池资源");
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
	public void connectZookeeper(String zookeeperServerHost, String IndexServerName)
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
		IndexServerPath = zooKeeper.create(groupNodePath + "/" + IndexServerName, null, Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL);
		LOG.info("创建了IndexServer节点：" + IndexServerPath);
		System.out.println("connectZookeeper:SUCCESSED ");
		updateNodeAndInfo();
	}
	private void InitialVMDIndex() throws KeeperException, InterruptedException, UnsupportedEncodingException {
		// TODO Auto-generated method stub
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
					System.out.println("新增RedisUntil失败：" + new String(data, "utf-8"));
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
			} 
		}
		for(String nodename:newNodeAndInfo.keySet()){
			RedisCluster redisCluster = (RedisCluster) newNodeAndInfo.get(nodename);
			JedisSentinelPool jedisSentinelPool = new JedisSentinelPool(redisCluster.ClusterName,
					redisCluster.RedisClusterIPs, redisCluster.ClusterPassport);
			jedisSentinelPools.put(redisCluster.ClusterName, jedisSentinelPool);
			Jedis jedis = null;
			try {
				jedis = jedisSentinelPool.getResource();
				// jedis.set("key", "value");
				Set s = jedis.keys("*");
				Iterator it = s.iterator();

				while (it.hasNext()) {
					String key = (String) it.next();
					// 判断ip地址是否与正则表达式匹配
					if (key.startsWith("hdfs://vm1:9000")) {
						VMDIndex.put(key, redisCluster.ClusterName);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				jedisSentinelPool.returnBrokenResource(jedis);
			}
		}
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
					System.out.println("新增RedisUntil失败：" + new String(data, "utf-8"));
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

		updateVMDIndex(newNodeAndInfo);
		// 替换NodeAndInfo列表
		NodeAndInfo = newNodeAndInfo;
		LOG.info("更新了VMDIndex表");
		LOG.info("更新了NodeAndInfo列表:" + NodeAndInfo);
		System.out.println("更新了NodeAndInfo列表:" + NodeAndInfo);
	}

	private void updateVMDIndex(ConcurrentHashMap<String, Object> newNodeAndInfo) {
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

		// 删掉下线RedisUnit上的VMD的索引
		if (newOffline.size() > 0) {
			for (String VMDurl : VMDIndex.keySet()) {
				if (newOffline.contains("RedisUnit-" + VMDIndex.get(VMDurl))) {
					VMDIndex.remove(VMDurl);
				}
			}
		}
		// 新增的RedisUnit
		for (String nodename : newNodeAndInfo.keySet()) {
			if (nodename.startsWith("RedisUnit-") && (!NodeAndInfo.keySet().contains(nodename))) {
				newOnline.add(nodename);
			}
		}
		// 增加上线RedisUnit上的VMD的索引
		for (String nodename : newOnline) {

			RedisCluster redisCluster = (RedisCluster) newNodeAndInfo.get(nodename);
			JedisSentinelPool jedisSentinelPool = new JedisSentinelPool(redisCluster.ClusterName,
					redisCluster.RedisClusterIPs, redisCluster.ClusterPassport);
			jedisSentinelPools.put(redisCluster.ClusterName, jedisSentinelPool);
			Jedis jedis = null;
			try {
				jedis = jedisSentinelPool.getResource();
				// jedis.set("key", "value");
				Set s = jedis.keys("*");
				Iterator it = s.iterator();

				while (it.hasNext()) {
					String key = (String) it.next();
					// 判断ip地址是否与正则表达式匹配
					if (key.startsWith("hdfs://vm1:9000")) {
						VMDIndex.put(key, redisCluster.ClusterName);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				jedisSentinelPool.returnBrokenResource(jedis);
			}
		}
		// 对比新旧NodeAndInfo，判断TaskServer是否失联
		for (String nodename : NodeAndInfo.keySet()) {
			if (nodename.startsWith("TaskServer-") && (!newNodeAndInfo.keySet().contains(nodename))) {
				for(String clusterName:jedisSentinelPools.keySet()){
					JedisSentinelPool jedisSentinelPool=jedisSentinelPools.get(clusterName);
					Jedis jedis = null;
					try {
						jedis = jedisSentinelPool.getResource();
						// jedis.set("key", "value");
						Set s = jedis.keys("*");
						Iterator it = s.iterator();

						while (it.hasNext()) {
							String key = (String) it.next();
							// 判断ip地址是否与正则表达式匹配
							if (key.startsWith("hdfs://vm1:9000")) {
								VMDIndex.put(key, clusterName);
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						jedisSentinelPool.returnBrokenResource(jedis);
					}
				}
			}
		}
	}

	/*
	 * private void downloadBarance(){ LOG.info("uploadBarance - start"); new
	 * Thread(new Runnable() { public void run() { while(true){ try {
	 * Thread.sleep(60000); Map<String,Double> newRedisLoadBalance=new
	 * ConcurrentHashMap<String,Double>(); for (RedisCluster redisCluster :
	 * redisClusters.values()) {
	 * 
	 * double lbrate=(double)0.1; String redisname=redisCluster.ClusterName;
	 * newRedisLoadBalance.put(redisname, lbrate); }
	 * redisLoadBalance=newRedisLoadBalance; } catch (Exception e) {
	 * e.printStackTrace(); } showInitial(); }
	 * 
	 * } }).start(); }
	 */

	/**
	 * 更新服务器节点的负载数据
	 * 
	 * @param serverNodePath
	 * @throws InterruptedException
	 * @throws KeeperException
	 * @throws UnsupportedEncodingException
	 */
	/*
	 * private void updateServerLoadBalance(String serverNodePath) throws
	 * KeeperException, InterruptedException, UnsupportedEncodingException{
	 * ServerInfo serverInfo=serverList.get(serverNodePath);
	 * if(null!=serverInfo){ //获取每个子节点下关联的服务器负载的信息 byte[]
	 * data=zk.getData(serverInfo.getPath(), true, stat); String loadBalance=new
	 * String(data,"utf-8"); serverInfo.setLoadBalance(loadBalance);
	 * serverList.put(serverInfo.getPath(), serverInfo);
	 * LOG.info("更新了服务器的负载："+serverInfo);
	 * LOG.info("更新服务器负载后，服务器列表信息："+serverList); } }
	 */

	@Override
	public void process(WatchedEvent event) {
		LOG.debug("监听到zookeeper事件-----eventType:" + event.getType() + ",path:" + event.getPath());
		System.out.println("监听到zookeeper事件-----eventType:" + event.getType() + ",path:" + event.getPath());
		// 集群总节点的子节点变化触发的事件
		if (event.getType() == EventType.NodeChildrenChanged && event.getPath().equals(groupNode)) {
			// 如果发生了"/sgroup"节点下的子节点变化事件, 更新server列表, 并重新注册监听
			try {
				updateNodeAndInfo();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * client的工作逻辑写在这个方法中 此处不做任何处理, 只让client sleep
	 * 
	 * @throws InterruptedException
	 */
	public void handle() throws InterruptedException {
		Thread.sleep(Long.MAX_VALUE);
	}

	public static String getVMDLocation(String url) {
		// TODO Auto-generated method stub
		return VMDIndex.get(url);
	}

}
