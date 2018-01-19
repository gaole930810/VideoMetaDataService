package com.VMDServiceStorage;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

public class RedisMonitor implements Watcher {
	private String RedisUnitname;
	private String RedisUnitIPs;
	private String RedisUnitPassword;
	private JedisSentinelPool jedisSentinelPool;
	private String ZookeeperAddress = "172.16.10.101:2181";
	private String groupNode = "/sgroup";
	private ZooKeeper zooKeeper;
	private String valuedMemory;

	public RedisMonitor() {
	}

	public RedisMonitor(String RedisUnitname, String RedisUnitIPs, String RedisUnitPassword, String ZookeeperAddress,
			String groupNode) {
		this.RedisUnitname = RedisUnitname;
		this.RedisUnitIPs = RedisUnitIPs;
		this.RedisUnitPassword = RedisUnitPassword;
		this.ZookeeperAddress = ZookeeperAddress;
		this.groupNode = groupNode;
	}

	public static final Log LOG = LogFactory.getLog(RedisMonitor.class);

	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		RedisMonitor redisMonitor = new RedisMonitor(args[0], args[1], args[2], args[3], args[4]);
		Set<String> RedisClusterIPs = new HashSet<String>();
		for (String RedisIP : redisMonitor.RedisUnitIPs.split("-")) {
			RedisClusterIPs.add(RedisIP);
		}
		redisMonitor.jedisSentinelPool = new JedisSentinelPool(redisMonitor.RedisUnitname, RedisClusterIPs,
				redisMonitor.RedisUnitPassword);
		redisMonitor.valuedMemory = redisMonitor.getValueMemory(redisMonitor.RedisUnitname,
				redisMonitor.jedisSentinelPool);
		String RedisUnitNodeData = redisMonitor.RedisUnitname;
		for (String RedisIP : redisMonitor.RedisUnitIPs.split("-")) {
			RedisUnitNodeData += "-" + RedisIP;
		}
		RedisUnitNodeData += "-" + redisMonitor.RedisUnitPassword;
		RedisUnitNodeData += "-" + redisMonitor.valuedMemory;
		redisMonitor.connectZookeeper(redisMonitor.ZookeeperAddress, "RedisUnit-"+redisMonitor.RedisUnitname, RedisUnitNodeData);

		redisMonitor.Monitor();

	}

	private void Monitor(){
		// TODO Auto-generated method stub
		while (true) {
			boolean RedisAvailable = true;
			Jedis jedis = null;
			try {
				jedis = jedisSentinelPool.getResource();
				Date date = new Date();
				DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				String CurrentTime = format.format(date);
				String setre;
				// 增
				setre = jedis.set("Test-" + CurrentTime, CurrentTime);
				if (!setre.equals("OK")) {
					RedisAvailable = false;
				}
				// 查
				String getre;
				getre = jedis.get("Test-" + CurrentTime);
				if (!getre.equals(CurrentTime)) {
					RedisAvailable = false;
				}
				// 删
				Long delre;
				delre = jedis.del("Test-" + CurrentTime);
				if (delre != 1) {
					RedisAvailable = false;
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				jedisSentinelPool.returnBrokenResource(jedis);
			}
			if(!RedisAvailable)
				break;
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private String getValueMemory(String redisUnitname, JedisSentinelPool jedisSentinelPool) {
		// TODO Auto-generated method stub
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
			return String.valueOf(maxMemory - usedMemory);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			jedisSentinelPool.returnBrokenResource(jedis);
		}
		return null;
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

	public void connectZookeeper(String zookeeperServerHost, String RedisUnitname, String RedisUnitNodeData)
			throws IOException, KeeperException, InterruptedException {
		String RedisUnitPath;
		zooKeeper = new ZooKeeper(zookeeperServerHost, 5000, this);
		// 先判断sgroup节点是否存在
		String groupNodePath = groupNode;
		Stat stat = zooKeeper.exists(groupNodePath, false);
		if (null == stat) {
			zooKeeper.create(groupNodePath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			LOG.info("creat a groupNode:SUCCESSED");

		}
		LOG.info("connectZookeeper:SUCCESSED");
		// 将server的地址数据关联到新创建的子节点上
		RedisUnitPath = zooKeeper.create(groupNodePath + "/" + RedisUnitname, RedisUnitNodeData.getBytes("utf-8"),
				Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		LOG.info("创建了RedisUnit节点：" + RedisUnitPath);
		//System.out.println(zooKeeper.getData(RedisUnitPath, watch, stat));
		LOG.info("connectZookeeper:SUCCESSED ");
	}

	/**
	 * 每隔60秒更新一次redis负载
	 * 
	 *//*
		 * private void downloadBarance() { LOG.info("downloadBarance - start");
		 * new Thread(new Runnable() { public void run() { while (true) { try {
		 * Map<String, Double> newRedisLoadBalance = new
		 * ConcurrentHashMap<String, Double>(); for (String redisname :
		 * redisClusters.keySet()) { JedisSentinelPool jedisSentinelPool =
		 * jedisSentinelPools.get(redisname); Jedis jedis = null;
		 * redis.clients.jedis.Client jedisclient = null; try { jedis =
		 * jedisSentinelPool.getResource(); String info = ""; try { jedisclient
		 * = jedis.getClient(); jedisclient.info(); info =
		 * jedisclient.getBulkReply(); } finally { // 关闭流 jedisclient.close(); }
		 * String[] strs = info.split("\n"); Long usedMemory = 0L; Long
		 * maxMemory = 0L; for (int i = 0; i < strs.length; i++) { String s =
		 * strs[i]; String[] detail = s.split(":"); if
		 * (detail[0].equals("used_memory")) { usedMemory =
		 * getIntFromString(detail[1]); break; } } for (int i = 0; i <
		 * strs.length; i++) { String s = strs[i]; String[] detail =
		 * s.split(":"); if (detail[0].equals("maxmemory")) { maxMemory =
		 * getIntFromString(detail[1]); maxMemory = maxMemory > 0 ? maxMemory :
		 * 4L * 1024 * 1024 * 1024;// 默认4G最大内存 break; } } double lbrate =
		 * (double) usedMemory / maxMemory; newRedisLoadBalance.put(redisname,
		 * lbrate); } catch (Exception e) { e.printStackTrace(); } finally {
		 * jedisSentinelPool.returnBrokenResource(jedis); } } redisLoadBalance =
		 * newRedisLoadBalance; LOG.info("更新了Redis负载列表:" + redisLoadBalance);
		 * Thread.sleep(60000); } catch (Exception e) { e.printStackTrace(); }
		 * 
		 * }
		 * 
		 * }
		 * 
		 * private Long getIntFromString(String input) { // TODO Auto-generated
		 * method stub Long re = 0L; for (int i = 0; i < input.length(); i++) {
		 * char t = input.charAt(i); if (t >= '0' && t <= '9') { re = re * 10 +
		 * t - '0'; } } return re; } }).start(); }
		 */
	@Override
	public void process(WatchedEvent arg0) {
		// TODO Auto-generated method stub

	}
}
