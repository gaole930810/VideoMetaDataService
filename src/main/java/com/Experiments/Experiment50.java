package com.Experiments;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;


import com.Proto.SecondaryMetaClass.SecondaryMeta;
import com.Proto.SecondaryMetaClass.SecondaryMeta.FrameInfoGroup;
import com.UtilClass.Service.RedisCluster;
import com.UtilClass.Service.RedisUtil;
import com.UtilClass.Service.SerializationUtil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

public class Experiment50 {
    private Jedis jedis;
    public static void main(String[] args) throws IOException{
    	int vmdsize=0;
    	FileWriter fw6;
        //fw = new FileWriter("F:\\result\\"+"Experiment11-"+new Path(Path).getName() + ".txt",true);
        fw6 = new FileWriter("E:\\学习\\毕业论文\\实验数据\\SetDetail1111.txt",true);
        RedisCluster[] redisClusters=new RedisCluster[3];
        redisClusters[0]=new RedisCluster("mymaster123", new HashSet<String>(), "b8311",String.valueOf(4*1024*1024*1024));
    	redisClusters[0].RedisClusterIPs.add("172.16.10.101:26379");
    	redisClusters[0].RedisClusterIPs.add("172.16.10.102:26379");
    	redisClusters[0].RedisClusterIPs.add("172.16.10.103:26379");
        	redisClusters[1]=new RedisCluster("mymaster456", new HashSet<String>(), "b8311",String.valueOf(4*1024*1024*1024));
        	redisClusters[1].RedisClusterIPs.add("172.16.10.104:26379");
        	redisClusters[1].RedisClusterIPs.add("172.16.10.105:26379");
        	redisClusters[1].RedisClusterIPs.add("172.16.10.106:26379");
        	redisClusters[2]=new RedisCluster("mymaster789", new HashSet<String>(), "b8311",String.valueOf(4*1024*1024*1024));
        	redisClusters[2].RedisClusterIPs.add("172.16.10.107:26379");
        	redisClusters[2].RedisClusterIPs.add("172.16.10.108:26379");
        	redisClusters[2].RedisClusterIPs.add("172.16.10.109:26379");
        	
        	for (RedisCluster redisCluster : redisClusters) {
        		JedisSentinelPool jedisSentinelPool = new JedisSentinelPool(redisCluster.ClusterName,
    					redisCluster.RedisClusterIPs, redisCluster.ClusterPassport);
    			Jedis jedis = null;
    			try {
    				jedis = jedisSentinelPool.getResource();
    				// jedis.set("key", "value");
    				Set s = jedis.keys("*");
    				Iterator it = s.iterator();

    				while (it.hasNext()) {
    					String key = (String) it.next();
    					// 判断ip地址是否与正则表达式匹配
    					//if (key.startsWith("new3000")||key.startsWith("new2000")||key.startsWith("new1500")||key.startsWith("new1000")||key.startsWith("hdfs://vm1:9000")){
    					//if (key.startsWith("new2000")||key.startsWith("new1500")||key.startsWith("new1000")||key.startsWith("hdfs://vm1:9000")){
    					//if (key.startsWith("new1500")||key.startsWith("new1000")||key.startsWith("hdfs://vm1:9000")){
    					//if (key.startsWith("new800")||key.startsWith("new600")||key.startsWith("new400")||key.startsWith("new200")||key.startsWith("hdfs://vm1:9000")) {
    					if (key.startsWith("new200")){
    					//if (key.startsWith("hdfs://vm1:9000")) {
    						//svmdRouter.put(key, redisCluster.ClusterName);
    						byte[] urlbyte=jedis.get(key.getBytes());
    	    				if(urlbyte!=null){
    	    					SecondaryMeta temp=(SecondaryMeta)SerializationUtil.deserialize(urlbyte);
    	    					int size=0;
    	    					
    	    	                
    	    	                String a=temp.getVideoSummary();
    	    	                size+=a.getBytes().length; 
    	    					long b=temp.getBlockInfo();
    	    					size+=8;
    	    					String c=temp.getContainerInfo();
    	    					size+=c.getBytes().length;
    	    					long d=temp.getTimestamp();
    	    					size+=8;
    	    					String e=temp.getEncodeInfo();
    	    					size+=e.getBytes().length;
    	    					long f=temp.getFrameNumber();
    	    					size+=8;
    	    					List<FrameInfoGroup> g=temp.getFrameMetaInfoList();
    	    					size+=g.size()*16;	
    	    					vmdsize+=size;
    	    					fw6.write(jedisSentinelPool.getCurrentHostMaster()+" "+key+" "+g.size()+" "+size+"\r\n");
        						fw6.flush();
    	    				}
    	    				System.out.println(key);
    						jedis.del(key.getBytes());
    					}
    				}
    			} catch (Exception e) {
    				e.printStackTrace();
    			} finally {
    				jedisSentinelPool.returnBrokenResource(jedis);
    			}
    		}
        fw6.write(String.valueOf(vmdsize)+"\r\n");
    	fw6.close();
    } 
}