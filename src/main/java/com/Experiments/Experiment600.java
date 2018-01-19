package com.Experiments;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;


import com.Proto.SecondaryMetaClass.SecondaryMeta;
import com.Proto.SecondaryMetaClass.SecondaryMeta.FrameInfoGroup;
import com.UtilClass.Service.RedisCluster;
import com.UtilClass.Service.SerializationUtil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

public class Experiment600 {
	public static String[] basic=new String[]{
			"hdfs://vm1:9000/yty/video/780.mp4", 
			"hdfs://vm1:9000/yty/video/781.mp4", 
			"hdfs://vm1:9000/yty/video/782.mp4",
			"hdfs://vm1:9000/yty/video/783.mp4",
			"hdfs://vm1:9000/yty/video/784.mp4",
			"hdfs://vm1:9000/yty/video/786.mp4",		
				"hdfs://vm1:9000/yty/video/movie0.mp4",
				"hdfs://vm1:9000/yty/video/movie1.mp4",
				"hdfs://vm1:9000/yty/video/movie2.mp4",
				"hdfs://vm1:9000/yty/video/movie3.mp4",
				"hdfs://vm1:9000/yty/video/movie4.mp4",
				"hdfs://vm1:9000/yty/video/movie00.mp4",				
				"hdfs://vm1:9000/yty/video/movie11.mp4",
				"hdfs://vm1:9000/yty/video/movie22.mp4",					
				"hdfs://vm1:9000/yty/video/movie33.mp4",
				"hdfs://vm1:9000/yty/video/movie44.mp4",
			"hdfs://vm1:9000/yty/video/m5.mp4"
	};
	public static Map<String ,Double> dsj=new HashMap<String,Double>();

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		
		
		dsj.put(basic[0], (double)(196.70/1024));
		dsj.put(basic[1], (double)(192.03/1024));
		dsj.put(basic[2], (double)(419.66/1024));
		dsj.put(basic[3], (double)(196.35/1024));
		dsj.put(basic[4], (double)(424.64/1024));
		dsj.put(basic[5], (double)(424.58/1024));
		
		dsj.put(basic[6], (double)(4.23));
		dsj.put(basic[7], (double)(2.37));
		dsj.put(basic[8], (double)(2.01));
		dsj.put(basic[9], (double)(2.54));
		dsj.put(basic[10], (double)(3.02));
		
		dsj.put(basic[11], (double)(2.12));
		dsj.put(basic[12], (double)(1.26));
		dsj.put(basic[13], (double)(1.02));
		dsj.put(basic[14], (double)(1.64));
		dsj.put(basic[15], (double)(1.67));
		
		dsj.put(basic[16], (double)(2.37));
		
		
		Map<String ,String> hdy=getnew(800);
		
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
        	Map<String,RedisCluster> sr=new HashMap<String,RedisCluster>();
        	sr.put("hdfs://vm1:9000/yty/video/780.mp4", redisClusters[1]);//104      	
        	sr.put("hdfs://vm1:9000/yty/video/781.mp4", redisClusters[1]); //106     	
        	sr.put("hdfs://vm1:9000/yty/video/782.mp4", redisClusters[0]);//102       	
        	sr.put("hdfs://vm1:9000/yty/video/783.mp4", redisClusters[0]);//102     	
        	sr.put("hdfs://vm1:9000/yty/video/784.mp4", redisClusters[2]);//108
        	sr.put("hdfs://vm1:9000/yty/video/786.mp4", redisClusters[2]);//107
        	sr.put("hdfs://vm1:9000/yty/video/movie0.mp4", redisClusters[2]);//109
        	sr.put("hdfs://vm1:9000/yty/video/movie1.mp4", redisClusters[2]);//108        	
        	sr.put("hdfs://vm1:9000/yty/video/movie2.mp4", redisClusters[0]);//103
        	sr.put("hdfs://vm1:9000/yty/video/movie3.mp4", redisClusters[1]);//105
        	sr.put("hdfs://vm1:9000/yty/video/movie4.mp4", redisClusters[1]);//104
        	sr.put("hdfs://vm1:9000/yty/video/movie00.mp4", redisClusters[1]);//105
        	sr.put("hdfs://vm1:9000/yty/video/movie11.mp4", redisClusters[0]);//103
        	sr.put("hdfs://vm1:9000/yty/video/movie22.mp4", redisClusters[0]);//101
        	sr.put("hdfs://vm1:9000/yty/video/movie33.mp4", redisClusters[2]);//108
        	sr.put("hdfs://vm1:9000/yty/video/movie44.mp4", redisClusters[2]);//107
        	sr.put("hdfs://vm1:9000/yty/video/m5.mp4", redisClusters[0]);//102

        	for(String key:hdy.keySet()){
        		
        		String oldname=key.split("-")[1];
        		RedisCluster redisCluster=sr.get(oldname);
        		JedisSentinelPool jedisSentinelPool = new JedisSentinelPool(redisCluster.ClusterName,
    					redisCluster.RedisClusterIPs, redisCluster.ClusterPassport);
    			Jedis jedis = null;
    			try {
    				jedis = jedisSentinelPool.getResource();
    				byte[] urlbyte=jedis.get(oldname.getBytes());
    				if(urlbyte!=null){
    					jedis.set(hdy.get(key).getBytes(),urlbyte);
    				}   
    			} catch (Exception e) {
    				e.printStackTrace();
    			} finally {
    				jedisSentinelPool.returnBrokenResource(jedis);
    			}
    			jedisSentinelPool.close();
    		}
        	
		
		
	}
	public static Map<String ,String> getnew(int size) throws IOException{
		int n=0;
		int count=614;
		int mm=new Random().nextInt()%20;
		String newnamehead="new"+String.valueOf(size)+"-";
		Map<String ,String> hdy=new HashMap<String,String>();
		FileWriter fwr;
        //fw = new FileWriter("F:\\result\\"+"Experiment11-"+new Path(Path).getName() + ".txt",true);
        fwr = new FileWriter("E:\\学习\\毕业论文\\实验数据\\SetBuild"+size+".txt",true);
        while(count<size+mm){
        	String oldname=basic[Math.abs(new Random().nextInt()%basic.length)];
    		String newname=newnamehead+n+oldname;
    		
    		hdy.put(String.valueOf(n)+"-"+oldname, newname);
    		n++;
    		fwr.write(oldname+" "+dsj.get(oldname)+" "+newname+"\r\n");
    		fwr.flush();
    		count+=dsj.get(oldname);
        }
        fwr.write(String.valueOf(count));
        fwr.close();
		return hdy;
	}

}
