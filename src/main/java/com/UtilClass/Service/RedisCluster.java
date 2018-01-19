package com.UtilClass.Service;

import java.util.HashSet;
import java.util.Set;

public class RedisCluster {
	public String ClusterName;
	public Set<String> RedisClusterIPs = new HashSet<String>();
	public String ClusterPassport;
	public Long ValuedMemory;
	public RedisCluster(String ClusterName,Set<String> RedisClusterIPs,String ClusterPassport,String ValuedMemory){
		this.ClusterName=ClusterName;
		this.RedisClusterIPs=RedisClusterIPs;
		this.ClusterPassport=ClusterPassport;
		this.ValuedMemory=Long.valueOf(ValuedMemory);
	}
	@Override
	public String toString(){
		String ClusterInfo =this.ClusterName;		
		for (String RedisIP : this.RedisClusterIPs) {
			ClusterInfo += "-" + RedisIP;
		}
		ClusterInfo+="-"+this.ClusterPassport;
		ClusterInfo+="-"+this.ValuedMemory;
		return ClusterInfo;
	}

}
