package com.UtilClass.Service;

import java.util.Map;

import com.Proto.SecondaryMetaClass;
import com.UtilClass.VMD.VMDProtoUtil;

public class Results {
	public Object results;
	public void print(){
		System.out.println((String)results);
	}
	public Object getResults() {
		return results;
	}
	public void setResults(String results) {
		this.results = results;
	}
	public void printVMDMap() {
		// TODO Auto-generated method stub
		Map<String,Map<String,Integer>> map= (Map<String,Map<String,Integer>>)results;
		String MapResults="";
		for(String redisname:map.keySet()){
			MapResults+=redisname+"\r\n";
			Map<String,Integer> map2=map.get(redisname);
			for(String url:map2.keySet()){
				MapResults+=url+":共"+String.valueOf(map2.get(url))+"个I帧\r\n";
			}
		}
		System.out.println(MapResults);
	}
	public void printNodeMap() {
		// TODO Auto-generated method stub
		Map<String,String> map= (Map<String,String>)results;
		String MapResults="";
		for(String nodename:map.keySet()){
			MapResults+=nodename+" = "+map.get(nodename)+"\r\n";
		}
		System.out.println(MapResults);
	}
	public void printVMDInfo() {
		// TODO Auto-generated method stub
		SecondaryMetaClass.SecondaryMeta vmd=(SecondaryMetaClass.SecondaryMeta)results;
		System.out.println(vmd.getAllFields());
	}
	public void printFRAMEIndex() {
		// TODO Auto-generated method stub
		Long[] frameIndex = (Long[])results;
		System.out.println("I帧时间偏移量:"+frameIndex[0]);
		System.out.println("I帧帧号:"+frameIndex[1]);
	}
	
}
