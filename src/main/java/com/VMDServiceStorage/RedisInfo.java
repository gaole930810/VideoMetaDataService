package com.VMDServiceStorage;

public class RedisInfo {
	//Redis的访问路径
    private String path;
    //Redis名称
    private String name;
    //服务器负载量
    private String loadBalance;

    public String getPath() {
        return path;
    }
    public void setPath(String path) {
        this.path = path;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getLoadBalance() {
        return loadBalance;
    }
    public void setLoadBalance(String loadBalance) {
        this.loadBalance = loadBalance;
    }
    @Override
    public String toString() {
        return " [服务器节点路径=" + path + ", 服务器名称=" + name + ", 服务器可用内存=" + loadBalance + "]";
    }
}
