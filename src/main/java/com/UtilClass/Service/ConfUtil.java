package com.UtilClass.Service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * 一个静态工厂类。提供两个直接生成hadoop配置类对象的静态方法。
 * Created by yty on 2016/11/29.
 */
public class ConfUtil {
    private final static Log LOG = LogFactory.getLog(ConfUtil.class);
    public final static String defaultFS="hdfs://vm1:9000";
    public final static String defaultHostname="vm1";
    public static Configuration generate() {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set("fs.defaultFS", defaultFS);
        conf.set("yarn.resourcemanager.hostname", defaultHostname);
        conf.set("yarn.nodemanager.aux-services", "shuffle");
        conf.set("io.file.buffer.size", "131072");
        LOG.debug("Configuration initial success!!");
        return conf;
    }

    public static Configuration generate(String namenodeHost, String port, String resourcemanagerHost) {
        Configuration conf = new Configuration();
        String defaultFS = new StringBuilder().append("hdfs://").append(namenodeHost).append(":")
                .append(port).toString();
        conf.set("fs.defaultFS", defaultFS);
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set("yarn.resourcemanager.hostname", resourcemanagerHost);
        conf.set("yarn.nodemanager.aux-services", "shuffle");
        conf.set("io.file.buffer.size", "131072");
        LOG.debug("Configuration initial success!!");
        return conf;
    }
}
