package com.UtilClass.VMD;

import org.apache.hadoop.conf.Configuration;

import com.UtilClass.Service.ConfUtil;
import com.xuggle.xuggler.io.IURLProtocolHandler;
import com.xuggle.xuggler.io.IURLProtocolHandlerFactory;


public class HDFSProtocolHandlerFactory implements IURLProtocolHandlerFactory {

    private Configuration conf;

    public HDFSProtocolHandlerFactory() {
        this.conf = ConfUtil.generate();
    }


    public HDFSProtocolHandlerFactory(Configuration conf) {
        this.conf = conf;
    }

    public IURLProtocolHandler getHandler(String protocol, String url, int flags) {
        if (protocol.equals("hdfs")) {
            return new HDFSProtocolHandler(conf);
        }
        return null;
    }

}
