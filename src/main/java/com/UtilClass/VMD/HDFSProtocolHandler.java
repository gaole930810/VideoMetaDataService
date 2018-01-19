package com.UtilClass.VMD;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.UtilClass.Service.ConfUtil;
import com.xuggle.xuggler.io.IURLProtocolHandler;

/**
 * 实现一个操作HDFSDataInputStream的Handler，此处只实现了read方法，因为当前项目不需要write的方法。
 *
 * Modified by yty on 2017/06/17.
 */
public class HDFSProtocolHandler implements IURLProtocolHandler {

    private static final Log LOG = LogFactory.getLog(HDFSProtocolHandler.class);

    private FSDataInputStream fIn;

    private Path path;

    private Configuration conf;

    private FileSystem fs;

    public HDFSProtocolHandler() {
        this.conf = ConfUtil.generate();
    }

    /**
     * 有参构造方法。
     * 参数是hadoop的配置类，用于后续打开hdfs读取流。
     *
     * @param conf hadoop配置类
     */
    public HDFSProtocolHandler(Configuration conf) {
        this.conf = conf;
    }

    public HDFSProtocolHandler(String input) {
        path = new Path(input);
        conf = ConfUtil.generate();
    }

    /* (non-Javadoc)
     * @see com.xuggle.xuggler.io.IURLProtocolHandler#close()
     */
    public int close() {
        try {
            fIn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    /* (non-Javadoc)
     * @see com.xuggle.xuggler.io.IURLProtocolHandler#isStreamed(java.lang.String, int)
     */
    public boolean isStreamed(String arg0, int arg1) {
        return false;
    }

    /*
     * 这里我们实现的版本是只读模式的，由于并没有涉及到写的相关操作，因此flag暂时无用。
     *
     * 能够正常读取数据流则返回0,无效的url返回-1,无效的hdfs FileSystem实例返回-2
     *
     * (non-Javadoc)
     * @see com.xuggle.xuggler.io.IURLProtocolHandler#open(java.lang.String, int)
     */
    public int open(String url, int flags) {
        // 这里的我们只处理hdfs的url。如果不是hdfs的url，直接返回-1，代表读取失败。
        LOG.info("Opening HDFSProtocolHandler with " + url);
        if (url != null && !url.startsWith("hdfs:"))
            return -1;

        if (url != null)
            path = new Path(url);

        if (path == null) return -1;
        try {
            this.fs = FileSystem.get(this.conf);
        } catch (IOException e) {
            LOG.info("HDFS client can't build");
            LOG.info(e.getStackTrace());
            return -2;
        }
        try {
            fIn = this.fs.open(path);
        } catch (IOException e) {
            LOG.info("FSDataInputStream can't open");
            return -2;
        }
        LOG.info("HDFSProtocolHandler opened");

        return 0;
    }

    /* (non-Javadoc)
     * @see com.xuggle.xuggler.io.IURLProtocolHandler#read(byte[], int)
     */
    public int read(byte[] buf, int size) {
        int r = 0;
        try {
            r = fIn.read(buf, 0, size);
            LOG.info("当前文件偏移量:"+fIn.getPos());
            LOG.debug("当前文件偏移量:"+fIn.getPos());
            // 老问题，如果一次FSDataInputStream不能读完所有的字节，说明有可能遇到block边界问题
            // 此时会重新读一次。
            if (r < size) {
                int r2 = fIn.read(buf, r, size - r);
                LOG.info("当前文件偏移量:"+fIn.getPos());
                LOG.debug("当前文件偏移量:"+fIn.getPos());
                if (r2 >= 0)
                    r += r2;
            }

            // IUrlProtocolHandler 要求如果读取至文件末尾需要返回0
            if (r == -1)
                r = 0;

        } catch (IOException e) {
            e.printStackTrace();
            r = -1;
        }
        return r;
    }

    /**
     * HDFSProtocolHandler类的关键方法
     * 能使xuggle能够处理自定义的协议一定要实现seek方法。
     * 因为xuggler封装的底层ffmpeg的相关方法需要依靠seek方法来定位某一个帧
     * 因此实现自定义协议的核心在于，如何实现seek方法。
     * 由于HDFSInputStream已经实现了seek方法，所以相对简单。
     * 但是很多InputStream类不支持seek方法，此时需要自己实现相关方法。
     *
     * @param offset 偏移量
     * @param whence 标志位
     * @return seek后的位置
     */
    public long seek(long offset, int whence) {
        LOG.debug("seeking to " + offset + ", whence = " + whence);
        long pos;
        try {
            FileStatus status = fs.getFileStatus(path);
            long len = status.getLen();
            switch (whence) {
                case SEEK_CUR://从当前位置操作偏移量
                    long old_pos = fIn.getPos();
                    fIn.seek(old_pos + offset);
                    pos = old_pos - fIn.getPos();
                    break;
                case SEEK_END://从文件结尾操作偏移量
                    fIn.seek(len + offset);
                    pos = fIn.getPos() - len;
                    break;
                case SEEK_SIZE://返回文件长度
                    pos = len;
                    break;
                case SEEK_SET://直接设置数据流的位置到offset处
                default:
                    fIn.seek(offset);
                    pos = fIn.getPos();
                    break;
            }
        } catch (IOException e) {
            LOG.error(e.getMessage());
            return -1;
        }
        return pos;
    }

    /* (non-Javadoc)
    此处我们只实现读取数据流的方法
     * @see com.xuggle.xuggler.io.IURLProtocolHandler#write(byte[], int)
     */
    public int write(byte[] buf, int size) {
        return 0;
    }

}
