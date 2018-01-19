package com.UtilClass.VMD;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;

/**
 * 这个类是用Xuggler封装HDFS提供的文件读取流FSDataInputStream的。
 * Created by yty on 2016/12/4.
 */

public class XugglerFSDataInputStream extends InputStream implements Closeable {

    private static final Log LOG = LogFactory
            .getLog(XugglerFSDataInputStream.class);

    /**
     * BUFFER_SIZE 是一次读取的Buffer规模，必须能够被HDFS的block size整除，这里设定为4MB.
     */
    private static int BUFFER_SIZE = 4 * 1024 * 1024;// VideoBatch.BUFFER_SIZE;

    /**
     * HDFS的文件读取流.
     */
    FSDataInputStream fIn;

    /**
     * 从FSDataInputStream开始读取的位置（字节）
     */
    long start;

    /**
     * 从FSDataInputStream结束读取的位置（字节）
     */
    long end;

    /**
     * 缓存的视频头信息。
     */
    private byte[] header = null;

    /**
     * XugglerFSDataInputStream的位置（**注意！这里不是FSDataInputStream的位置**）
     */
    private long pos;

    public XugglerFSDataInputStream(InputStream in, long start, long end)
            throws IOException {
        this.fIn = (FSDataInputStream) in;
        this.fIn.seek(start);

        this.start = start;
        this.end = end;

    }

    public void setHeader(byte[] b) {
        LOG.debug("header set, length: " + b.length);
        this.header = b;
    }

    public boolean isHeaderSet() {
        return this.header != null;
    }

    /**
     * 首先，要读取原始视频的视频头信息，并缓存到header里面。
     * 然后，定位到XugglerFSDataInputStream规定的起始位置，开始读取数据。
     * 最后，如果已经到达规定的结束位置，那么就返回-1.
     */
    public int read(byte[] bytes) throws IOException {

        if (!isHeaderSet())
            throw new IOException("Header not set");

        if (isEnd())
            return -1;

        int b = 0;

        LOG.debug("reading " + bytes.length + ", pos = " + pos);

        if (pos < header.length) {
            // copy header
            for (b = (int) pos; b < bytes.length + pos && b < header.length; b++) {
                bytes[b] = (byte) readHeader(b);
            }
            pos = b;
        }

        if (pos >= header.length) {
            int toRead = bytes.length - b;
            long restToEnd = getRestToEnd();
            if (restToEnd < toRead)
                toRead = (int) restToEnd;

            int r = fIn.read(bytes, b, toRead);
            if (r > 0) {
                // 如果出现没有一次读完所有应读的字节，说明可能遇到了block边界，那么就在重新都一次剩下的字节。
                if (r < toRead) {
                    int r2 = fIn.read(bytes, b + r, toRead - r);
                    if (r2 >= 0)
                        r += r2;
                }
                pos += r;
                if (b == -1)
                    b = 0;
                b += r;
            } else
                return -1;
        }
        return b;
    }

    /**
     * 在规定的结束位置前，还剩下多少字节需要读取
     */
    private long getRestToEnd() {
        return end - start - pos + header.length;
    }

    /**
     * 重载read方法。
     * 如果当前pos属于header的范围内，那直接从header里面读取1Byte即可。
     * 如果是其他情况，就正常读取1byte即可，如果返回值小于0，说明原始FIn已经没有可读的字节了。
     */
    @Override
    public int read() throws IOException {

        if (!isHeaderSet())
            throw new IOException("Header not set");

        if (pos < header.length) {
            return readHeader((int) pos++);
        }
        if (isEnd()) {
            LOG.debug("end reached, pos = " + (pos + start - header.length));
            return -1;
        }

        int b = fIn.read();
        if (b >= 0)
            pos++;
        return b;
    }

    /**
     * 判断这段流是否已经到了终止位置（end position）
     */
    private boolean isEnd() {
        return pos + start - header.length >= end;
    }

    /**
     * 从header里面读取 1 Byte
     */
    private int readHeader(int l) {
        return (header[l] & 0xFF);
    }

    public boolean markSupported() {
        return false;
    }

    public void close() {
    }

    public long getPos() {
        return pos;
    }

}

