package com.UtilClass.VMD;

import com.Proto.SecondaryMetaClass;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.compress.Decompressor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by yty on 2016/12/7.
 * 一个基于Xuggler的解码器。
 * 其实核心就是元数据字典。也就是帧号和其对应的帧的起始偏移量。
 */
public class XugglerDecompressor implements Decompressor {
    private static final Log LOG = LogFactory.getLog(XugglerDecompressor.class);

    public XugglerDecompressor(List<SecondaryMetaClass.SecondaryMeta.FrameInfoGroup> fig){
        this.indices = fig;
    }
    /**
     * 视频帧偏移量字典，包含了偏移量和帧号，是由视频元数据生成的。
     */
    private List<SecondaryMetaClass.SecondaryMeta.FrameInfoGroup> indices;

    /**
     * Not needed in this implementation.
     * (non-Javadoc)
     *
     * @see org.apache.hadoop.io.compress.Decompressor#setInput(byte[], int, int)
     */
    public void setInput(byte[] b, int off, int len) {
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.compress.Decompressor#needsInput()
     */
    public boolean needsInput() {
        return false;
    }

    /**
     * 设置视频元数据字典的，即<帧偏移量--帧号>对应关系。
     */
    public void setDictionary(byte[] b, int off, int len) {
        ByteArrayInputStream bis = new ByteArrayInputStream(b, off, len);
        try {
            ObjectInputStream ois = new ObjectInputStream(bis);
            indices = (List<SecondaryMetaClass.SecondaryMeta.FrameInfoGroup>) ois.readObject();
            LOG.debug("De-serialized indices: size = " + indices.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean needsDictionary() {
        return indices == null || indices.isEmpty();
    }

    public boolean finished() {
        return false;
    }

    public int decompress(byte[] b, int off, int len) {
        return 0;
    }

    public void reset() {
    }

    public void end() {
    }

    /**
     * 对于一个给定的原始文件偏移量，找到一个最近的有效的帧起始偏移量
     */
    public long getNearestIndexBefore(long pos) {
        return indices.stream()
                .filter(fig -> fig.getStartIndex() > pos)
                .findFirst()
                .get()
                .getStartIndex();
    }

    /**
     * 通过查找第一帧位置的偏移量来确定视频头(header)的规模
     */
    public long getHeaderSize() {
        if (needsDictionary())
            return 0;
        return indices.stream()
                .findFirst()
                .get()
                .getStartIndex();
    }

    /**
     * 返回给定帧号的帧起始偏移量。
     *
     */
    public long getFrameAt(long key_start) {
        SecondaryMetaClass.SecondaryMeta.FrameInfoGroup fig = SecondaryMetaClass.SecondaryMeta.FrameInfoGroup
                .newBuilder()
                .setStartFrameNo(key_start)
                .setStartIndex(0)
                .build();
        int index = Collections.binarySearch(indices, fig,
                Comparator.comparing(SecondaryMetaClass
                        .SecondaryMeta
                        .FrameInfoGroup::getStartFrameNo));
        return indices.get(index).getStartIndex();
    }

    @Override
    public int getRemaining() {
        return 0;
    }

}
