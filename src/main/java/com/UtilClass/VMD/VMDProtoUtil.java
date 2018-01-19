package com.UtilClass.VMD;

import com.Proto.SecondaryMetaClass;
import com.UtilClass.Service.ConfUtil;
import com.UtilClass.Service.UploadFile;
import com.xuggle.xuggler.*;
import com.xuggle.xuggler.io.URLProtocolManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by yty on 2016/11/30.
 */
public class VMDProtoUtil {
    public static final Log LOG = LogFactory.getLog(VMDProtoUtil.class);

    /**
     * 根据给定的配置生成SecondaryMeta对象。
     * @param VS 视频摘要
     * @param BI hdfs的block规模
     * @param CI 视频容器信息
     * @param TS 视频总时长
     * @param EI 视频编码信息
     * @param FN 视频总帧数
     * @param FMI 元数据字典<视频keyPacket偏移量 - 帧号>
     * @return
     */
    public static SecondaryMetaClass.SecondaryMeta genProto(String VS, Long BI, String CI, Long TS, String EI, Long FN, Map<Long, Long> FMI) {

        List<SecondaryMetaClass.SecondaryMeta.FrameInfoGroup> fmi = FMI.entrySet().stream()
                .map(entry -> SecondaryMetaClass.SecondaryMeta.FrameInfoGroup
                        .newBuilder()
                        .setStartIndex(entry.getValue())
                        .setStartFrameNo(entry.getKey())
                        .build())
                .collect(ArrayList::new, (L, F) -> L.add(F), (l1, l2) -> l1.addAll(l2));
        SecondaryMetaClass.SecondaryMeta SM = SecondaryMetaClass.SecondaryMeta
                .newBuilder()
                .setBlockInfo(BI)
                .setContainerInfo(CI)
                .setVideoSummary(VS)
                .setTimestamp(TS)
                .setEncodeInfo(EI)
                .setFrameNumber(FN)
                .addAllFrameMetaInfo(fmi)
                .build();
        return SM;
    }

    /**
     * 打印元数据信息
     * @param sm 元数据对象
     */
    public static void print(SecondaryMetaClass.SecondaryMeta sm) {
        LOG.info("------------------------------------");
        LOG.info(sm.getAllFields());
        LOG.info("------------------------------------");
    }

    /**
     * 读取视频文件对应元数据信息。
     * @param path 原始视频文件地址
     */
    public static void readMeta(Path path) {
        String VideoSummary = UploadFile.generateSummary(path);
        String MetaPath = ConfUtil.defaultFS + "/meta/" + VideoSummary;
        try {
            FileSystem hdfs = FileSystem.get(ConfUtil.generate());
            SecondaryMetaClass.SecondaryMeta SM = SecondaryMetaClass.SecondaryMeta.parseFrom(hdfs.open(new Path(MetaPath)));
            print(SM);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 得到对应视频的元数据对象
     * @param path 视频文件url
     * @return
     */
    public static SecondaryMetaClass.SecondaryMeta getMeta(Path path) {
        String VideoSummary = UploadFile.generateSummary(path);
        String MetaPath = ConfUtil.defaultFS + "/yty/meta/" + VideoSummary;
        SecondaryMetaClass.SecondaryMeta SM = null;
        try {
            FileSystem hdfs = FileSystem.get(ConfUtil.generate());
            SM = SecondaryMetaClass.SecondaryMeta.parseFrom(hdfs.open(new Path(MetaPath)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return SM;
    }

    /**
     * 核心方法，生成元数据
     * 1.找到视频流对应的流编号
     * 2.记录视频文件头的规模和起始帧偏移量
     * 3.每当有key packet时，记录进入元数据字典中（LinkedHashMap）。
     * 4.将视频元数据所需的各种信息加上字典组装成为视频二级元数据。
     * @param path 视频文件url
     * @return 对应的二级元数据对象。
     */
    public static SecondaryMetaClass.SecondaryMeta writeMeta(Path path) {
        URLProtocolManager mgr = URLProtocolManager.getManager();
        if (path.toString().startsWith("hdfs:"))
            mgr.registerFactory("hdfs", new HDFSProtocolHandlerFactory());
        IContainer container = IContainer.make();
        container.open(path.toString(), IContainer.Type.READ, null);
        boolean header = false;
        for (int i = 0; i < container.getNumStreams(); i++) {
            IStream stream = container.getStream(i);
            IStreamCoder coder = stream.getStreamCoder();
            if (coder.getCodecType() != ICodec.Type.CODEC_TYPE_VIDEO)
                continue;
            IPacket packet = IPacket.make();
            Long frameNo = 0L;
            Map<Long, Long> map = new LinkedHashMap<>();
            while (container.readNextPacket(packet) >= 0) {
                if (packet.getStreamIndex() == i) {
                    if (!header) {
                        map.put(frameNo, packet.getPosition());
                        header = true;
                    }
                    frameNo++;
                    if (packet.isKeyPacket()) {
                        map.put(frameNo, packet.getPosition());
                    }
                }
            }
            LOG.debug(map.size());
            SecondaryMetaClass.SecondaryMeta SM = genProto(UploadFile.generateSummary(path)
                    , Long.parseLong(ConfUtil.generate().get("dfs.blocksize"))
                    , container.getContainerFormat().getInputFormatShortName()
                    , container.getDuration()
                    , coder.getCodec().getName()
                    , frameNo
                    , map);
            try {
                FileSystem hdfs = FileSystem.get(ConfUtil.generate());
                FSDataOutputStream fsout = hdfs.create(new Path("hdfs://vm1:9000/yty/meta/" + SM.getVideoSummary()), true);
                SM.writeTo(fsout);
                fsout.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            container.close();
            return SM;
        }
        container.close();
        return null;
    }

    /**
     * 目前已废弃
     * 原本是想利用视频文件中内嵌的IndexEntry来快速定位，但是IndexEntry的数量太少。
     * 定位效果不理想，所以考虑废弃，并使用key packet来作为分布式读取的入口。
     * @param path
     * @param isLocal
     */
    @Deprecated
    public static void gererate(Path path, boolean isLocal) {
        IContainer container;
        int numStreams;
        if (isLocal) {
            try {
                File file = new File(path.toString());
                container = IContainer.make();

                // open the video file
                int r = container.open(file.toString(), IContainer.Type.READ, null);

                // if video file can't be read , throw exception
                if (r < 0) {
                    IError error = IError.make(r);
                    throw new IllegalArgumentException("could not open container from " + file + ", error = " + error.getDescription());
                }

                numStreams = 1;

                // if the video has no stream , throw exception
                if (numStreams <= 0)
                    throw new IllegalArgumentException("No streams found in container.");
                // all_indices include all the byte pos of the video stream
                TreeMap<Long, Long> all_indices = new TreeMap<>();
                // get lists of index entries of the streams
                List<IIndexEntry> index_list[] = new List[numStreams];
                boolean[] get_keyframes = new boolean[numStreams];
                int list_done = 0;
                int i = 0;
                for (int j = 0; j < container.getNumStreams(); j++) {
                    IStream stream = container.getStream(j);
                    IStreamCoder coder = stream.getStreamCoder();
                    LOG.debug("stream " + j + ": " + coder.getCodecType().name());
                    if (coder.getCodecType() != ICodec.Type.CODEC_TYPE_VIDEO)
                        continue;
                    index_list[i] = stream.getIndexEntries();
                    if (index_list[i].isEmpty()) list_done++;
                    LOG.debug("index-list-size = " + index_list[i].size());
                    // if the stream is a video stream get_keyframes flag is set
                    get_keyframes[i] = coder.getCodecType() == ICodec.Type.CODEC_TYPE_VIDEO;
                    LOG.debug("get_keyframes : " + get_keyframes[i]);
                    i++;
                    // video stream was found. if "onlyVideo" then don't look at any other streams
                    break;
                }
                int counter[] = new int[numStreams];
                for (int j = 0; j < numStreams; j++) counter[j] = 0;

                // iterate over all streams and put the smallest (key-)index-entry of the current index-entries of the streams
                // into all_indices-list. stops when all list have been parsed.
                while (list_done < numStreams) {
                    long frame_no = -1;
                    long min_pos = -1;
                    int list_nr = -1;
                    for (int j = 0; j < numStreams; j++) {
                        if (index_list[j].size() > counter[j]) {

                            // if get_keyframes is true for this stream move up to next keyframe
                            // but at the beginning put the smallest index entry at first position (see first boolean statement)
                            while (counter[j] != 0 &&
                                    get_keyframes[j] &&
                                    counter[j] < index_list[j].size() &&
                                    !index_list[j].get(counter[j]).isKeyFrame()) counter[j]++;
                            if (counter[j] >= index_list[j].size()) {
                                list_done++;
                                continue;
                            }

                            // get index with smallest current_index-position
                            IIndexEntry current_index = index_list[j].get(counter[j]);
                            if (min_pos == -1 || current_index.getPosition() < min_pos) {
                                min_pos = current_index.getPosition();
                                frame_no = current_index.getTimeStamp();
                                list_nr = j;
                            }
                        }
                    }

                    if (list_nr == -1) break;
                    counter[list_nr]++;
                    if (index_list[list_nr].size() <= counter[list_nr]) list_done++;

                    all_indices.put(min_pos, frame_no);
                }
                // end-of-file is also treated as index entry
                // frame_no = -1, because it does not matter

                all_indices.put(container.getFileSize(), -1L);
                LOG.debug("all_indices.size = " + all_indices.size());
                System.out.println(all_indices);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}
