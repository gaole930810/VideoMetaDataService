package com.UtilClass.VMD;


import com.Proto.SecondaryMetaClass.SecondaryMeta.*;
import com.UtilClass.Service.ConfUtil;
import com.UtilClass.Service.SummaryUtil;
import com.Proto.SecondaryMetaClass.SecondaryMeta;
import com.xuggle.xuggler.*;
import com.xuggle.xuggler.io.URLProtocolManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.*;

/**
 * 这个类用来提供Video Meta Data的相关操作。
 * public方法有两种，generate和read，分别用来生成和读取VMD文件
 * private方法有两个，分别时打印VMD和将信息包装成SecondaryMeta类
 * Modified by yty on 2017/06/19.
 */
public class VideoMetaData {
    public static final Log LOG = LogFactory.getLog(VideoMetaData.class);

    /**
     * 根据给定的配置包装生成SecondaryMeta对象。
     *
     * @param VS  视频摘要
     * @param BI  hdfs的block规模
     * @param CI  视频容器信息
     * @param TS  视频总时长
     * @param EI  视频编码信息
     * @param FN  视频总帧数
     * @param FMI 元数据字典<视频keyPacket偏移量 - 帧号>
     * @return SecondaryMeta 的实例
     */
    private static SecondaryMeta genProto(String VS, Long BI, String CI, Long TS, String EI, Long FN, Map<Long, Long> FMI) {

        List<FrameInfoGroup> fmi = FMI.entrySet().stream()
                .map(entry -> FrameInfoGroup
                        .newBuilder()
                        .setStartIndex(entry.getValue())
                        .setStartFrameNo(entry.getKey())
                        .build())
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        return SecondaryMeta
                .newBuilder()
                .setBlockInfo(BI)
                .setContainerInfo(CI)
                .setVideoSummary(VS)
                .setTimestamp(TS)
                .setEncodeInfo(EI)
                .setFrameNumber(FN)
                .addAllFrameMetaInfo(fmi)
                .build();
    }

    /**
     * 打印元数据信息
     *
     * @param sm 元数据对象
     */
    public static void print(SecondaryMeta sm) {
        LOG.info("------------------------------------");
        LOG.info(sm.getAllFields());
        LOG.info("------------------------------------");
    }
    public static Long getSize(SecondaryMeta sm){
    	Long size=0L;
    	 
        String a=sm.getVideoSummary();
        size+=a.getBytes().length; 
		long b=sm.getBlockInfo();
		size+=8;
		String c=sm.getContainerInfo();
		size+=c.getBytes().length;
		long d=sm.getTimestamp();
		size+=8;
		String e=sm.getEncodeInfo();
		size+=e.getBytes().length;
		long f=sm.getFrameNumber();
		size+=8;
		List<FrameInfoGroup> g=sm.getFrameMetaInfoList();
		size+=g.size()*16;
		return size;
    }

    /**
     * 此方法是用来获取一个视频元数据的实例。
     *
     * @param path    视频文件路径
     * @param MetaDir 视频元数据文件夹路径(like hdfs://vm1:9000/yty/meta)
     * @param conf    HDFS配置类实例
     * @return 视频元数据实例
     */
    public static SecondaryMeta read(String path, String MetaDir, Configuration conf) {
        SecondaryMeta SM = null;
        try {
            FileSystem hdfs = FileSystem.get(conf);
            String VideoSummary = SummaryUtil.generate(path, "hdfs", hdfs);
            String metaPath = MetaDir + VideoSummary;
            SM = SecondaryMeta.parseFrom(hdfs.open(new Path(metaPath)));
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return SM;
    }

    /**
     * 此方法是用来获取一个视频元数据的实例。使用默认的配置类对象和默认的视频元数据文件夹路径
     *
     * @param path 视频文件路径
     * @return 视频元数据实例
     */
    public static SecondaryMeta read(String path) {
        return read(path, ConfUtil.generate().get("fs.defaultFS") + "/yty/meta/", ConfUtil.generate());
    }

    /**
     * 这个方法是基于Key Packet生成视频元数据
     * <p>
     * 基本流程是：1.读取一个Packet -> 2.如果是keyPacket，记录<帧号--时间偏移量>，否则，跳转到1.
     * 直到所有的Packet都遍历完成。
     *
     * @param Path 视频文件的路径，如果是HDFS的文件，采用”hdfs：//“开头
     * @return 视频元数据对象（SecondaryMeta）
     * @throws IOException 如果无法创建hdfs FileSystem实例，则会抛出IOException
     */
    public static SecondaryMeta generateViaKeyPacket(String Path, Configuration Conf) throws IOException {
        //创建一个Container来打开视频，如果path是hdfs的路径，则使用HDFSProtocolHandler
        URLProtocolManager mgr = URLProtocolManager.getManager();
        if (Path.startsWith("hdfs://"))
            mgr.registerFactory("hdfs", new HDFSProtocolHandlerFactory(Conf));
        IContainer container = IContainer.make();
        int r = container.open(Path, IContainer.Type.READ, null);
        if (r < 0) throw new IllegalArgumentException("Can not open the video container of path {" + Path + "}");
        LOG.info("Xuggle video container opens");

        //获取视频流
        IStreamCoder coder = null;
        int VideoStreamIndex = -1;
        for (int i = 0; i < container.getNumStreams(); i++) {
            if (container.getStream(i).getStreamCoder().getCodecType() == ICodec.Type.CODEC_TYPE_VIDEO) {
                VideoStreamIndex = i;
                coder = container.getStream(i).getStreamCoder();
                break;
            }
        }
        if (VideoStreamIndex == -1) throw new IllegalArgumentException("this container doesn't hava a video stream.");
        LOG.info("the video stream id is " + VideoStreamIndex);

        //解析Container中所有的视频流的Key Packet,将相关信息加入I帧字典中
        IPacket packet = IPacket.make();
        Long frameNo = 0L;
        Map<Long, Long> IFrameDict = new LinkedHashMap<>();
        while (container.readNextPacket(packet) >= 0) {
            if (packet.getStreamIndex() == VideoStreamIndex) {
                if (IFrameDict.size() == 0 || packet.isKeyPacket())
                    IFrameDict.put(frameNo, packet.getTimeStamp());
                frameNo++;
            }
        }
        LOG.info("The I frame dictionary contains " + IFrameDict.size() + " I frames");

        //将相关信息和I帧字典包装成SecondaryMeta对象，并返回
        FileSystem hdfs;
        String Summary;
        if (Path.startsWith("hdfs")) {
            hdfs = FileSystem.get(Conf);
            Summary = SummaryUtil.generate(Path, "hdfs", hdfs);
        } else {
            Summary = SummaryUtil.generate(Path, "file", null);
        }
        SecondaryMeta SM = genProto(Path          //用path代替摘要
                , Long.parseLong(Conf.get("dfs.blocksize"))
                , container.getContainerFormat().getInputFormatShortName()
                , container.getDuration()
                , coder.getCodec().getName()
                , frameNo
                , IFrameDict);
        container.close();
        return SM;
    }

    /**
     * 这个方法是根据IndexEntry生成视频元数据中的I帧字典，一方面不是所有视频容器都支持IIndexEntry来存储I帧信息，另一方面，部分
     * 视频容器格式的IndexEntry数量较少。但是
     * 但是
     * 但是
     * 推荐使用这个方法，因为IndexEntry记录在文件头，感觉更可靠？？
     * 经过验证，IndexEntry标记的KeyFrame的时间偏移量对应的packet都是KeyPacket，但是不知道反之是不是（对于测试用MOV文件，好像反之亦然）
     * <p>
     * ***************注意**************
     * 此处的I帧字典内的帧号部分是以时间为基础，单位为（1/（1000×帧率））（好像是这样）
     * ×××××××××××××××××××××××××××××××××
     * <p>
     * ***************流程****************
     * 1.从文件头获取所有的IIndexEntry，我们将所有KeyFrame对应的IndexEntry的时间偏移量放入一个Set中
     * 2.遍历所有Packet，记录帧号信息，将所有在IndexEntrySet中包含的packet的时间偏移量记录进I帧字典
     * 3.包装成SecondaryMeta对象
     * <p>
     * 重点，此处的I帧字典的每一个表项是<帧号--时间偏移量！！>
     * ***********************************
     *
     * @param Path 视频文件的路径
     * @return 视频元数据实例
     * @throws IOException 如果无法创建hdfs FileSystem实例，则会抛出IOException
     */
    public static SecondaryMeta generateViaIndexEntry(String Path, Configuration Conf) throws IOException {
        //创建一个Container来打开视频，如果path是hdfs的路径，则使用HDFSProtocolHandler
        URLProtocolManager mgr = URLProtocolManager.getManager();
        if (Path.startsWith("hdfs"))
            mgr.registerFactory("hdfs", new HDFSProtocolHandlerFactory(Conf));
        IContainer container = IContainer.make();
        int r = container.open(Path, IContainer.Type.READ, null);
        if (r < 0){
        	LOG.error("Can not open the video container of path {" + Path + "}");
        	System.out.println("Can not open the video container of path {" + Path + "}");
        	return null;
        } 
        LOG.info("Xuggle video container opens");

        //获取IIndexEntry列表。通常我们认为一个视频只有一个视频流，所以只要检测到一条流的类型是视频，就不再进行检测了
        List<IIndexEntry> IndexEntryList = null;
        int VideoStreamIndex = 0;
        IStreamCoder coder = null;
        for (int i = 0; i < container.getNumStreams(); i++) {
            if (container.getStream(i).getStreamCoder().getCodecType() == ICodec.Type.CODEC_TYPE_VIDEO) {
                IndexEntryList = container.getStream(i).getIndexEntries();
                VideoStreamIndex = i;
                coder = container.getStream(i).getStreamCoder();
                break;
            }
        }
        if (IndexEntryList == null) throw new IllegalArgumentException("This container doesn't hava valid Index Entry");

        //获取所有的IIndexEntry的时间偏移量
        Set<Long> IndexEntrySet = new HashSet<>();
        Map<Long, Long> IFrameDict = new LinkedHashMap<>();
        IFrameDict.put(0L, 0L);//第一个I帧肯定是第0帧，时间戳也为0；
        for (IIndexEntry ie : IndexEntryList) {
            if (ie.isKeyFrame())
                IndexEntrySet.add(ie.getTimeStamp());
        }


        //将IndexEntry对应的packet的帧号获取，并写入I帧字典
        IPacket packet = IPacket.make();
        long frameNo = 0L;
        while (container.readNextPacket(packet) >= 0) {
            if (packet.getStreamIndex() == VideoStreamIndex) {
                if (IndexEntrySet.contains(packet.getTimeStamp()))
                    IFrameDict.put(frameNo, packet.getTimeStamp());
                frameNo++;
            }
        }
        if (IFrameDict.size() == 0)
            throw new IllegalArgumentException("This container doesn't have [Support] key Frame");


        //将相关信息和I帧字典包装成SecondaryMeta对象，并返回
        FileSystem hdfs;
        String Summary;
        if (Path.startsWith("hdfs")) {
            hdfs = FileSystem.get(Conf);
            Summary = SummaryUtil.generate(Path, "hdfs", hdfs);
        } else {
            Summary = SummaryUtil.generate(Path, "file", null);
        }
        SecondaryMeta SM = genProto(Path      //用path代替摘要      
                , Long.parseLong(Conf.get("dfs.blocksize"))
                , container.getContainerFormat().getInputFormatShortName()
                , container.getDuration()
                , coder.getCodec().getName()
                , -1L
                , IFrameDict);
        container.close();
        return SM;

    }

//    /**
//     * 核心方法，生成元数据
//     * 1.找到视频流对应的流编号
//     * 2.记录视频文件头的规模和起始帧偏移量
//     * 3.每当有key packet时，记录进入元数据字典中（LinkedHashMap）。
//     * 4.将视频元数据所需的各种信息加上字典组装成为视频二级元数据。
//     *
//     * @param path 视频文件url
//     * @return 对应的二级元数据对象。
//     */
//    public static SecondaryMeta writeMeta(Path path) {
//        URLProtocolManager mgr = URLProtocolManager.getManager();
//        if (path.toString().startsWith("hdfs:"))
//            mgr.registerFactory("hdfs", new HDFSProtocolHandlerFactory());
//        IContainer container = IContainer.make();
//        container.open(path.toString(), IContainer.Type.READ, null);
//        boolean header = false;
//        for (int i = 0; i < container.getNumStreams(); i++) {
//            IStream stream = container.getStream(i);
//            IStreamCoder coder = stream.getStreamCoder();
//            if (coder.getCodecType() != ICodec.Type.CODEC_TYPE_VIDEO)
//                continue;
//            IPacket packet = IPacket.make();
//            Long frameNo = 0L;
//            Map<Long, Long> map = new LinkedHashMap<>();
//            while (container.readNextPacket(packet) >= 0) {
//                if (packet.getStreamIndex() == i) {
//                    if (!header) {
//                        map.put(frameNo, packet.getPosition());
//                        header = true;
//                    }
//                    frameNo++;
//                    if (packet.isKeyPacket()) {
//                        map.put(frameNo, packet.getPosition());
//                    }
//                }
//            }
//            LOG.debug(map.size());
//            SecondaryMeta SM = genProto(SummaryUtil.generateSummary(path)
//                    , Long.parseLong(ConfUtil.generate().get("dfs.blocksize"))
//                    , container.getContainerFormat().getInputFormatShortName()
//                    , container.getDuration()
//                    , coder.getCodec().getName()
//                    , frameNo
//                    , map);
//            try {
//                FileSystem hdfs = FileSystem.get(ConfUtil.generate());
//                FSDataOutputStream fsout = hdfs.create(new Path("hdfs://vm1:9000/yty/meta/" + SM.getVideoSummary()), true);
//                SM.writeTo(fsout);
//                fsout.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            container.close();
//            return SM;
//        }
//        container.close();
//        return null;
//    }

//    /**
//     * 利用视频文件中内嵌的IndexEntry来快速定位，但是一方面部分视频容器的IndexEntry的数量太少，另一方面部分视频容器可能不支持
//     * 定位效果不理想，所以考虑废弃，并使用key packet来作为分布式读取的入口。
//     *
//     * @param path    文件路径
//     * @param isLocal
//     */
//    @Deprecated
//    public static void gererate(Path path, boolean isLocal) {
//        IContainer container;
//        int numStreams;
//        if (isLocal) {
//            try {
//                File file = new File(path.toString());
//                container = IContainer.make();
//
//                // open the video file
//                int r = container.open(file.toString(), IContainer.Type.READ, null);
//
//                // if video file can't be read , throw exception
//                if (r < 0) {
//                    IError error = IError.make(r);
//                    throw new IllegalArgumentException("could not open container from " + file + ", error = " + error.getDescription());
//                }
//
//                numStreams = 1;
//
//                // if the video has no stream , throw exception
//                if (numStreams <= 0)
//                    throw new IllegalArgumentException("No streams found in container.");
//                // all_indices include all the byte pos of the video stream
//                TreeMap<Long, Long> all_indices = new TreeMap<>();
//                // get lists of index entries of the streams
//                List<IIndexEntry> index_list[] = new List[numStreams];
//                boolean[] get_keyframes = new boolean[numStreams];
//                int list_done = 0;
//                int i = 0;
//                for (int j = 0; j < container.getNumStreams(); j++) {
//                    IStream stream = container.getStream(j);
//                    IStreamCoder coder = stream.getStreamCoder();
//                    LOG.debug("stream " + j + ": " + coder.getCodecType().name());
//                    if (coder.getCodecType() != ICodec.Type.CODEC_TYPE_VIDEO)
//                        continue;
//                    index_list[i] = stream.getIndexEntries();
//                    if (index_list[i].isEmpty()) list_done++;
//                    LOG.debug("index-list-size = " + index_list[i].size());
//                    // if the stream is a video stream get_keyframes flag is set
//                    get_keyframes[i] = coder.getCodecType() == ICodec.Type.CODEC_TYPE_VIDEO;
//                    LOG.debug("get_keyframes : " + get_keyframes[i]);
//                    i++;
//                    // video stream was found. if "onlyVideo" then don't look at any other streams
//                    break;
//                }
//                int counter[] = new int[numStreams];
//                for (int j = 0; j < numStreams; j++) counter[j] = 0;
//
//                // iterate over all streams and put the smallest (key-)index-entry of the current index-entries of the streams
//                // into all_indices-list. stops when all list have been parsed.
//                while (list_done < numStreams) {
//                    long frame_no = -1;
//                    long min_pos = -1;
//                    int list_nr = -1;
//                    for (int j = 0; j < numStreams; j++) {
//                        if (index_list[j].size() > counter[j]) {
//
//                            // if get_keyframes is true for this stream move up to next keyframe
//                            // but at the beginning put the smallest index entry at first position (see first boolean statement)
//                            while (counter[j] != 0 &&
//                                    get_keyframes[j] &&
//                                    counter[j] < index_list[j].size() &&
//                                    !index_list[j].get(counter[j]).isKeyFrame()) counter[j]++;
//                            if (counter[j] >= index_list[j].size()) {
//                                list_done++;
//                                continue;
//                            }
//
//                            // get index with smallest current_index-position
//                            IIndexEntry current_index = index_list[j].get(counter[j]);
//                            if (min_pos == -1 || current_index.getPosition() < min_pos) {
//                                min_pos = current_index.getPosition();
//                                frame_no = current_index.getTimeStamp();
//                                list_nr = j;
//                            }
//                        }
//                    }
//
//                    if (list_nr == -1) break;
//                    counter[list_nr]++;
//                    if (index_list[list_nr].size() <= counter[list_nr]) list_done++;
//
//                    all_indices.put(min_pos, frame_no);
//                }
//                // end-of-file is also treated as index entry
//                // frame_no = -1, because it does not matter
//
//                all_indices.put(container.getFileSize(), -1L);
//                LOG.debug("all_indices.size = " + all_indices.size());
//                System.out.println(all_indices);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//
//        }
//    }
}
