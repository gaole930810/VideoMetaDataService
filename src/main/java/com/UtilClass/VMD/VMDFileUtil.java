package com.UtilClass.VMD;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.Proto.SecondaryMetaClass;
import com.UtilClass.Service.ConfUtil;
import com.UtilClass.Service.UploadFile;
import com.xuggle.xuggler.ICodec;
import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IPacket;
import com.xuggle.xuggler.IStream;
import com.xuggle.xuggler.IStreamCoder;
import com.xuggle.xuggler.io.URLProtocolManager;

public class VMDFileUtil {
	public static final Log LOG = LogFactory.getLog(VMDFileUtil.class);

	public static void main(String[] args) {
		Path src = new Path(args[0]);
		File sf = new File(args[0]);
		Path des = new Path(args[1] + sf.getName());
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(ConfUtil.generate());
			if (!hdfs.exists(des)) {
				LOG.info("the file doesn't exist !\r\nstart uploading file......");
				UploadFile.upload(src, des);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		LOG.info("start Generate VMDFile......");
		String[] fn = sf.getName().split("\\.");
//		LOG.info(args[1]);
//		LOG.info(args[2]);
//		LOG.info(args[2]+File.separator + fn[0] + "_" + fn[1]);
		GenerateVMDFile(args[2]+File.separator + fn[0] + "_" + fn[1], des);
		LOG.info(GenerateMetaFromFile(args[2]+File.separator + fn[0] + "_" + fn[1]).getVideoSummary());
		return;
	}
/**
 * Generate VMDFile
 * @param url VMDFilePath
 * @param path  VideoPathOnHDFS
 */
	public static void GenerateVMDFile(String url, Path path) {
		File file= new File(url);
		if(file.exists()){
			LOG.info("file:"+url+" exist!");
			return;
		}
		SecondaryMetaClass.SecondaryMeta SM = GenerateMetaFromHDFS(path);
		try {
			// FileInputStream ist=new FileInputStream(args[0]);
			FileOutputStream fost = new FileOutputStream(url);
			SM.writeTo(fost);
			fost.close();
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			System.out.println(e);
		}
	}
/**
 * GenerateMeta From Local VMDFile
 * @param url VMDFilePath
 * @return SecondaryMetaClass.SecondaryMeta
 */
	public static SecondaryMetaClass.SecondaryMeta GenerateMetaFromFile(String url) {
		try {
			FileInputStream fist = new FileInputStream(url);
			SecondaryMetaClass.SecondaryMeta SM = SecondaryMetaClass.SecondaryMeta.parseFrom(fist);
			return SM;
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			System.out.println(e);
		}
		return null;
	}

	public static SecondaryMetaClass.SecondaryMeta GenerateMetaFromHDFS(Path path) {
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
			SecondaryMetaClass.SecondaryMeta SM = genProto(path.toString(),
					Long.parseLong(ConfUtil.generate().get("dfs.blocksize")),
					container.getContainerFormat().getInputFormatShortName(), container.getDuration(),
					coder.getCodec().getName(), frameNo, map);
			container.close();
			return SM;
		}
		container.close();
		return null;
	}

	public static SecondaryMetaClass.SecondaryMeta genProto(String VS, Long BI, String CI, Long TS, String EI, Long FN,
			Map<Long, Long> FMI) {

		List<SecondaryMetaClass.SecondaryMeta.FrameInfoGroup> fmi = FMI.entrySet().stream()
				.map(entry -> SecondaryMetaClass.SecondaryMeta.FrameInfoGroup.newBuilder()
						.setStartIndex(entry.getValue()).setStartFrameNo(entry.getKey()).build())
				.collect(ArrayList::new, (L, F) -> L.add(F), (l1, l2) -> l1.addAll(l2));
		SecondaryMetaClass.SecondaryMeta SM = SecondaryMetaClass.SecondaryMeta.newBuilder().setBlockInfo(BI)
				.setContainerInfo(CI).setVideoSummary(VS).setTimestamp(TS).setEncodeInfo(EI).setFrameNumber(FN)
				.addAllFrameMetaInfo(fmi).build();
		return SM;
	}

}
