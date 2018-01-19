package com.UtilClass.Service;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
public class ServerHashUtil {
	public static int findServerSeq(String url, int serverSize) {
		String sha = SHA_256(url);
		return Math.abs(sha.hashCode()) % serverSize;
	}
	private static String SHA_256(String strText) {
		return SHA(strText, "SHA-256");
	}
	private static String SHA_512(String strText) {
		return SHA(strText, "SHA-512");
	}
	private static String SHA(final String strText, final String strType) {
		// 返回值
		String strResult = null;
		// 是否是有效字符串
		if (strText != null && strText.length() > 0) {
			try {
				MessageDigest messageDigest = MessageDigest.getInstance(strType);
				// 传入要加密的字符串
				messageDigest.update(strText.getBytes());
				// 得到 byte 类型结果
				byte byteBuffer[] = messageDigest.digest();
				// 將 byte 转换为 string
				StringBuffer strHexString = new StringBuffer();
				// 遍历 byte buffer
				for (int i = 0; i < byteBuffer.length; i++) {
					String hex = Integer.toHexString(0xff & byteBuffer[i]);
					if (hex.length() == 1) {
						strHexString.append('0');
					}
					strHexString.append(hex);
				}
				// 得到返回结果
				strResult = strHexString.toString();
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
		return strResult;
	}
}
