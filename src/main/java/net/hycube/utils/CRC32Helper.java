package net.hycube.utils;

import java.util.zip.CRC32;

public class CRC32Helper {
	
	public static int calculateCRC32(byte[] byteArray) {
		CRC32 crc32 = new CRC32();
		crc32.reset();
		crc32.update(byteArray);
		return (int)crc32.getValue();
		
	}
	
	
}
