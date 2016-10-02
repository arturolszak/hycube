package net.hycube.dht;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import net.hycube.core.UnrecoverableRuntimeException;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.MessageByteConversionException;
import net.hycube.messaging.messages.MessageByteConversionRuntimeException;

public class HyCubeReplicateMessageData {

	
	public static final String MSG_RESOURCE_DESCRIPTOR_STRING_CHARSET = "UTF-8";

	
	
	protected int calculateMessageDataLength() {
		
		int dataLength;
		
		dataLength = 

			+ Integer.SIZE								//resources number
			
			;
				
			for (int i = 0; i < resourcesNum; i++) {

				int resourceDescriptorSize;
				try {
					resourceDescriptorSize = resourceDescriptorStrings[i].getBytes(MSG_RESOURCE_DESCRIPTOR_STRING_CHARSET).length;
				} catch (UnsupportedEncodingException e) {
					throw new UnrecoverableRuntimeException("Invalid encoding specified for the conversion.");
				}
				
				dataLength +=
				
				+ Short.SIZE/8								//key length
				+ Short.SIZE/ 8								//resource descriptor length


				+ (int) Math.ceil((keys[i].bitLength() + 1)/8) + 1	//key
				+ resourceDescriptorSize							//resource descriptor (String)
				+ Long.SIZE/8										//refresh time
				
				+ Integer.SIZE/8									//replication spread nodes num
				
				;
				
			}

			;


		return dataLength;
		
	}
													
						
	
	protected HyCubeReplicateMessageData() {
		
	}
	
	public HyCubeReplicateMessageData(int resourcesNum, BigInteger[] keys, String[] resourceDescriptorStrings, long[] refreshTimes, int[] replicationSpreadNodesNums) {
		this.resourcesNum = resourcesNum;
		this.keys = keys;
		this.resourceDescriptorStrings = resourceDescriptorStrings;
		this.refreshTimes = refreshTimes;
		this.replicationSpreadNodesNums = replicationSpreadNodesNums;
		
	
	}
	

	protected int resourcesNum;
	protected BigInteger[] keys;
	protected String[] resourceDescriptorStrings;
	protected long[] refreshTimes;
	int[] replicationSpreadNodesNums;
	
	

	
	public int getResourcesNum() {
		return resourcesNum;
	}

	public void setResourcesNum(int resourcesNum) {
		this.resourcesNum = resourcesNum;
	}
	
	public BigInteger[] getKeys() {
		return keys;
	}

	public void setKeys(BigInteger[] keys) {
		this.keys = keys;
	}
	
	public String[] getResourceDescriptorStrings() {
		return resourceDescriptorStrings;
	}

	public void setResourceDescriptorStrings(String[] resourceDescriptorStrings) {
		this.resourceDescriptorStrings = resourceDescriptorStrings;
	}
	
	public long[] getRefreshTimes() {
		return refreshTimes;
	}

	public void setRefreshTimes(long[] refreshTimes) {
		this.refreshTimes = refreshTimes;
	}
	
	public int[] getReplicationSpreadNodesNums() {
		return replicationSpreadNodesNums;
	}
	
	public void setReplicationSpreadNodesNums(int[] replicationSpreadNodesNums) {
		this.replicationSpreadNodesNums = replicationSpreadNodesNums;
	}
	
	
	
	
	public byte[] getBytes() {
					
		ByteBuffer b = ByteBuffer.allocate(calculateMessageDataLength());
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		b.putInt(resourcesNum);
		
		for (int i = 0; i < resourcesNum; i++) {
		
			
			byte[] keyB = keys[i].toByteArray();
			if (keyB.length > Short.MAX_VALUE) {
				throw new MessageByteConversionRuntimeException("The length of the key exceeds Short.MAX_VALUE.");
			}
			b.putShort((short) keyB.length);
		
		
		
		
			byte[] resourceDescriptorStringB;
			try {
				resourceDescriptorStringB = resourceDescriptorStrings[i].getBytes(MSG_RESOURCE_DESCRIPTOR_STRING_CHARSET);
			} catch (UnsupportedEncodingException e) {
				throw new UnrecoverableRuntimeException("Invalid encoding specified for the conversion.");
			}
			if (resourceDescriptorStringB.length > Short.MAX_VALUE) {
				throw new MessageByteConversionRuntimeException("The length of the descriptor string byte representation length exceeds Short.MAX_VALUE.");
			}
			b.putShort((short) resourceDescriptorStringB.length);
		
		
		

		
		
			b.put(keyB);
		
		
			b.put(resourceDescriptorStringB);
		
		
			
			
			
			b.putLong(refreshTimes[i]);
			
			
			
			b.putInt(replicationSpreadNodesNums[i]);
			
			
			
		}
		
		
		byte[] bytes = b.array();
		return bytes;

	}
	
	
	
	public static HyCubeReplicateMessageData fromBytes(byte[] bytes) throws MessageByteConversionException {
		
		HyCubeReplicateMessageData msgData = new HyCubeReplicateMessageData();
		
		if (bytes == null) {
			throw new MessageByteConversionRuntimeException("Could not convert the byte array to the message object. The byte array passed to the method is null.");
		}
		
		ByteBuffer b = ByteBuffer.wrap(bytes);
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		try {
			
			
			int resourcesNum = b.getInt();
			if (resourcesNum < 0) {
				throw new MessageByteConversionException("Invalid resources number value.");
			}
			msgData.setResourcesNum(resourcesNum);			
			
			
			msgData.keys = new BigInteger[resourcesNum];
			msgData.resourceDescriptorStrings = new String[resourcesNum];
			msgData.refreshTimes = new long[resourcesNum];
			msgData.replicationSpreadNodesNums = new int[resourcesNum];
			
					
			for (int i = 0; i < resourcesNum; i++) {
				
			
				int keyLength = b.getShort();
				if (keyLength < 0) {
					throw new MessageByteConversionException("Could not convert the byte array to the message data object. The key length is negative.");
				}
	
				int resourceDescriptorLength = b.getShort();
				if (resourceDescriptorLength < 0) {
					throw new MessageByteConversionException("Could not convert the byte array to the message data object. The resource descriptor length is negative.");
				}
				

				
				
				//key
				byte[] keyB = new byte[keyLength];
				b.get(keyB);
				BigInteger key = new BigInteger(keyB);
				msgData.keys[i] = key;
				
				
	
				//resource descriptor string:
				byte[] resourceDescriptorStringB = new byte[resourceDescriptorLength];
				b.get(resourceDescriptorStringB);
				String resourceDescriptorString;
				try {
					resourceDescriptorString = new String(resourceDescriptorStringB, MSG_RESOURCE_DESCRIPTOR_STRING_CHARSET);
				} catch (UnsupportedEncodingException e) {
					throw new UnrecoverableRuntimeException("Invalid encoding specified for the conversion.");
				}
				msgData.resourceDescriptorStrings[i] = resourceDescriptorString;
				
			
				
				
				//refresh time
				long refreshTime = b.getLong();
				msgData.refreshTimes[i] = refreshTime;
				
				
				
				//replication spread nodes num
				int replicationSpreadNodesNums = b.getInt();
				msgData.replicationSpreadNodesNums[i] = replicationSpreadNodesNums;
				
				
				
			}
			
						
		}
		catch (BufferUnderflowException e) {
			throw new MessageByteConversionException("The length of the byte array passed to the method is not equal to the expected message data length.");
		}
		
		if (msgData.calculateMessageDataLength() != bytes.length) {
			throw new MessageByteConversionException("The length of the byte array passed to the method is not equal to the expected message data length.");
		}
		
		return msgData;
		
		
	}



	
}
