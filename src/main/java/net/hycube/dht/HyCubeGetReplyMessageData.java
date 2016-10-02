package net.hycube.dht;

import java.io.UnsupportedEncodingException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import net.hycube.core.UnrecoverableRuntimeException;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.MessageByteConversionException;
import net.hycube.messaging.messages.MessageByteConversionRuntimeException;

public class HyCubeGetReplyMessageData {

	
	public static final String MSG_RESOURCE_DESCRIPTOR_STRING_CHARSET = "UTF-8";

	
	
	protected int calculateMessageDataLength() {
		int dataLength;
		
		dataLength = 
			
			+ Integer.SIZE/8							//get id

			+ Integer.SIZE/ 8								//number of resources returned
			
			;

		if (resourceDescriptorStrings == null || resourcesData == null || resourceDescriptorStrings.length != resourcesData.length) {
			throw new UnrecoverableRuntimeException("Resource descriptor string and data tables should not be not null and should have the same lengths.");
		}
		
		for (int i = 0; i < resourceDescriptorStrings.length; i++) {
			
			int resourceDescriptorSize;
			try {
				resourceDescriptorSize = resourceDescriptorStrings[i].getBytes(MSG_RESOURCE_DESCRIPTOR_STRING_CHARSET).length;
			} catch (UnsupportedEncodingException e) {
				throw new UnrecoverableRuntimeException("Invalid encoding specified for the conversion.");
			}
			
			dataLength += Short.SIZE/ 8;					//resource descriptor length			
			dataLength += Integer.SIZE/ 8;					//resource data length

			dataLength += resourceDescriptorSize;			//resource descriptor (String)
			dataLength += resourcesData[i].length;				//resource data
			
		}


		return dataLength;
		
		
	}
													
						
	
	protected HyCubeGetReplyMessageData() {
		
	}
	
	public HyCubeGetReplyMessageData(int commandId, String[] resourceDescriptorStrings, byte[][] resourcesData) {
		this.commandId = commandId;
		this.resourceDescriptorStrings = resourceDescriptorStrings;
		this.resourcesData = resourcesData;
	
	}
	

	protected int commandId;
	protected String[] resourceDescriptorStrings;
	protected byte[][] resourcesData;
	
	

	
	public int getCommandId() {
		return commandId;
	}

	public void setCommandId(int commandId) {
		this.commandId = commandId;
	}
	
	public String[] getResourceDescriptorStrings() {
		return resourceDescriptorStrings;
	}

	public void setResourceDescriptorStrings(String[] resourceDescriptorStrings) {
		this.resourceDescriptorStrings = resourceDescriptorStrings;
	}
	
	public byte[][] getResourcesData() {
		return resourcesData;
	}
	
	public void setResourceData(byte[][] resourcesData) {
		this.resourcesData = resourcesData;
	}
	
	
	
	public byte[] getBytes() {
		
		if (resourceDescriptorStrings == null || resourcesData == null || resourceDescriptorStrings.length != resourcesData.length) {
			throw new UnrecoverableRuntimeException("Resource descriptor string and data tables should not be not null and should have the same lengths.");
		}
		
		ByteBuffer b = ByteBuffer.allocate(calculateMessageDataLength());
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		b.putInt(commandId);
		
		
		b.putInt(resourceDescriptorStrings.length);
		
		
		
		
		for (int i = 0; i < resourceDescriptorStrings.length; i++) {
		
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
			
			
			b.putInt(resourcesData[i] != null ? resourcesData[i].length : 0);
			
	
			
			b.put(resourceDescriptorStringB);
			
			if (resourcesData[i] != null && resourcesData[i].length > 0) b.put(resourcesData[i]);
			
			
		}
		
		
		byte[] bytes = b.array();
		return bytes;

	}
	
	
	
	public static HyCubeGetReplyMessageData fromBytes(byte[] bytes) throws MessageByteConversionException {
		
		HyCubeGetReplyMessageData msgData = new HyCubeGetReplyMessageData();
		
		if (bytes == null) {
			throw new MessageByteConversionRuntimeException("Could not convert the byte array to the message object. The byte array passed to the method is null.");
		}
		
		ByteBuffer b = ByteBuffer.wrap(bytes);
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		try {
			
			
			int commandId = b.getInt();
			msgData.setCommandId(commandId);
			


			int resourceDescriptorsCount = b.getInt();
			
			if (resourceDescriptorsCount < 0) {
				throw new MessageByteConversionException("Could not convert the byte array to the message data object. The descriptors count is negative.");
			}
			
			
			
			
			String[] resourceDescriptorStrings = new String[resourceDescriptorsCount];
			byte[][] resourcesData = new byte[resourceDescriptorsCount][];

			
			for (int i = 0; i < resourceDescriptorsCount; i++) {
				
				int resourceDescriptorLength = b.getShort();
				if (resourceDescriptorLength < 0) {
					throw new MessageByteConversionException("Could not convert the byte array to the message data object. The resource descriptor length is negative.");
				}
				
				int resourceDataLength = b.getInt();
				if (resourceDataLength < 0) {
					throw new MessageByteConversionException("Could not convert the byte array to the message data object. The resource data length is negative.");
				}
				
				
				
	
				//resource descriptor string:
				byte[] resourceDescriptorStringB = new byte[resourceDescriptorLength];
				b.get(resourceDescriptorStringB);
				String resourceDescriptorString;
				try {
					resourceDescriptorString = new String(resourceDescriptorStringB, MSG_RESOURCE_DESCRIPTOR_STRING_CHARSET);
				} catch (UnsupportedEncodingException e) {
					throw new UnrecoverableRuntimeException("Invalid encoding specified for the conversion.");
				}
				resourceDescriptorStrings[i] = resourceDescriptorString;
				
				
				
				//resource data:
				if (resourceDataLength > 0) {
					byte[] resourceData = new byte[resourceDataLength];
					b.get(resourceData);
					resourcesData[i] = resourceData;
				}
			

			}
			
			msgData.resourcesData = resourcesData;
			msgData.resourceDescriptorStrings = resourceDescriptorStrings;
			
						
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
