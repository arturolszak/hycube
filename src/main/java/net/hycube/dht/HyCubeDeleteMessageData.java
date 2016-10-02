package net.hycube.dht;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import net.hycube.core.UnrecoverableRuntimeException;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.MessageByteConversionException;
import net.hycube.messaging.messages.MessageByteConversionRuntimeException;

public class HyCubeDeleteMessageData {

	
	public static final String MSG_RESOURCE_DESCRIPTOR_STRING_CHARSET = "UTF-8";

	
	
	protected int calculateMessageDataLength() {
		int dataLength;
		
		
		int resourceDescriptorSize;
		try {
			resourceDescriptorSize = resourceDescriptorString.getBytes(MSG_RESOURCE_DESCRIPTOR_STRING_CHARSET).length;
		} catch (UnsupportedEncodingException e) {
			throw new UnrecoverableRuntimeException("Invalid encoding specified for the conversion.");
		}

		
		dataLength = 
			
			+ Integer.SIZE/8							//delete id

			
			+ Short.SIZE/8								//key length

			+ Short.SIZE/ 8								//resource descriptor length			

			
			+ (int) Math.ceil((key.bitLength() + 1)/8) + 1	//key
			
			+ resourceDescriptorSize					//resource descriptor (String)

			
			;


		return dataLength;
		
	}
													
						
	
	protected HyCubeDeleteMessageData() {
		
	}
	
	public HyCubeDeleteMessageData(int commandId, BigInteger key, String resourceDescriptorString) {
		this.commandId = commandId;
		this.key = key;
		this.resourceDescriptorString = resourceDescriptorString;
	
	}
	

	protected int commandId;
	protected BigInteger key;
	protected String resourceDescriptorString;
	
	

	
	public int getCommandId() {
		return commandId;
	}

	public void setCommandId(int commandId) {
		this.commandId = commandId;
	}
	
	public BigInteger getKey() {
		return key;
	}

	public void setKey(BigInteger key) {
		this.key = key;
	}
	
	public String getResourceDescriptorString() {
		return resourceDescriptorString;
	}
	
	public void setResourceDescriptorString(String resourceDescriptorString) {
		this.resourceDescriptorString = resourceDescriptorString;
	}

	
	
	public byte[] getBytes() {
					
		ByteBuffer b = ByteBuffer.allocate(calculateMessageDataLength());
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		b.putInt(commandId);
		
		
		byte[] keyB = key.toByteArray();
		if (keyB.length > Short.MAX_VALUE) {
			throw new MessageByteConversionRuntimeException("The length of the key exceeds Short.MAX_VALUE.");
		}
		b.putShort((short) keyB.length);
		
		

		
		
		byte[] resourceDescriptorStringB;
		try {
			resourceDescriptorStringB = resourceDescriptorString.getBytes(MSG_RESOURCE_DESCRIPTOR_STRING_CHARSET);
		} catch (UnsupportedEncodingException e) {
			throw new UnrecoverableRuntimeException("Invalid encoding specified for the conversion.");
		}
		if (resourceDescriptorStringB.length > Short.MAX_VALUE) {
			throw new MessageByteConversionRuntimeException("The length of the descriptor string byte representation length exceeds Short.MAX_VALUE.");
		}
		b.putShort((short) resourceDescriptorStringB.length);

		
		
		
		b.put(keyB);
		
		b.put(resourceDescriptorStringB);
		
		
		
		
		byte[] bytes = b.array();
		return bytes;

	}
	
	
	
	public static HyCubeDeleteMessageData fromBytes(byte[] bytes) throws MessageByteConversionException {
		
		HyCubeDeleteMessageData msgData = new HyCubeDeleteMessageData();
		
		if (bytes == null) {
			throw new MessageByteConversionRuntimeException("Could not convert the byte array to the message object. The byte array passed to the method is null.");
		}
		
		ByteBuffer b = ByteBuffer.wrap(bytes);
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		try {
			
			
			int commandId = b.getInt();
			msgData.setCommandId(commandId);
			

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
			msgData.key = key;
			
			
			
			//resource descriptor string:
			byte[] resourceDescriptorStringB = new byte[resourceDescriptorLength];
			b.get(resourceDescriptorStringB);
			String resourceDescriptorString;
			try {
				resourceDescriptorString = new String(resourceDescriptorStringB, MSG_RESOURCE_DESCRIPTOR_STRING_CHARSET);
			} catch (UnsupportedEncodingException e) {
				throw new UnrecoverableRuntimeException("Invalid encoding specified for the conversion.");
			}
			msgData.resourceDescriptorString = resourceDescriptorString;
			
			
			

			

						
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
