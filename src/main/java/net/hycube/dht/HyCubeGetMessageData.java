package net.hycube.dht;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import net.hycube.core.UnrecoverableRuntimeException;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.MessageByteConversionException;
import net.hycube.messaging.messages.MessageByteConversionRuntimeException;

public class HyCubeGetMessageData {

	
	public static final String MSG_RESOURCE_DESCRIPTOR_STRING_CHARSET = "UTF-8";
	public static final String MSG_RESOURCE_CRITERIA_STRING_CHARSET = "UTF-8";

	
	protected int BOOLEAN_OPTIONS_BYTES = 
			Integer.SIZE / 8		//boolean options
			;
	
	protected static final int OPTION_BIT_NO_GET_FROM_CLOSEST_NODE = 0;
	
	
	protected int calculateMessageDataLength() {
		int dataLength;
		
		int criteriaSize;
		try {
			criteriaSize = criteriaString.getBytes(MSG_RESOURCE_DESCRIPTOR_STRING_CHARSET).length;
		} catch (UnsupportedEncodingException e) {
			throw new UnrecoverableRuntimeException("Invalid encoding specified for the conversion.");
		}
		
		dataLength = 
			
			+ Integer.SIZE/8							//get id

			+ BOOLEAN_OPTIONS_BYTES
			
			+ Short.SIZE/8								//key length
			
			+ Short.SIZE/8								//criteria length

			+ (int) Math.ceil((key.bitLength() + 1)/8) + 1	//key
			
			+ criteriaSize
	
			;


		return dataLength;
		
	}
													
						
	
	protected HyCubeGetMessageData() {
		
	}
	
	public HyCubeGetMessageData(int commandId, BigInteger key, String criteriaString, boolean getFromClosestNode) {
		this.commandId = commandId;
		this.key = key;
		this.criteriaString = criteriaString;
		this.getFromClosestNode = getFromClosestNode;
	
	}
	

	protected int commandId;
	protected BigInteger key;
	protected String criteriaString;
	protected boolean getFromClosestNode;
	
	

	
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
	
	public String getCriteriaString() {
		return criteriaString;
	}
	
	public void setCriteriaString(String criteriaString) {
		this.criteriaString = criteriaString;
	}
	
	public boolean isGetFromClosestNode() {
		return getFromClosestNode;
	}
	
	public void setGetFromClosestNode(boolean getFromClosestNode) {
		this.getFromClosestNode = getFromClosestNode;
	}
	
	
	
	public byte[] getBytes() {
					
		ByteBuffer b = ByteBuffer.allocate(calculateMessageDataLength());
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		b.putInt(commandId);
		
		//set the options field:
		int options = 0; 
		if (getFromClosestNode) {
			options = options | (1 << OPTION_BIT_NO_GET_FROM_CLOSEST_NODE);
		}
		b.putInt(options);					//options
		
		
		byte[] keyB = key.toByteArray();
		if (keyB.length > Short.MAX_VALUE) {
			throw new MessageByteConversionRuntimeException("The length of the key exceeds Short.MAX_VALUE.");
		}
		b.putShort((short) keyB.length);
		
		
		byte[] criteriaB;
		try {
			criteriaB = criteriaString.getBytes(MSG_RESOURCE_CRITERIA_STRING_CHARSET);
		} catch (UnsupportedEncodingException e) {
			throw new UnrecoverableRuntimeException("Invalid encoding specified for the conversion.");
		}
		if (criteriaB.length > Short.MAX_VALUE) {
			throw new MessageByteConversionRuntimeException("The length of the criteria string byte representation exceeds Short.MAX_VALUE.");
		}
		b.putShort((short) criteriaB.length);
		
		
		
		
		b.put(keyB);
		
		b.put(criteriaB);
		
		
		
		byte[] bytes = b.array();
		return bytes;

		
	}
	
	
	
	public static HyCubeGetMessageData fromBytes(byte[] bytes) throws MessageByteConversionException {
		
		HyCubeGetMessageData msgData = new HyCubeGetMessageData();
		
		if (bytes == null) {
			throw new MessageByteConversionRuntimeException("Could not convert the byte array to the message object. The byte array passed to the method is null.");
		}
		
		ByteBuffer b = ByteBuffer.wrap(bytes);
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		try {
			
			
			int commandId = b.getInt();
			msgData.setCommandId(commandId);
			
			
			
			//get options:
			int options = b.getInt();
			
			if ((options & (1 << OPTION_BIT_NO_GET_FROM_CLOSEST_NODE)) != 0) {
				msgData.getFromClosestNode = true;
			}
			else {
				msgData.getFromClosestNode = false;
			}
			

			int keyLength = b.getShort();
			if (keyLength < 0) {
				throw new MessageByteConversionException("Could not convert the byte array to the message data object. The key length is negative.");
			}
			
			
			
			int criteriaLength = b.getShort();
			if (criteriaLength < 0) {
				throw new MessageByteConversionException("Could not convert the byte array to the message data object. The criteria length is negative.");
			}
			
			
		
			
			//key
			byte[] keyB = new byte[keyLength];
			b.get(keyB);
			BigInteger key = new BigInteger(keyB);
			msgData.key = key;
			
			
			
			//resource descriptor string:
			byte[] criteriaB = new byte[criteriaLength];
			b.get(criteriaB);
			String criteria;
			try {
				criteria = new String(criteriaB, MSG_RESOURCE_CRITERIA_STRING_CHARSET);
			} catch (UnsupportedEncodingException e) {
				throw new UnrecoverableRuntimeException("Invalid encoding specified for the conversion.");
			}
			msgData.criteriaString = criteria;
			

						
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
