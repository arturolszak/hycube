package net.hycube.dht;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.MessageByteConversionException;
import net.hycube.messaging.messages.MessageByteConversionRuntimeException;

public class HyCubePutReplyMessageData {

	
	protected static final  int BOOLEAN_OPTIONS_BYTES = 
			Integer.SIZE / 8		//boolean options
			;
	
	protected static final int OPTION_BIT_NO_PUT_STATUS = 0;
	
	
	public static final String MSG_RESOURCE_DESCRIPTOR_STRING_CHARSET = "UTF-8";

	
	
	protected int calculateMessageDataLength() {
		int dataLength;
		
		dataLength = 
			
			+ Integer.SIZE/8				//put id

			+ BOOLEAN_OPTIONS_BYTES			//options
			
			;


		return dataLength;
		
		
	}
													
						
	
	protected HyCubePutReplyMessageData() {
		
	}
	
	public HyCubePutReplyMessageData(int commandId, boolean putStatus) {
		this.commandId = commandId;
		this.putStatus = putStatus;
	
	}
	

	protected int commandId;
	protected boolean putStatus;
	
	

	
	public int getCommandId() {
		return commandId;
	}

	public void setCommandId(int commandId) {
		this.commandId = commandId;
	}
	
	public boolean getPutStatus() {
		return putStatus;
	}

	public void setPutStatus(boolean putStatus) {
		this.putStatus = putStatus;
	}
	

	
	
	public byte[] getBytes() {
					
		ByteBuffer b = ByteBuffer.allocate(calculateMessageDataLength());
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		b.putInt(commandId);
		
		
		int options = 0;
		if (putStatus) {
			options = options | (1 << OPTION_BIT_NO_PUT_STATUS);
		}
		b.putInt(options);
		

		
		byte[] bytes = b.array();
		return bytes;

	}
	
	
	
	public static HyCubePutReplyMessageData fromBytes(byte[] bytes) throws MessageByteConversionException {
		
		HyCubePutReplyMessageData msgData = new HyCubePutReplyMessageData();
		
		if (bytes == null) {
			throw new MessageByteConversionRuntimeException("Could not convert the byte array to the message object. The byte array passed to the method is null.");
		}
		
		ByteBuffer b = ByteBuffer.wrap(bytes);
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		try {
			
			int commandId = b.getInt();
			msgData.setCommandId(commandId);
			

			int options = b.getInt();
			if ((options & (1 << OPTION_BIT_NO_PUT_STATUS)) != 0) {
				msgData.putStatus = true;
			}
			else {
				msgData.putStatus = false;
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
