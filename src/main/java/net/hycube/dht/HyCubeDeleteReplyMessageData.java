package net.hycube.dht;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.MessageByteConversionException;
import net.hycube.messaging.messages.MessageByteConversionRuntimeException;

public class HyCubeDeleteReplyMessageData {

	
	protected static final  int BOOLEAN_OPTIONS_BYTES = 
			Integer.SIZE / 8		//boolean options
			;
	
	protected static final int OPTION_BIT_NO_DELETE_STATUS = 0;
	
	
	public static final String MSG_RESOURCE_DESCRIPTOR_STRING_CHARSET = "UTF-8";

	
	
	protected int calculateMessageDataLength() {
		int dataLength;
		
		dataLength = 
			
			+ Integer.SIZE/8				//delete id

			+ BOOLEAN_OPTIONS_BYTES			//options
			
			;


		return dataLength;
		
		
	}
													
						
	
	protected HyCubeDeleteReplyMessageData() {
		
	}
	
	public HyCubeDeleteReplyMessageData(int commandId, boolean status) {
		this.commandId = commandId;
		this.deleteStatus = status;
	
	}
	

	protected int commandId;
	protected boolean deleteStatus;
	
	

	
	public int getCommandId() {
		return commandId;
	}

	public void setCommandId(int commandId) {
		this.commandId = commandId;
	}
	
	public boolean getDeleteStatus() {
		return deleteStatus;
	}

	public void setDeleteStatus(boolean deleteStatus) {
		this.deleteStatus = deleteStatus;
	}
	

	
	
	public byte[] getBytes() {
					
		ByteBuffer b = ByteBuffer.allocate(calculateMessageDataLength());
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		b.putInt(commandId);
		
		
		int options = 0;
		if (deleteStatus) {
			options = options | (1 << OPTION_BIT_NO_DELETE_STATUS);
		}
		b.putInt(options);
		

		
		byte[] bytes = b.array();
		return bytes;

	}
	
	
	
	public static HyCubeDeleteReplyMessageData fromBytes(byte[] bytes) throws MessageByteConversionException {
		
		HyCubeDeleteReplyMessageData msgData = new HyCubeDeleteReplyMessageData();
		
		if (bytes == null) {
			throw new MessageByteConversionRuntimeException("Could not convert the byte array to the message object. The byte array passed to the method is null.");
		}
		
		ByteBuffer b = ByteBuffer.wrap(bytes);
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		try {
			
			int commandId = b.getInt();
			msgData.setCommandId(commandId);
			

			int options = b.getInt();
			if ((options & (1 << OPTION_BIT_NO_DELETE_STATUS)) != 0) {
				msgData.deleteStatus = true;
			}
			else {
				msgData.deleteStatus = false;
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
