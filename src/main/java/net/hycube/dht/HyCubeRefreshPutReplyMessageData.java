package net.hycube.dht;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.MessageByteConversionException;
import net.hycube.messaging.messages.MessageByteConversionRuntimeException;

public class HyCubeRefreshPutReplyMessageData {

	
	protected static final  int BOOLEAN_OPTIONS_BYTES = 
			Integer.SIZE / 8		//boolean options
			;
	
	protected static final int OPTION_BIT_NO_REFRESH_PUT_STATUS = 0;
	
	
	public static final String MSG_RESOURCE_DESCRIPTOR_STRING_CHARSET = "UTF-8";

	
	
	protected int calculateMessageDataLength() {
		int dataLength;
		
		dataLength = 
			
			+ Integer.SIZE/8				//refresh put id

			+ BOOLEAN_OPTIONS_BYTES			//options
			
			;


		return dataLength;
		
		
	}
													
						
	
	protected HyCubeRefreshPutReplyMessageData() {
		
	}
	
	public HyCubeRefreshPutReplyMessageData(int commandId, boolean refreshPutStatus) {
		this.commandId = commandId;
		this.refreshPutStatus = refreshPutStatus;
	
	}
	

	protected int commandId;
	protected boolean refreshPutStatus;
	
	

	
	public int getCommandId() {
		return commandId;
	}

	public void setCommandId(int commandId) {
		this.commandId = commandId;
	}
	
	public boolean getRefreshPutStatus() {
		return refreshPutStatus;
	}

	public void setRefreshPutStatus(boolean refreshPutStatus) {
		this.refreshPutStatus = refreshPutStatus;
	}
	

	
	
	public byte[] getBytes() {
					
		ByteBuffer b = ByteBuffer.allocate(calculateMessageDataLength());
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		b.putInt(commandId);
		
		
		int options = 0;
		if (refreshPutStatus) {
			options = options | (1 << OPTION_BIT_NO_REFRESH_PUT_STATUS);
		}
		b.putInt(options);
		

		
		byte[] bytes = b.array();
		return bytes;

	}
	
	
	
	public static HyCubeRefreshPutReplyMessageData fromBytes(byte[] bytes) throws MessageByteConversionException {
		
		HyCubeRefreshPutReplyMessageData msgData = new HyCubeRefreshPutReplyMessageData();
		
		if (bytes == null) {
			throw new MessageByteConversionRuntimeException("Could not convert the byte array to the message object. The byte array passed to the method is null.");
		}
		
		ByteBuffer b = ByteBuffer.wrap(bytes);
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		try {
			
			int commandId = b.getInt();
			msgData.setCommandId(commandId);
			

			int options = b.getInt();
			if ((options & (1 << OPTION_BIT_NO_REFRESH_PUT_STATUS)) != 0) {
				msgData.refreshPutStatus = true;
			}
			else {
				msgData.refreshPutStatus = false;
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
