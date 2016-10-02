package net.hycube.maintenance;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.MessageByteConversionException;
import net.hycube.messaging.messages.MessageByteConversionRuntimeException;

public class HyCubePongMessageData {

	public static int PONG_MESSAGE_DATA_LENGTH = 
													Integer.SIZE/8;					//message serial number
	
	protected HyCubePongMessageData() {
		
	}
	
	public HyCubePongMessageData(int pingSerialNo) {
		this.pingSerialNo = pingSerialNo;
	}
	
	protected int pingSerialNo;

	public int getPingSerialNo() {
		return pingSerialNo;
	}

	public void setPingSerialNo(int pingSerialNo) {
		this.pingSerialNo = pingSerialNo;
	}
	
	
	public byte[] getBytes() {
		ByteBuffer b = ByteBuffer.allocate(PONG_MESSAGE_DATA_LENGTH);
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		b.putInt(pingSerialNo);
		byte[] bytes = b.array();
		return bytes;

	}
	
	public static HyCubePongMessageData fromBytes(byte[] bytes) throws MessageByteConversionException {
		
		HyCubePongMessageData pongMsg = new HyCubePongMessageData();
		
		if (bytes == null) {
			throw new MessageByteConversionRuntimeException("Could not convert the byte array to the message object. The byte array passed to the method is null.");
		}
		
		ByteBuffer b = ByteBuffer.wrap(bytes);
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		try {
			pongMsg.pingSerialNo = b.getInt();
		}
		catch (BufferUnderflowException e) {
			//this should not happen, as the size of the array is checked before.
			throw new MessageByteConversionException("The length of the byte array passed to the method is not equal to the expected message data length.");
		}
		
		if (bytes.length != PONG_MESSAGE_DATA_LENGTH) {
			throw new MessageByteConversionException("The length of the byte array passed to the method is not equal to the expected message data length.");
		}
		
		return pongMsg;
		
	}
	
}
