package net.hycube.messaging.ack;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.MessageByteConversionException;
import net.hycube.messaging.messages.MessageByteConversionRuntimeException;

public class HyCubeAckMessageData {

	public static int ACK_MESSAGE_DATA_LENGTH = 
													Integer.SIZE/8;					//message serial number
	
	protected HyCubeAckMessageData() {
		
	}
	
	public HyCubeAckMessageData(int ackSerialNo) {
		this.ackSerialNo = ackSerialNo;
	}
	
	protected int ackSerialNo;

	public int getAckSerialNo() {
		return ackSerialNo;
	}

	public void setAckSerialNo(int ackSerialNo) {
		this.ackSerialNo = ackSerialNo;
	}
	
	
	public byte[] getBytes() {
		ByteBuffer b = ByteBuffer.allocate(ACK_MESSAGE_DATA_LENGTH);
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		b.putInt(ackSerialNo);
		byte[] bytes = b.array();
		return bytes;

	}
	
	public static HyCubeAckMessageData fromBytes(byte[] bytes) throws MessageByteConversionException {
		
		HyCubeAckMessageData ackMsg = new HyCubeAckMessageData();
		
		if (bytes == null) {
			throw new MessageByteConversionRuntimeException("Could not convert the byte array to the message object. The byte array passed to the method is null.");
		}
		
		if (bytes.length != ACK_MESSAGE_DATA_LENGTH) {
			throw new MessageByteConversionException("The length of the byte array passed to the method is not equal to the expected message data length.");
		}
		
		ByteBuffer b = ByteBuffer.wrap(bytes);
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		try {
			ackMsg.ackSerialNo = b.getInt();
		}
		catch (BufferUnderflowException e) {
			//this should not happen, as the size of the array is checked before.
			throw new MessageByteConversionException("The length of the byte array passed to the method is not equal to the expected message data length.");
		}
		
		return ackMsg;
		
	}
	
}
