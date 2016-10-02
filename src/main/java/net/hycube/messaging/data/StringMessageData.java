package net.hycube.messaging.data;

import java.io.UnsupportedEncodingException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import net.hycube.core.UnrecoverableRuntimeException;
import net.hycube.logging.LogHelper;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.MessageByteConversionException;
import net.hycube.messaging.messages.MessageByteConversionRuntimeException;

public class StringMessageData {

	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(StringMessageData.class); 
	
	protected int calculateStringMessageDataLength() {
		int dataLength =
							Short.SIZE/8					//string length
							+ string.length()		//string
							;
		return dataLength;
	}
													
						
	
	protected StringMessageData() {
		
	}
	
	public StringMessageData(String string) {
		this.string = string;
		
	}
	
	
	protected String string;

	
	public String getString() {
		return string;
	}

	public void setString(String string) {
		this.string = string;
	}
	
	
	public byte[] getBytes() {
		if (string == null) {
			throw new MessageByteConversionRuntimeException("The message data cannot be converted to a byte array. All message fields should be set.");
		}
		if (string.length() > Short.MAX_VALUE) {
			throw new MessageByteConversionRuntimeException("The message data cannot be converted to a byte array. String length should be less than or equal to " + Short.MAX_VALUE + ".");
		}
		
			
		ByteBuffer b = ByteBuffer.allocate(calculateStringMessageDataLength());
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		b.putShort((short) string.length())	;										//network address length
		try {
			b.put(string.getBytes(HyCubeMessage.MSG_STRING_CHARSET));		//network address
		} catch (UnsupportedEncodingException e) {
			//invalid charset defined in GlobalConstants class - this should never happen
			userLog.fatal("Internal application error. See the dev log for more details.");
			devLog.fatal("Invalid charset for encoding strings in messages in net.hycube.GlobalConstants class.", e);
			throw new UnrecoverableRuntimeException("Invalid charset defined for encoding strings in messages.", e);
		}
		
		
		byte[] bytes = b.array();
		return bytes;

	}
	
	public static StringMessageData fromBytes(byte[] bytes) throws MessageByteConversionException {
		
		StringMessageData stringMsg = new StringMessageData();
		
		if (bytes == null) {
			throw new MessageByteConversionException("Could not convert the byte array to the message object. The byte array passed to the method is null.");
		}
		
		ByteBuffer b = ByteBuffer.wrap(bytes);
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		try {
			//string:
			int stringLength = b.getShort();
			if (stringLength < 0) {
				throw new MessageByteConversionException("Could not convert the byte array to the message data object. The string length is negative.");
			}
			if (stringLength == 0) stringMsg.string = ""; 
			else {
				byte[] stringB = new byte[stringLength];
				b.get(stringB);
				try {
					stringMsg.string = new String(stringB, HyCubeMessage.MSG_STRING_CHARSET);
				} catch (UnsupportedEncodingException e) {
					//invalid charset defined in GlobalConstants class - this should never happen
					userLog.fatal("Internal application error. See the dev log for more details.");
					devLog.fatal("Invalid charset for encoding strings in messages in net.hycube.GlobalConstants class.", e);
					throw new UnrecoverableRuntimeException("Invalid charset defined for encoding strings in messages.", e);
				}
			}
						
		}
		catch (BufferUnderflowException e) {
			throw new MessageByteConversionException("The length of the byte array passed to the method is not equal to the expected message data length.");
		}
		
		if (stringMsg.calculateStringMessageDataLength() != bytes.length) {
			throw new MessageByteConversionException("The length of the byte array passed to the method is not equal to the expected message data length.");
		}
		
		return stringMsg;
		
	}



	
}
