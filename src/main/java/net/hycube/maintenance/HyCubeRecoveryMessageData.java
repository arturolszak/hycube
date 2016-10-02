package net.hycube.maintenance;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.MessageByteConversionException;
import net.hycube.messaging.messages.MessageByteConversionRuntimeException;

public class HyCubeRecoveryMessageData {

//	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
//	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeRecoveryMessageData.class); 
	
	
	protected int BOOLEAN_OPTIONS_BYTES = 
			Integer.SIZE / 8		//boolean options
			;
	
	
	protected static final int OPTION_BIT_NO_RETURN_NS = 0;
	protected static final int OPTION_BIT_NO_RETURN_RT1 = 1;
	protected static final int OPTION_BIT_NO_RETURN_RT2 = 2;

	
	
	protected boolean returnNS;
	protected boolean returnRT1;
	protected boolean returnRT2;
	
	
	
	public boolean isReturnNS() {
		return returnNS;
	}



	public void setReturnNS(boolean returnNS) {
		this.returnNS = returnNS;
	}



	public boolean isReturnRT1() {
		return returnRT1;
	}



	public void setReturnRT1(boolean returnRT1) {
		this.returnRT1 = returnRT1;
	}



	public boolean isReturnRT2() {
		return returnRT2;
	}



	public void setReturnRT2(boolean returnRT2) {
		this.returnRT2 = returnRT2;
	}



	
	protected int calculateRecoveryMessageDataLength() {
		int dataLength =
				
							+ BOOLEAN_OPTIONS_BYTES
				
							;
		
		return dataLength;
		
	}
													
						
	
//	protected HyCubeRecoveryMessageData() {
//		
//	}
	
	public HyCubeRecoveryMessageData() {
		
		
	}
	
	

	
	
	
	
	public byte[] getBytes() {

			
		ByteBuffer b = ByteBuffer.allocate(calculateRecoveryMessageDataLength());
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		
		//set the options field:
		int options = 0; 
		if (returnNS) {
			options = options | (1 << OPTION_BIT_NO_RETURN_NS);
		}
		if (returnRT1) {
			options = options | (1 << OPTION_BIT_NO_RETURN_RT1);
		}
		if (returnRT2) {
			options = options | (1 << OPTION_BIT_NO_RETURN_RT2);
		}
		b.putInt(options);		//options
		
		
		byte[] bytes = b.array();
		return bytes;

	}
	
	public static HyCubeRecoveryMessageData fromBytes(byte[] bytes) throws MessageByteConversionException {
		
		HyCubeRecoveryMessageData recoveryMsg = new HyCubeRecoveryMessageData();
		
		if (bytes == null) {
			throw new MessageByteConversionRuntimeException("Could not convert the byte array to the message object. The byte array passed to the method is null.");
		}
		
		ByteBuffer b = ByteBuffer.wrap(bytes);
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		try {
			//get options:
			int options = b.getInt();
			
			if ((options & (1 << OPTION_BIT_NO_RETURN_NS)) != 0) {
				recoveryMsg.returnNS = true;
			}
			else {
				recoveryMsg.returnNS = false;
			}
			
			if ((options & (1 << OPTION_BIT_NO_RETURN_RT1)) != 0) {
				recoveryMsg.returnRT1 = true;
			}
			else {
				recoveryMsg.returnRT1 = false;
			}
			
			if ((options & (1 << OPTION_BIT_NO_RETURN_RT2)) != 0) {
				recoveryMsg.returnRT2 = true;
			}
			else {
				recoveryMsg.returnRT2 = false;
			}
			
		}
		catch (BufferUnderflowException e) {
			throw new MessageByteConversionException("The length of the byte array passed to the method is not equal to the expected message data length.");
		}
		
		if (recoveryMsg.calculateRecoveryMessageDataLength() != bytes.length) {
			throw new MessageByteConversionException("The length of the byte array passed to the method is not equal to the expected message data length.");
		}
		
		return recoveryMsg;
		
	}



	
}
