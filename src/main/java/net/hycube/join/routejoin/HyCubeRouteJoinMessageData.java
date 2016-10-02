package net.hycube.join.routejoin;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import net.hycube.core.HyCubeNodeId;
import net.hycube.core.NodeIdByteConversionException;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.MessageByteConversionException;
import net.hycube.messaging.messages.MessageByteConversionRuntimeException;

public class HyCubeRouteJoinMessageData {

//	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
//	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(JoinMessageData.class); 
	
	
	protected int BOOLEAN_OPTIONS_BYTES = 
			Integer.SIZE / 8		//boolean options
			;

	protected static final int OPTION_BIT_NO_DISCOVER_PUBLIC_NETWORK_ADDRESS = 0;
	
	
	protected int calculateSearchJoinMessageDataLength(int nodeIdDimensions, int nodeIdDigitsCount) {
		int dataLength =
							+ Integer.SIZE/8					//join id
							+ BOOLEAN_OPTIONS_BYTES
							+ HyCubeNodeId.getByteLength(nodeIdDimensions, nodeIdDigitsCount)	//join node id
							;

		return dataLength;
		
	}
													
						
	
	protected HyCubeRouteJoinMessageData() {
		
	}
	
	public HyCubeRouteJoinMessageData(int joinId, HyCubeNodeId joinNodeId, boolean discoverPublicNetworkAddress) {
		this.joinId = joinId;
		this.joinNodeId = joinNodeId;
		this.discoverPublicNetworkAddress = discoverPublicNetworkAddress;
	
	}
	

	protected int joinId;
	protected HyCubeNodeId joinNodeId;
	protected boolean discoverPublicNetworkAddress;

	
	public int getJoinId() {
		return joinId;
	}

	public void setJoinId(int joinId) {
		this.joinId = joinId;
	}
	
	public HyCubeNodeId getJoinNodeId() {
		return joinNodeId;
	}

	public void setJoinNodeId(HyCubeNodeId joinNodeId) {
		this.joinNodeId = joinNodeId;
	}
	
	public boolean getDiscoverPublicNetworkAddress() {
		return discoverPublicNetworkAddress;
	}

	public void setDiscoverPublicNetworkAddress(boolean discoverPublicNetworkAddress) {
		this.discoverPublicNetworkAddress = discoverPublicNetworkAddress;
	}
	
	
	
	public byte[] getBytes(int nodeIdDimensions, int nodeIdDigitsCount) {
					
		ByteBuffer b = ByteBuffer.allocate(calculateSearchJoinMessageDataLength(nodeIdDimensions, nodeIdDigitsCount));
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		b.putInt(joinId);					//joinId
		
		
		//join node id
		if (joinNodeId.getDimensions() != nodeIdDimensions) throw new MessageByteConversionRuntimeException("The message data cannot be converted to a byte array. Invalid dimensions count."); 
		if (joinNodeId.getDigitsCount() != nodeIdDigitsCount) throw new MessageByteConversionRuntimeException("The message data cannot be converted to a byte array. Invalid digits count.");
		b.put(joinNodeId.getBytes(HyCubeMessage.MESSAGE_BYTE_ORDER));		//join node id
		
		
		
		
		//set the options field:
		int options = 0; 
		if (discoverPublicNetworkAddress) {
			options = options | (1 << OPTION_BIT_NO_DISCOVER_PUBLIC_NETWORK_ADDRESS);
		}
		b.putInt(options);					//options
		
		


		byte[] bytes = b.array();
		return bytes;

	}
	
	public static HyCubeRouteJoinMessageData fromBytes(byte[] bytes, int nodeIdDimensions, int nodeIdDigitsCount) throws MessageByteConversionException {
		
		HyCubeRouteJoinMessageData joinMsg = new HyCubeRouteJoinMessageData();
		
		if (bytes == null) {
			throw new MessageByteConversionRuntimeException("Could not convert the byte array to the message object. The byte array passed to the method is null.");
		}
		
		ByteBuffer b = ByteBuffer.wrap(bytes);
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		int idByteLength = HyCubeNodeId.getByteLength(nodeIdDimensions, nodeIdDigitsCount);
		byte[] nodeIdB;
		
		
		try {
			
			//joinId:
			int joinId = b.getInt();
			joinMsg.setJoinId(joinId);
			
			
		
			//join node id:
			HyCubeNodeId joinNodeId;
			nodeIdB = new byte[idByteLength];
			b.get(nodeIdB);
			try {
				joinNodeId = HyCubeNodeId.fromBytes(nodeIdB, nodeIdDimensions, nodeIdDigitsCount, HyCubeMessage.MESSAGE_BYTE_ORDER);
			} catch (NodeIdByteConversionException e) {
				throw new MessageByteConversionException("Error while converting a byte array to Message. Could not convert the byte representation of a node id.", e);
			}
			joinMsg.setJoinNodeId(joinNodeId);
			
			
			
			//get options:
			int options = b.getInt();
			
			boolean discoverPublicNetworkAddress;
			if ((options & (1 << OPTION_BIT_NO_DISCOVER_PUBLIC_NETWORK_ADDRESS)) != 0) {
				discoverPublicNetworkAddress = true;
			}
			else {
				discoverPublicNetworkAddress = false;
			}
			joinMsg.setDiscoverPublicNetworkAddress(discoverPublicNetworkAddress);
			
			
						
		}
		catch (BufferUnderflowException e) {
			throw new MessageByteConversionException("The length of the byte array passed to the method is not equal to the expected message data length.");
		}
		
		if (joinMsg.calculateSearchJoinMessageDataLength(nodeIdDimensions, nodeIdDigitsCount) != bytes.length) {
			throw new MessageByteConversionException("The length of the byte array passed to the method is not equal to the expected message data length.");
		}
		
		return joinMsg;
		
	}



	
}
