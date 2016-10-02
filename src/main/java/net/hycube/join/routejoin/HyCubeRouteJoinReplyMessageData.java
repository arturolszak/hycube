package net.hycube.join.routejoin;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import net.hycube.core.HyCubeNodeId;
import net.hycube.core.NodeId;
import net.hycube.core.NodeIdByteConversionException;
import net.hycube.core.NodePointer;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.MessageByteConversionException;
import net.hycube.messaging.messages.MessageByteConversionRuntimeException;
import net.hycube.messaging.messages.MessageErrorRuntimeException;
import net.hycube.transport.NetworkAdapter;
import net.hycube.transport.NetworkNodePointer;

public class HyCubeRouteJoinReplyMessageData {

	
	
	protected int BOOLEAN_OPTIONS_BYTES = 
			Integer.SIZE / 8		//boolean options
			;
	
	
	protected static final int OPTION_BIT_NO_FINAL_JOIN_REPLY = 0;
	protected static final int OPTION_BIT_NO_DISCOVER_PUBLIC_NETWORK_ADDRESS = 1;
	
	
	
	protected int calculateSearchJoinReplyMessageDataLength(int nodeIdDimensions, int nodeIdDigitsCount) {
		int dataLength =
				+ BOOLEAN_OPTIONS_BYTES		//boolean options
				+ Integer.SIZE/8			//join id
				+ Short.SIZE/ 8				//number of nodes
				;
		
		//requestor address:
		if (requestorNetworkAddress != null) {
			dataLength += requestorNetworkAddress.length;
		}
		
		//check the count of the addresses and ids:
		if (networkAddresses == null || nodeIds == null || networkAddresses.length != nodeIds.length) {
			throw new MessageByteConversionRuntimeException("The message data cannot be converted to a byte array. Cannot calculate the data length. The number of network addresses is not equal to the number of node ids.");
		}
		
		////network addresses lengths:
		//dataLength += networkAddresses.length * Short.SIZE/8;
		
		//network addresses:
		for (int i = 0; i < nodeIds.length; i++) {
			dataLength += networkAddresses[i].length;
		}
		
		//node ids:
		dataLength += nodeIds.length * HyCubeNodeId.getByteLength(nodeIdDimensions, nodeIdDigitsCount);
		
		return dataLength;
	}
													
						
	
	protected HyCubeRouteJoinReplyMessageData() {
		
	}
	
	public HyCubeRouteJoinReplyMessageData(int joinId, byte[] requestorNetworkAddress, byte[][] networkAddresses, HyCubeNodeId[] nodeIds, boolean finalJoinReply) {
		this.joinId = joinId;
		
		this.requestorNetworkAddress = requestorNetworkAddress;
		
		this.networkAddresses = new byte[networkAddresses.length][];
		this.nodeIds = new HyCubeNodeId[nodeIds.length];
		
		int i;
		for (i = 0; i < networkAddresses.length; i++) {
			this.networkAddresses[i] = networkAddresses[i];
		}
		for (i = 0; i < networkAddresses.length; i++) {
			this.nodeIds[i] = nodeIds[i];
		}
		
		this.finalJoinReply = finalJoinReply;
		
	}
	
	public HyCubeRouteJoinReplyMessageData(int joinId, NetworkNodePointer requestor, NodePointer[] nodePointers, boolean finalJoinReply) {
		if (nodePointers == null) {
			throw new MessageErrorRuntimeException("Cannot create a message data object instance. The input array cannot be null.");
		}
		
		this.joinId = joinId;
		
		if (requestor != null) {
			this.requestorNetworkAddress = requestor.getAddressBytes();
		}
		else {
			this.requestorNetworkAddress = null;
		}
		
		this.networkAddresses = new byte[nodePointers.length][];
		this.nodeIds = new HyCubeNodeId[nodePointers.length];
				
		int i = 0;
		for (NodePointer np : nodePointers) {
			this.networkAddresses[i] = np.getNetworkNodePointer().getAddressBytes();
			this.nodeIds[i] = (HyCubeNodeId)np.getNodeId();
			i++;
		}
		
		this.finalJoinReply = finalJoinReply;
		
	}
	
	
	protected int joinId;
	
	protected byte[] requestorNetworkAddress;
	
	protected byte[][] networkAddresses;
	protected HyCubeNodeId[] nodeIds;

	protected boolean finalJoinReply;
	
	
	public int getJoinId() {
		return joinId;
	}

	public void setJoinId(int joinId) {
		this.joinId = joinId;
	}
	
	public byte[] getRequestorNetworkAddress() {
		return requestorNetworkAddress;
	}
	
	public void setRequestorNetworkAddress(byte[] requestorNetworkAddress) {
		this.requestorNetworkAddress = requestorNetworkAddress;
	}
	
	public NetworkNodePointer getRequestorNetworkNodePointer(NetworkAdapter networkAdapter) {
		if (requestorNetworkAddress != null) {
			NetworkNodePointer pointer = networkAdapter.createNetworkNodePointer(requestorNetworkAddress);
			return pointer;
		}
		else return null;
	}
	
	public byte[][] getNetworkAddresses() {
		return networkAddresses;
	}

	public void setNetworkAddress(byte[][] networkAddresses) {
		this.networkAddresses = networkAddresses;
	}
	
	public NodeId[] nodeIds() {
		return nodeIds;
	}

	public void setNodeId(HyCubeNodeId[] nodeIds) {
		this.nodeIds = nodeIds;
	}
	
	public boolean isFinalJoinReply() {
		return finalJoinReply;
	}

	public void setFinalJoinReply(boolean finalJoinReply) {
		this.finalJoinReply = finalJoinReply;
	}
	
	
	public NodePointer[] getNodePointers(NetworkAdapter networkAdapter) {
		if (networkAddresses == null || nodeIds == null || networkAddresses.length != nodeIds.length) {
			throw new MessageErrorRuntimeException("Could not get node pointers from the message. The network addresses count should be equal to the node ids length.");
		}
		
		NodePointer[] pointers = new NodePointer[nodeIds.length];
		for (int i = 0; i < nodeIds.length; i++) {
			NodePointer pointer = new NodePointer();
			pointer.setNetworkNodePointer(networkAdapter.createNetworkNodePointer(networkAddresses[i]));
			pointer.setNodeId(nodeIds[i]);
			pointers[i] = pointer;
		}
		
		return pointers;
	}
	
	
	public byte[] getBytes(int nodeIdDimensions, int nodeIdDigitsCount) {
		
		if (networkAddresses == null) {
			throw new MessageByteConversionRuntimeException("The message data cannot be converted to a byte array. All message fields should be set.");
		}
		if (networkAddresses.length != nodeIds.length) {
			throw new MessageByteConversionRuntimeException("The message data cannot be converted to a byte array. Network addresseses count should be equal to node ids count.");
		}
		if (networkAddresses.length > Short.MAX_VALUE) {
			throw new MessageByteConversionRuntimeException("The message data cannot be converted to a byte array. The number of nodes in the message should be less than or equal to " + Short.MAX_VALUE + ".");
		}
		for (byte[] networkAddress : networkAddresses) {
			if (networkAddress.length > Short.MAX_VALUE) throw new MessageByteConversionRuntimeException("The message data cannot be converted to a byte array. Network addresses length should be less than or equal to " + Short.MAX_VALUE + ".");
		}
		if (nodeIds == null) {
			throw new MessageByteConversionRuntimeException("The message data cannot be converted to a byte array. All message fields should be set.");
		}
		
		for (HyCubeNodeId nodeId : nodeIds) { 
			if (nodeId.getDimensions() != nodeIdDimensions) throw new MessageByteConversionRuntimeException("The message data cannot be converted to a byte array. Invalid dimensions count."); 
			if (nodeId.getDigitsCount() != nodeIdDigitsCount) throw new MessageByteConversionRuntimeException("The message data cannot be converted to a byte array. Invalid digits count.");
		}
		
		
		
		
		ByteBuffer b = ByteBuffer.allocate(calculateSearchJoinReplyMessageDataLength(nodeIdDimensions, nodeIdDigitsCount));
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		
		b.putInt(joinId);					//joinId
		
		
	
		//set the options field:
		int options = 0; 
		if (finalJoinReply) {
			options = options | (1 << OPTION_BIT_NO_FINAL_JOIN_REPLY);
		}
		if (requestorNetworkAddress != null) {
			options = options | (1 << OPTION_BIT_NO_DISCOVER_PUBLIC_NETWORK_ADDRESS);
		}
		b.putInt(options);					//options
		
		
		
		//requestor network address
		if (requestorNetworkAddress != null) {
			b.put(requestorNetworkAddress);					//requestor network address
		}
		
		
		b.putShort((short) networkAddresses.length);		//nodes count
		
		
		for (int i = 0; i < networkAddresses.length; i++) {
			
			b.put(networkAddresses[i]);												//network address

			
			
			b.put(nodeIds[i].getBytes(HyCubeMessage.MESSAGE_BYTE_ORDER));							//node id
			
		}

		byte[] bytes = b.array();
		return bytes;

	}
	
	
	public static HyCubeRouteJoinReplyMessageData fromBytes(byte[] bytes, int nodeIdDimensions, int nodeIdDigitsCount, int networkAddressByteLength) throws MessageByteConversionException {
		
		HyCubeRouteJoinReplyMessageData joinReplyMsg = new HyCubeRouteJoinReplyMessageData();
		
		if (bytes == null) {
			throw new MessageByteConversionRuntimeException("Could not convert the byte array to the message object. The byte array passed to the method is null.");
		}
		
		if (networkAddressByteLength < 0) {
			throw new MessageByteConversionRuntimeException("Could not convert the byte array to the message data object. The network address length is negative.");
		}
		
		ByteBuffer b = ByteBuffer.wrap(bytes);
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		int nodeIdByteLength = HyCubeNodeId.getByteLength(nodeIdDimensions, nodeIdDigitsCount);
		byte[] nodeIdB;
		
		
		try {
			
			//joinId:
			int joinId = b.getInt();
			joinReplyMsg.setJoinId(joinId);
			
			
			
			//get options:
			int options = b.getInt();
			
			if ((options & (1 << OPTION_BIT_NO_FINAL_JOIN_REPLY)) != 0) {
				joinReplyMsg.finalJoinReply = true;
			}
			else {
				joinReplyMsg.finalJoinReply = false;
			}

			boolean joinNodeNetworkAddressSet;
			if ((options & (1 << OPTION_BIT_NO_DISCOVER_PUBLIC_NETWORK_ADDRESS)) != 0) {
				joinNodeNetworkAddressSet = true;
			}
			else {
				joinNodeNetworkAddressSet = false;
			}
			
			
			
			
			if (joinNodeNetworkAddressSet) {
				//requestor network address:
				byte[] requestorNetworkAddress;
				if (networkAddressByteLength == 0) requestorNetworkAddress = new byte[0];
				else {
					requestorNetworkAddress = new byte[networkAddressByteLength];
					b.get(requestorNetworkAddress);
				}
				joinReplyMsg.requestorNetworkAddress = requestorNetworkAddress;
			}
			else {
				joinReplyMsg.requestorNetworkAddress = null;
			}
			
			
			
			//nodes count:
			int nodesCount = b.getShort();
			if (nodesCount < 0) {
				throw new MessageByteConversionException("Could not convert the byte array to the message data object. The nodes count is negative.");
			}
			
			byte[][] networkAddresses = new byte[nodesCount][];
			HyCubeNodeId[] nodeIds = new HyCubeNodeId[nodesCount];
			
			//network addresses and node ids:
			for (int i = 0; i < nodesCount; i++) {
				byte[] networkAddress;
				HyCubeNodeId nodeId;
				
				//network address:
				if (networkAddressByteLength == 0) networkAddress = new byte[0];
				else {
					networkAddress = new byte[networkAddressByteLength];
					b.get(networkAddress);
				}
				
				
				//node id:
				nodeIdB = new byte[nodeIdByteLength];
				b.get(nodeIdB);
				try {
					nodeId = HyCubeNodeId.fromBytes(nodeIdB, nodeIdDimensions, nodeIdDigitsCount, HyCubeMessage.MESSAGE_BYTE_ORDER);
				} catch (NodeIdByteConversionException e) {
					throw new MessageByteConversionException("Error while converting a byte array to Message. Could not convert the byte representation of a node id.", e);
				}
				
				//add no tables:
				networkAddresses[i] = networkAddress;
				nodeIds[i] = nodeId;
			}
			
			joinReplyMsg.networkAddresses = networkAddresses;
			joinReplyMsg.nodeIds = nodeIds;
			
		}
		catch (BufferUnderflowException e) {
			throw new MessageByteConversionException("The length of the byte array passed to the method is not equal to the expected message data length.");
		}
		
		if (joinReplyMsg.calculateSearchJoinReplyMessageDataLength(nodeIdDimensions, nodeIdDigitsCount) != bytes.length) {
			throw new MessageByteConversionException("The length of the byte array passed to the method is not equal to the expected message data length.");
		}
		
		return joinReplyMsg;
		
	}



	
}
