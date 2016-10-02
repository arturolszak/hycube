package net.hycube.maintenance;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import net.hycube.core.HyCubeNodeId;
import net.hycube.core.NodeIdByteConversionException;
import net.hycube.core.NodePointer;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.MessageByteConversionException;
import net.hycube.messaging.messages.MessageByteConversionRuntimeException;
import net.hycube.messaging.messages.MessageErrorRuntimeException;
import net.hycube.transport.NetworkAdapter;

public class HyCubeRecoveryReplyMessageData {

	
	protected int calculateRecoveryReplyMessageDataLength(int nodeIdDimensions, int nodeIdDigitsCount) {
		int dataLength =
							+ Short.SIZE/8					//number of nodes
							;
		
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
													
						
	
	protected HyCubeRecoveryReplyMessageData() {
		
	}
	
	public HyCubeRecoveryReplyMessageData(byte[][] networkAddresses, HyCubeNodeId[] nodeIds) {
		this.networkAddresses = networkAddresses;
		this.nodeIds = nodeIds;
		
	}
	
	public HyCubeRecoveryReplyMessageData(NodePointer[] nodePointers) {
		if (nodePointers == null) {
			throw new MessageErrorRuntimeException("Cannot create a message data object instance. The input array cannot be null.");
		}
		
		this.networkAddresses = new byte[nodePointers.length][];
		this.nodeIds = new HyCubeNodeId[nodePointers.length];
				
		int i = 0;
		for (NodePointer np : nodePointers) {
			this.networkAddresses[i] = np.getNetworkNodePointer().getAddressBytes();
			this.nodeIds[i] = (HyCubeNodeId) np.getNodeId();
			i++;
		}
	}
	
	
	protected byte[][] networkAddresses;
	protected HyCubeNodeId[] nodeIds;

	
	public byte[][] getNetworkAddresses() {
		return networkAddresses;
	}

	public void setNetworkAddress(byte[][] networkAddresses) {
		this.networkAddresses = networkAddresses;
	}
	
	public HyCubeNodeId[] nodeIds() {
		return nodeIds;
	}

	public void setNodeId(HyCubeNodeId[] nodeIds) {
		this.nodeIds = nodeIds;
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
		
					
		ByteBuffer b = ByteBuffer.allocate(calculateRecoveryReplyMessageDataLength(nodeIdDimensions, nodeIdDigitsCount));
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		b.putShort((short) networkAddresses.length);		//nodes count
		for (int i = 0; i < networkAddresses.length; i++) {
			
			b.put(networkAddresses[i]);										//network address

			
			
			b.put(nodeIds[i].getBytes(HyCubeMessage.MESSAGE_BYTE_ORDER));							//node id
			
		}
				
		byte[] bytes = b.array();
		return bytes;

	}
	
	public static HyCubeRecoveryReplyMessageData fromBytes(byte[] bytes, int nodeIdDimensions, int nodeIdDigitsCount, int networkAddressByteLength) throws MessageByteConversionException {
		
		HyCubeRecoveryReplyMessageData recoveryReplyMsg = new HyCubeRecoveryReplyMessageData();
		
		if (bytes == null) {
			throw new MessageByteConversionRuntimeException("Could not convert the byte array to the message object. The byte array passed to the method is null.");
		}
		
		if (networkAddressByteLength < 0) {
			throw new MessageByteConversionRuntimeException("Could not convert the byte array to the message data object. The network address length is negative.");
		}
		
		ByteBuffer b = ByteBuffer.wrap(bytes);
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		try {
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
				int idByteLength = HyCubeNodeId.getByteLength(nodeIdDimensions, nodeIdDigitsCount);
				byte[] nodeIdB = new byte[idByteLength];
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
			recoveryReplyMsg.networkAddresses = networkAddresses;
			recoveryReplyMsg.nodeIds = nodeIds;
			
		}
		catch (BufferUnderflowException e) {
			throw new MessageByteConversionException("The length of the byte array passed to the method is not equal to the expected message data length.");
		}
		
		if (recoveryReplyMsg.calculateRecoveryReplyMessageDataLength(nodeIdDimensions, nodeIdDigitsCount) != bytes.length) {
			throw new MessageByteConversionException("The length of the byte array passed to the method is not equal to the expected message data length.");
		}
		
		return recoveryReplyMsg;
		
	}



	
}
