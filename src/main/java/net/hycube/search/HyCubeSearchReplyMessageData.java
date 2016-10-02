package net.hycube.search;

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

public class HyCubeSearchReplyMessageData {

	
	protected int BOOLEAN_OPTIONS_BYTES = 
			Integer.SIZE / 8		//boolean options
			;
	
	protected static final int OPTION_BIT_NO_STEINHAUS_TRANSFORM_APPLIED = 0;
	protected static final int OPTION_BIT_NO_PMH_APPLIED = 1;
	protected static final int OPTION_BIT_NO_PREVENT_PMH = 2;
	protected static final int OPTION_BIT_NO_INCLUDE_MORE_DISTANT_NODES = 3;
	protected static final int OPTION_BIT_NO_SKIP_TARGET_NODE = 4;
	protected static final int OPTION_BIT_NO_SKIP_RANDOM_NUMBER_OF_NODES_APPLIED = 5;
	protected static final int OPTION_BIT_NO_SECURE_ROUTING_APPLIED = 6;
	protected static final int OPTION_BIT_NO_FINAL_SEARCH = 7;
	
	
	protected int calculateSearchReplyMessageDataLength(int nodeIdDimensions, int nodeIdDigitsCount) {
		int dataLength =
				+ Integer.SIZE/8					//search id
				+ BOOLEAN_OPTIONS_BYTES
				+ (parameters.isSteinhausTransformApplied() ? HyCubeNodeId.getByteLength(nodeIdDimensions, nodeIdDigitsCount) : 0)	//steinhaus point
				+ Short.SIZE / 8		//beta
				//+ Short.SIZE / 8		//gamma
				+ Short.SIZE/ 8			//number of nodes
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
													
						
	
	protected HyCubeSearchReplyMessageData() {
		
	}
	
	public HyCubeSearchReplyMessageData(int searchId, HyCubeSearchNextHopSelectionParameters parameters, byte[][] networkAddresses, HyCubeNodeId[] nodeIds) {
		this.searchId = searchId;
		this.parameters = parameters;
		
		this.networkAddresses = new byte[networkAddresses.length][];
		this.nodeIds = new HyCubeNodeId[nodeIds.length];
		
		int i;
		for (i = 0; i < networkAddresses.length; i++) {
			this.networkAddresses[i] = networkAddresses[i];
		}
		for (i = 0; i < networkAddresses.length; i++) {
			this.nodeIds[i] = nodeIds[i];
		}
		
		
	}
	
	public HyCubeSearchReplyMessageData(int searchId, HyCubeSearchNextHopSelectionParameters parameters, NodePointer[] nodePointers) {
		if (nodePointers == null) {
			throw new MessageErrorRuntimeException("Cannot create a message data object instance. The input array cannot be null.");
		}
		
		this.searchId = searchId;
		this.parameters = parameters;
		
		this.networkAddresses = new byte[nodePointers.length][];
		this.nodeIds = new HyCubeNodeId[nodePointers.length];
				
		int i = 0;
		for (NodePointer np : nodePointers) {
			this.networkAddresses[i] = np.getNetworkNodePointer().getAddressBytes();
			this.nodeIds[i] = (HyCubeNodeId)np.getNodeId();
			i++;
		}
	}
	
	
	protected int searchId;
	protected HyCubeSearchNextHopSelectionParameters parameters;
	
	protected byte[][] networkAddresses;
	protected HyCubeNodeId[] nodeIds;

	
	public int getSearchId() {
		return searchId;
	}

	public void setSearchId(int searchId) {
		this.searchId = searchId;
	}
	
	public HyCubeSearchNextHopSelectionParameters getParameters() {
		return parameters;
	}

	public void setParameters(HyCubeSearchNextHopSelectionParameters parameters) {
		this.parameters = parameters;
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
		
		
		
		
		ByteBuffer b = ByteBuffer.allocate(calculateSearchReplyMessageDataLength(nodeIdDimensions, nodeIdDigitsCount));
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		
		b.putInt(searchId);					//searchId
		
		//set the options field:
		int options = 0; 
		if (parameters.isSteinhausTransformApplied()) {
			options = options | (1 << OPTION_BIT_NO_STEINHAUS_TRANSFORM_APPLIED);
		}
		if (parameters.isPMHApplied()) {
			options = options | (1 << OPTION_BIT_NO_PMH_APPLIED);
		}
		if (parameters.isPreventPMH()) {
			options = options | (1 << OPTION_BIT_NO_PREVENT_PMH);
		}
		if (parameters.isIncludeMoreDistantNodes()) {
			options = options | (1 << OPTION_BIT_NO_INCLUDE_MORE_DISTANT_NODES);
		}
		if (parameters.isSkipTargetNode()) {
			options = options | (1 << OPTION_BIT_NO_SKIP_TARGET_NODE);
		}
		if (parameters.isSkipRandomNumOfNodesApplied()) {
			options = options | (1 << OPTION_BIT_NO_SKIP_RANDOM_NUMBER_OF_NODES_APPLIED);
		}
		if (parameters.isSecureRoutingApplied()) {
			options = options | (1 << OPTION_BIT_NO_SECURE_ROUTING_APPLIED);
		}
		if (parameters.isFinalSearch()) {
			options = options | (1 << OPTION_BIT_NO_FINAL_SEARCH);
		}
		b.putInt(options);					//options
		
		
		
		if (parameters.isSteinhausTransformApplied()) {		//steinhaus point - only if steinhaus transform applied
			HyCubeNodeId steihausPoint = parameters.getSteinhausPoint();
			if (steihausPoint.getDimensions() != nodeIdDimensions) throw new MessageByteConversionRuntimeException("The message data cannot be converted to a byte array. Invalid dimensions count."); 
			if (steihausPoint.getDigitsCount() != nodeIdDigitsCount) throw new MessageByteConversionRuntimeException("The message data cannot be converted to a byte array. Invalid digits count.");
			b.put(steihausPoint.getBytes(HyCubeMessage.MESSAGE_BYTE_ORDER));		//steinhaus point
		}
		
		
		b.putShort(parameters.getBeta());	//beta
		//b.putShort(parameters.getGamma());	//gamma

		
		b.putShort((short) networkAddresses.length);		//nodes count
		
		
		for (int i = 0; i < networkAddresses.length; i++) {
			
			b.put(networkAddresses[i]);								//network address

			
			
			b.put(nodeIds[i].getBytes(HyCubeMessage.MESSAGE_BYTE_ORDER));							//node id
			
		}
				
		byte[] bytes = b.array();
		return bytes;

	}
	
	
	public static HyCubeSearchReplyMessageData fromBytes(byte[] bytes, int nodeIdDimensions, int nodeIdDigitsCount, int networkAddressByteLength) throws MessageByteConversionException {
		
		HyCubeSearchReplyMessageData searchReplyMsg = new HyCubeSearchReplyMessageData();
		HyCubeSearchNextHopSelectionParameters parameters = new HyCubeSearchNextHopSelectionParameters();
		
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
			
			//searchId:
			int searchId = b.getInt();
			searchReplyMsg.setSearchId(searchId);
			
			
			//get options:
			int options = b.getInt();
			
			if ((options & (1 << OPTION_BIT_NO_STEINHAUS_TRANSFORM_APPLIED)) != 0) {
				parameters.setSteinhausTransformApplied(true);
			}
			else {
				parameters.setSteinhausTransformApplied(false);
			}
			
			if ((options & (1 << OPTION_BIT_NO_PMH_APPLIED)) != 0) {
				parameters.setPMHApplied(true);
			}
			else {
				parameters.setPMHApplied(false);
			}
			
			if ((options & (1 << OPTION_BIT_NO_PREVENT_PMH)) != 0) {
				parameters.setPreventPMH(true);
			}
			else {
				parameters.setPreventPMH(false);
			}
			
			if ((options & (1 << OPTION_BIT_NO_INCLUDE_MORE_DISTANT_NODES)) != 0) {
				parameters.setIncludeMoreDistantNodes(true);
			}
			else {
				parameters.setIncludeMoreDistantNodes(false);
			}
			
			if ((options & (1 << OPTION_BIT_NO_SKIP_TARGET_NODE)) != 0) {
				parameters.setSkipTargetNode(true);
			}
			else {
				parameters.setSkipTargetNode(false);
			}
			
			if ((options & (1 << OPTION_BIT_NO_SKIP_RANDOM_NUMBER_OF_NODES_APPLIED)) != 0) {
				parameters.setSkipRandomNumOfNodesApplied(true);
			}
			else {
				parameters.setSkipRandomNumOfNodesApplied(false);
			}
			
			if ((options & (1 << OPTION_BIT_NO_SECURE_ROUTING_APPLIED)) != 0) {
				parameters.setSecureRoutingApplied(true);
			}
			else {
				parameters.setSecureRoutingApplied(false);
			}
			
			if ((options & (1 << OPTION_BIT_NO_FINAL_SEARCH)) != 0) {
				parameters.setFinalSearch(true);
			}
			else {
				parameters.setFinalSearch(false);
			}
			
			
			if (parameters.isSteinhausTransformApplied()) {	//steinhaus point - only when the option "steinhaus point applied" is set
				//steinhaus point:
				HyCubeNodeId steinhausPoint;
				nodeIdB = new byte[nodeIdByteLength];
				b.get(nodeIdB);
				try {
					steinhausPoint = HyCubeNodeId.fromBytes(nodeIdB, nodeIdDimensions, nodeIdDigitsCount, HyCubeMessage.MESSAGE_BYTE_ORDER);
				} catch (NodeIdByteConversionException e) {
					throw new MessageByteConversionException("Error while converting a byte array to Message. Could not convert the byte representation of a node id.", e);
				}
				parameters.setSteinhausPoint(steinhausPoint);
			}
			else parameters.setSteinhausPoint(null);
			

			//beta:
			short beta = b.getShort();
			
			//gamma:
			//short gamma = b.getShort();
			
			parameters.setBeta(beta);
			//parameters.setGamma(gamma);
			
			
			//set the parameters field of the message:
			searchReplyMsg.parameters = parameters;
			
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
			
			searchReplyMsg.networkAddresses = networkAddresses;
			searchReplyMsg.nodeIds = nodeIds;
			
		}
		catch (BufferUnderflowException e) {
			throw new MessageByteConversionException("The length of the byte array passed to the method is not equal to the expected message data length.");
		}
		
		if (searchReplyMsg.calculateSearchReplyMessageDataLength(nodeIdDimensions, nodeIdDigitsCount) != bytes.length) {
			throw new MessageByteConversionException("The length of the byte array passed to the method is not equal to the expected message data length.");
		}
		
		return searchReplyMsg;
		
	}



	
}
