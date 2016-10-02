package net.hycube.join.searchjoin;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import net.hycube.core.HyCubeNodeId;
import net.hycube.core.NodeIdByteConversionException;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.MessageByteConversionException;
import net.hycube.messaging.messages.MessageByteConversionRuntimeException;

public class HyCubeSearchJoinMessageData {

//	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
//	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(JoinMessageData.class); 
	
	protected int BOOLEAN_OPTIONS_BYTES = 
			Integer.SIZE / 8		//boolean options
			;
	
	protected static final int OPTION_BIT_NO_STEINHAUS_TRANSFORM_APPLIED = 0;
	protected static final int OPTION_BIT_NO_STEINHAUS_POINT_SET = 1;
	protected static final int OPTION_BIT_NO_PMH_APPLIED = 2;
	protected static final int OPTION_BIT_NO_PREVENT_PMH = 3;
	protected static final int OPTION_BIT_NO_INCLUDE_MORE_DISTANT_NODES = 4;
	protected static final int OPTION_BIT_NO_SKIP_TARGET_NODE = 5;
	protected static final int OPTION_BIT_NO_SKIP_RANDOM_NUMBER_OF_NODES_APPLIED = 6;
	protected static final int OPTION_BIT_NO_SECURE_ROUTING_APPLIED = 7;
	protected static final int OPTION_BIT_NO_INITIAL_REQUEST = 8;
	protected static final int OPTION_BIT_NO_FINAL_SEARCH = 9;
	protected static final int OPTION_BIT_NO_DISCOVER_PUBLIC_NETWORK_ADDRESS = 10;
	
	
	protected int calculateSearchJoinMessageDataLength(int nodeIdDimensions, int nodeIdDigitsCount) {
		int dataLength =
							+ Integer.SIZE/8					//join id
							+ BOOLEAN_OPTIONS_BYTES
							+ HyCubeNodeId.getByteLength(nodeIdDimensions, nodeIdDigitsCount)	//join node id
							+ (parameters.getSteinhausPoint()!=null ? HyCubeNodeId.getByteLength(nodeIdDimensions, nodeIdDigitsCount) : 0)	//steinhaus point
							+ Short.SIZE / 8		//beta
							//+ Short.SIZE / 8		//gamma
							;
	

		return dataLength;
		
	}
													
						
	
	protected HyCubeSearchJoinMessageData() {
		
	}
	
	public HyCubeSearchJoinMessageData(int joinId, HyCubeNodeId joinNodeId, HyCubeSearchJoinNextHopSelectionParameters parameters, boolean discoverPublicNetworkAddress) {
		this.joinId = joinId;
		this.joinNodeId = joinNodeId;
		this.parameters = parameters;
		this.discoverPublicNetworkAddress = discoverPublicNetworkAddress;
	
	}
	

	protected int joinId;
	protected HyCubeNodeId joinNodeId;
	protected HyCubeSearchJoinNextHopSelectionParameters parameters;
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
	
	public HyCubeSearchJoinNextHopSelectionParameters getParameters() {
		return parameters;
	}

	public void setParameters(HyCubeSearchJoinNextHopSelectionParameters parameters) {
		this.parameters = parameters;
	}
	
	public boolean getDiscoverPublicNetworkAddress() {
		return discoverPublicNetworkAddress;
	}

	public void setDiscoverPublicNetworkAddress(boolean discoverPublicNetworkAddress) {
		this.discoverPublicNetworkAddress = discoverPublicNetworkAddress;
	}
	
	
	
	public byte[] getBytes(int nodeIdDimensions, int nodeIdDigitsCount) {
		if (parameters == null) {
			throw new MessageByteConversionRuntimeException("The message data cannot be converted to a byte array. Search parameters not set.");
		}
					
		ByteBuffer b = ByteBuffer.allocate(calculateSearchJoinMessageDataLength(nodeIdDimensions, nodeIdDigitsCount));
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		b.putInt(joinId);					//joinId
		
		
		if (joinNodeId.getDimensions() != nodeIdDimensions) throw new MessageByteConversionRuntimeException("The message data cannot be converted to a byte array. Invalid dimensions count."); 
		if (joinNodeId.getDigitsCount() != nodeIdDigitsCount) throw new MessageByteConversionRuntimeException("The message data cannot be converted to a byte array. Invalid digits count.");
		b.put(joinNodeId.getBytes(HyCubeMessage.MESSAGE_BYTE_ORDER));		//join node id
		
		//set the options field:
		int options = 0; 
		if (parameters.isSteinhausTransformApplied()) {
			options = options | (1 << OPTION_BIT_NO_STEINHAUS_TRANSFORM_APPLIED);
		}
		if (parameters.getSteinhausPoint() != null) {
			options = options | (1 << OPTION_BIT_NO_STEINHAUS_POINT_SET);
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
		if (parameters.isInitialRequest()) {
			options = options | (1 << OPTION_BIT_NO_INITIAL_REQUEST);
		}
		if (parameters.isFinalSearch()) {
			options = options | (1 << OPTION_BIT_NO_FINAL_SEARCH);
		}
		if (discoverPublicNetworkAddress) {
			options = options | (1 << OPTION_BIT_NO_DISCOVER_PUBLIC_NETWORK_ADDRESS);
		}
		b.putInt(options);					//options
		
		if (parameters.getSteinhausPoint() != null) {		//steinhaus point - only if steinhaus point != null, OPTION_BIT_NO_STEINHAUS_POINT_SET flag informs whether it is null or not
			HyCubeNodeId steihausPoint = parameters.getSteinhausPoint();
			if (steihausPoint.getDimensions() != nodeIdDimensions) throw new MessageByteConversionRuntimeException("The message data cannot be converted to a byte array. Invalid dimensions count."); 
			if (steihausPoint.getDigitsCount() != nodeIdDigitsCount) throw new MessageByteConversionRuntimeException("The message data cannot be converted to a byte array. Invalid digits count.");
			b.put(steihausPoint.getBytes(HyCubeMessage.MESSAGE_BYTE_ORDER));
		}
			
		b.putShort(parameters.getBeta());	//beta
		
		//b.putShort(parameters.getGamma());	//gamma
		
		

		byte[] bytes = b.array();
		return bytes;

	}
	
	public static HyCubeSearchJoinMessageData fromBytes(byte[] bytes, int nodeIdDimensions, int nodeIdDigitsCount) throws MessageByteConversionException {
		
		HyCubeSearchJoinMessageData joinMsg = new HyCubeSearchJoinMessageData();
		HyCubeSearchJoinNextHopSelectionParameters parameters = new HyCubeSearchJoinNextHopSelectionParameters();
		
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
			
			if ((options & (1 << OPTION_BIT_NO_STEINHAUS_TRANSFORM_APPLIED)) != 0) {
				parameters.setSteinhausTransformApplied(true);
			}
			else {
				parameters.setSteinhausTransformApplied(false);
			}
			
			boolean steinhausPointSet;
			if ((options & (1 << OPTION_BIT_NO_STEINHAUS_POINT_SET)) != 0) {
				steinhausPointSet = true;
			}
			else {
				steinhausPointSet = false;
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
			
			if ((options & (1 << OPTION_BIT_NO_INITIAL_REQUEST)) != 0) {
				parameters.setInitialRequest(true);
			}
			else {
				parameters.setInitialRequest(false);
			}
			
			if ((options & (1 << OPTION_BIT_NO_FINAL_SEARCH)) != 0) {
				parameters.setFinalSearch(true);
			}
			else {
				parameters.setFinalSearch(false);
			}
			
			boolean discoverPublicNetworkAddress;
			if ((options & (1 << OPTION_BIT_NO_DISCOVER_PUBLIC_NETWORK_ADDRESS)) != 0) {
				discoverPublicNetworkAddress = true;
			}
			else {
				discoverPublicNetworkAddress = false;
			}
			joinMsg.setDiscoverPublicNetworkAddress(discoverPublicNetworkAddress);
			
			
			
			if (steinhausPointSet) {	//steinhaus point - only when the option "steinhaus point set" is set to true
				//steinhaus point:
				HyCubeNodeId steinhausPoint;
				nodeIdB = new byte[idByteLength];
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
			joinMsg.parameters = parameters;
			
			
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
