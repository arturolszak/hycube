package net.hycube.search;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import net.hycube.core.HyCubeNodeId;
import net.hycube.core.NodeIdByteConversionException;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.MessageByteConversionException;
import net.hycube.messaging.messages.MessageByteConversionRuntimeException;
import net.hycube.search.HyCubeSearchNextHopSelectionParameters;

public class HyCubeSearchMessageData {

//	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
//	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(SearchMessageData.class); 
	
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
	
	
	protected int calculateSearchMessageDataLength(int nodeIdDimensions, int nodeIdDigitsCount) {
		int dataLength =
							+ Integer.SIZE/8					//search id
							+ BOOLEAN_OPTIONS_BYTES
							+ HyCubeNodeId.getByteLength(nodeIdDimensions, nodeIdDigitsCount)	//search node id
							+ (parameters.isSteinhausTransformApplied() ? HyCubeNodeId.getByteLength(nodeIdDimensions, nodeIdDigitsCount) : 0)	//steinhaus point
							+ Short.SIZE / 8		//beta
							//+ Short.SIZE / 8		//gamma
							;
	

		return dataLength;
		
	}
													
						
	
	protected HyCubeSearchMessageData() {
		
	}
	
	public HyCubeSearchMessageData(int searchId, HyCubeNodeId searchNodeId, HyCubeSearchNextHopSelectionParameters parameters) {
		this.searchId = searchId;
		this.searchNodeId = searchNodeId;
		this.parameters = parameters;
	
	}
	

	protected int searchId;
	protected HyCubeNodeId searchNodeId;
	protected HyCubeSearchNextHopSelectionParameters parameters;

	
	public int getSearchId() {
		return searchId;
	}

	public void setSearchId(int searchId) {
		this.searchId = searchId;
	}
	
	public HyCubeNodeId getSearchNodeId() {
		return searchNodeId;
	}

	public void setSearchNodeId(HyCubeNodeId searchNodeId) {
		this.searchNodeId = searchNodeId;
	}
	
	public HyCubeSearchNextHopSelectionParameters getParameters() {
		return parameters;
	}

	public void setParameters(HyCubeSearchNextHopSelectionParameters parameters) {
		this.parameters = parameters;
	}
	
	
	
	public byte[] getBytes(int nodeIdDimensions, int nodeIdDigitsCount) {
		if (parameters == null) {
			throw new MessageByteConversionRuntimeException("The message data cannot be converted to a byte array. Search parameters not set.");
		}
					
		ByteBuffer b = ByteBuffer.allocate(calculateSearchMessageDataLength(nodeIdDimensions, nodeIdDigitsCount));
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		b.putInt(searchId);					//searchId
		
		
		if (searchNodeId.getDimensions() != nodeIdDimensions) throw new MessageByteConversionRuntimeException("The message data cannot be converted to a byte array. Invalid dimensions count."); 
		if (searchNodeId.getDigitsCount() != nodeIdDigitsCount) throw new MessageByteConversionRuntimeException("The message data cannot be converted to a byte array. Invalid digits count.");
		b.put(searchNodeId.getBytes(HyCubeMessage.MESSAGE_BYTE_ORDER));		//search node id
		
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
			b.put(steihausPoint.getBytes(HyCubeMessage.MESSAGE_BYTE_ORDER));
		}
			
		b.putShort(parameters.getBeta());	//beta
		
		//b.putShort(parameters.getGamma());	//gamma
		
		

		byte[] bytes = b.array();
		return bytes;

	}
	
	public static HyCubeSearchMessageData fromBytes(byte[] bytes, int nodeIdDimensions, int nodeIdDigitsCount) throws MessageByteConversionException {
		
		HyCubeSearchMessageData searchMsg = new HyCubeSearchMessageData();
		HyCubeSearchNextHopSelectionParameters parameters = new HyCubeSearchNextHopSelectionParameters();
		
		if (bytes == null) {
			throw new MessageByteConversionRuntimeException("Could not convert the byte array to the message object. The byte array passed to the method is null.");
		}
		
		ByteBuffer b = ByteBuffer.wrap(bytes);
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		int idByteLength = HyCubeNodeId.getByteLength(nodeIdDimensions, nodeIdDigitsCount);
		byte[] nodeIdB;
		
		
		try {
			
			//searchId:
			int searchId = b.getInt();
			searchMsg.setSearchId(searchId);
			
			
			
			
			
			
			//search node id:
			HyCubeNodeId searchNodeId;
			nodeIdB = new byte[idByteLength];
			b.get(nodeIdB);
			try {
				searchNodeId = HyCubeNodeId.fromBytes(nodeIdB, nodeIdDimensions, nodeIdDigitsCount, HyCubeMessage.MESSAGE_BYTE_ORDER);
			} catch (NodeIdByteConversionException e) {
				throw new MessageByteConversionException("Error while converting a byte array to Message. Could not convert the byte representation of a node id.", e);
			}
			searchMsg.setSearchNodeId(searchNodeId);
			
			
			
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
			searchMsg.parameters = parameters;
			
			
		}
		catch (BufferUnderflowException e) {
			throw new MessageByteConversionException("The length of the byte array passed to the method is not equal to the expected message data length.");
		}
		
		if (searchMsg.calculateSearchMessageDataLength(nodeIdDimensions, nodeIdDigitsCount) != bytes.length) {
			throw new MessageByteConversionException("The length of the byte array passed to the method is not equal to the expected message data length.");
		}
		
		return searchMsg;
		
	}



	
}
