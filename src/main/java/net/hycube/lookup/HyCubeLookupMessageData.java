package net.hycube.lookup;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import net.hycube.core.HyCubeNodeId;
import net.hycube.core.NodeIdByteConversionException;
import net.hycube.lookup.HyCubeLookupNextHopSelectionParameters;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.MessageByteConversionException;
import net.hycube.messaging.messages.MessageByteConversionRuntimeException;

public class HyCubeLookupMessageData {

//	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
//	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(LookupMessageData.class); 
	
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
	protected static final int OPTION_BIT_NO_FINAL_LOOKUP = 7;
	
	
	protected int calculateLookupMessageDataLength(int nodeIdDimensions, int nodeIdDigitsCount) {
		int dataLength =
							+ Integer.SIZE/8					//lookup id
							+ BOOLEAN_OPTIONS_BYTES
							+ HyCubeNodeId.getByteLength(nodeIdDimensions, nodeIdDigitsCount)	//lookup node id
							+ (parameters.isSteinhausTransformApplied() ? HyCubeNodeId.getByteLength(nodeIdDimensions, nodeIdDigitsCount) : 0)	//steinhaus point
							+ Short.SIZE / 8		//beta
							//+ Short.SIZE / 8		//gamma
							;
	

		return dataLength;
		
	}
													
						
	
	protected HyCubeLookupMessageData() {
		
	}
	
	public HyCubeLookupMessageData(int lookupId, HyCubeNodeId lookupNodeId, HyCubeLookupNextHopSelectionParameters parameters) {
		this.lookupId = lookupId;
		this.lookupNodeId = lookupNodeId;
		this.parameters = parameters;
	
	}
	

	protected int lookupId;
	protected HyCubeNodeId lookupNodeId;
	protected HyCubeLookupNextHopSelectionParameters parameters;

	
	public int getLookupId() {
		return lookupId;
	}

	public void setLookupId(int lookupId) {
		this.lookupId = lookupId;
	}
	
	public HyCubeNodeId getLookupNodeId() {
		return lookupNodeId;
	}

	public void setLookupNodeId(HyCubeNodeId lookupNodeId) {
		this.lookupNodeId = lookupNodeId;
	}
	
	public HyCubeLookupNextHopSelectionParameters getParameters() {
		return parameters;
	}

	public void setParameters(HyCubeLookupNextHopSelectionParameters parameters) {
		this.parameters = parameters;
	}
	
	
	
	public byte[] getBytes(int nodeIdDimensions, int nodeIdDigitsCount) {
		if (parameters == null) {
			throw new MessageByteConversionRuntimeException("The message data cannot be converted to a byte array. Lookup parameters not set.");
		}
					
		ByteBuffer b = ByteBuffer.allocate(calculateLookupMessageDataLength(nodeIdDimensions, nodeIdDigitsCount));
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		b.putInt(lookupId);					//lookupId
		
		
		if (lookupNodeId.getDimensions() != nodeIdDimensions) throw new MessageByteConversionRuntimeException("The message data cannot be converted to a byte array. Invalid dimensions count."); 
		if (lookupNodeId.getDigitsCount() != nodeIdDigitsCount) throw new MessageByteConversionRuntimeException("The message data cannot be converted to a byte array. Invalid digits count.");
		b.put(lookupNodeId.getBytes(HyCubeMessage.MESSAGE_BYTE_ORDER));		//lookup node id
		
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
		if (parameters.isFinalLookup()) {
			options = options | (1 << OPTION_BIT_NO_FINAL_LOOKUP);
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
	
	public static HyCubeLookupMessageData fromBytes(byte[] bytes, int nodeIdDimensions, int nodeIdDigitsCount) throws MessageByteConversionException {
		
		HyCubeLookupMessageData lookupMsg = new HyCubeLookupMessageData();
		HyCubeLookupNextHopSelectionParameters parameters = new HyCubeLookupNextHopSelectionParameters();
		
		if (bytes == null) {
			throw new MessageByteConversionRuntimeException("Could not convert the byte array to the message object. The byte array passed to the method is null.");
		}
		
		ByteBuffer b = ByteBuffer.wrap(bytes);
		b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
		
		
		int idByteLength = HyCubeNodeId.getByteLength(nodeIdDimensions, nodeIdDigitsCount);
		byte[] nodeIdB;
		
		
		try {
			
			//lookupId:
			int lookupId = b.getInt();
			lookupMsg.setLookupId(lookupId);
			
			
			
			
			
			
			//lookup node id:
			HyCubeNodeId lookupNodeId;
			nodeIdB = new byte[idByteLength];
			b.get(nodeIdB);
			try {
				lookupNodeId = HyCubeNodeId.fromBytes(nodeIdB, nodeIdDimensions, nodeIdDigitsCount, HyCubeMessage.MESSAGE_BYTE_ORDER);
			} catch (NodeIdByteConversionException e) {
				throw new MessageByteConversionException("Error while converting a byte array to Message. Could not convert the byte representation of a node id.", e);
			}
			lookupMsg.setLookupNodeId(lookupNodeId);
			
			
			
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
			
			if ((options & (1 << OPTION_BIT_NO_FINAL_LOOKUP)) != 0) {
				parameters.setFinalLookup(true);
			}
			else {
				parameters.setFinalLookup(false);
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
			

			//beta
			short beta = b.getShort();
			
			//gamma:
			//short gamma = b.getShort();
			
			
			parameters.setBeta(beta);
			//parameters.setGamma(gamma);
			
			
			//set the parameters field of the message:
			lookupMsg.parameters = parameters;
			
			
		}
		catch (BufferUnderflowException e) {
			throw new MessageByteConversionException("The length of the byte array passed to the method is not equal to the expected message data length.");
		}
		
		if (lookupMsg.calculateLookupMessageDataLength(nodeIdDimensions, nodeIdDigitsCount) != bytes.length) {
			throw new MessageByteConversionException("The length of the byte array passed to the method is not equal to the expected message data length.");
		}
		
		return lookupMsg;
		
	}



	
}
