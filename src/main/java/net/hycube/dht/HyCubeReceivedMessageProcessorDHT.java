package net.hycube.dht;

import java.util.List;

import net.hycube.core.HyCubeNodeIdFactory;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodeIdFactory;
import net.hycube.core.NodePointer;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.environment.NodeProperties;
import net.hycube.logging.LogHelper;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.HyCubeMessageType;
import net.hycube.messaging.messages.Message;
import net.hycube.messaging.messages.MessageByteConversionException;
import net.hycube.messaging.processing.ProcessMessageException;
import net.hycube.messaging.processing.ReceivedMessageProcessor;
import net.hycube.transport.NetworkNodePointer;

public class HyCubeReceivedMessageProcessorDHT implements ReceivedMessageProcessor {

	//private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeReceivedMessageProcessorDHT.class); 
	
	
	protected static final String PROP_KEY_MESSAGE_TYPES = "MessageTypes";
	
	protected NodeAccessor nodeAccessor;
	protected NodeProperties properties;
	
	protected List<Enum<?>> messageTypes;
	
	
	protected HyCubeDHTManager dhtManager;
	
	protected int nodeIdDimensions;
	protected int nodeIdDigitsCount;
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
	
		if (devLog.isTraceEnabled()) {
			devLog.trace("Initializing HyCubeReceivedMessageProcessorDHT.");
		}
		
		this.nodeAccessor = nodeAccessor;
		this.properties = properties;
		
		
		try {
			
			//load message types processed by this message processor:
			this.messageTypes = properties.getEnumListProperty(PROP_KEY_MESSAGE_TYPES, HyCubeMessageType.class);
			if (this.messageTypes == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES) + ".");
			
			if (nodeAccessor.getDHTManager() instanceof HyCubeDHTManager) {
				this.dhtManager = (HyCubeDHTManager) nodeAccessor.getDHTManager();
			}
			else {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize received message processor instance. The DHT manager is expected to be an instance of: " + HyCubeDHTManager.class.getName() + ".");
			}
			
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize received message processor instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		NodeIdFactory nodeIdFactory = nodeAccessor.getNodeIdFactory();
		if (nodeIdFactory instanceof HyCubeNodeIdFactory) {
			nodeIdDimensions = ((HyCubeNodeIdFactory)(nodeAccessor.getNodeIdFactory())).getDimensions();
			nodeIdDigitsCount = ((HyCubeNodeIdFactory)(nodeAccessor.getNodeIdFactory())).getDigitsCount();
		}
		else {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize received message processor instance. node id factory should be an instance of: " + HyCubeNodeIdFactory.class.getName() + ".");
		}
		
		
	}
	
	
	@Override
	public boolean processMessage(Message message, NetworkNodePointer directSender) throws ProcessMessageException {
		
		HyCubeMessage msg = (HyCubeMessage)message;
		
		if (! messageTypes.contains(msg.getType())) return true;
		
		if (devLog.isTraceEnabled()) {
			devLog.trace("Message #" + msg.getSerialNoAndSenderString() + " received. Processing.");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Message #" + msg.getSerialNoAndSenderString() + " received. Processing.");
		}
		
		try {
			
//			if (msg.getRecipientId().equals(nodeAccessor.getNodeId())) {
				switch (msg.getType()) {
					case PUT:
						processPutMessage(msg);
						break;
					case PUT_REPLY:
						processPutReplyMessage(msg);
						break;
					case REFRESH_PUT:
						processRefreshPutMessage(msg);
						break;
					case REFRESH_PUT_REPLY:
						processRefreshPutReplyMessage(msg);
						break;
					case GET:
						processGetMessage(msg);
						break;
					case GET_REPLY:
						processGetReplyMessage(msg);
						break;
					case DELETE:
						processDeleteMessage(msg);
						break;
					case DELETE_REPLY:
						processDeleteReplyMessage(msg);
						break;
					case REPLICATE:
						processReplicateMessage(msg);
						
					default:
						break;
				}
//			}
//			else {
//				if (devLog.isDebugEnabled()) {
//					devLog.debug("Skipping message #" + msg.getSerialNoAndSenderString() + ". Node id different from recipient node id.");
//				}
//				if (msgLog.isInfoEnabled()) {
//					msgLog.info("Skipping message #" + msg.getSerialNoAndSenderString() + ". Node id different from recipient node id.");
//				}
//				
//			}
		}
		catch (Exception e) {
			throw new ProcessMessageException("An exception thrown while processing a message.", e);
		}

		
		return true;
		
	}

	

	protected void processPutMessage(HyCubeMessage msg) throws ProcessMessageException {

		if (devLog.isTraceEnabled()) {
			devLog.trace("Processing PUT message #" + msg.getSerialNoAndSenderString() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing PUT message #" + msg.getSerialNoAndSenderString() + ".");
		}
		
		HyCubePutMessageData msgData = null;
		try {
			msgData = HyCubePutMessageData.fromBytes(msg.getData());
		} catch (MessageByteConversionException e) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("PUT message #" + msg.getSerialNoAndSenderString() + " is corrupted.", e);
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("PUT message #" + msg.getSerialNoAndSenderString() + "is corrupted.");
			}
			return;
		}
		
		NodePointer sender = new NodePointer(nodeAccessor.getNetworkAdapter(), msg.getSenderNetworkAddress(), msg.getSenderId());
		
		this.dhtManager.processPutRequest(sender, msg, msgData.getCommandId(), msgData.getKey(), msgData.getResourceDescriptorString(), msgData.getResourceData(), msgData.getRefreshTime());
		
		
	}
	
	
	protected void processPutReplyMessage(HyCubeMessage msg) throws ProcessMessageException {

		if (devLog.isTraceEnabled()) {
			devLog.trace("Processing PUT_REPLY message #" + msg.getSerialNoAndSenderString() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing PUT_REPLY message #" + msg.getSerialNoAndSenderString() + ".");
		}
		
		HyCubePutReplyMessageData msgData = null;
		try {
			msgData = HyCubePutReplyMessageData.fromBytes(msg.getData());
		} catch (MessageByteConversionException e) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("PUT_REPLY message #" + msg.getSerialNoAndSenderString() + " is corrupted.", e);
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("PUT_REPLY message #" + msg.getSerialNoAndSenderString() + "is corrupted.");
			}
			return;
		}
		
		NodePointer sender = new NodePointer(nodeAccessor.getNetworkAdapter(), msg.getSenderNetworkAddress(), msg.getSenderId());
		
		this.dhtManager.processPutResponse(sender, msg, msgData.getCommandId(), msgData.getPutStatus());
	
		
	}
	
	protected void processGetMessage(HyCubeMessage msg) throws ProcessMessageException {
		
		if (devLog.isTraceEnabled()) {
			devLog.trace("Processing GET message #" + msg.getSerialNoAndSenderString() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing GET message #" + msg.getSerialNoAndSenderString() + ".");
		}
		
		HyCubeGetMessageData msgData = null;
		try {
			msgData = HyCubeGetMessageData.fromBytes(msg.getData());
		} catch (MessageByteConversionException e) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("GET message #" + msg.getSerialNoAndSenderString() + " is corrupted.", e);
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("GET message #" + msg.getSerialNoAndSenderString() + "is corrupted.");
			}
			return;
		}
		
		NodePointer sender = new NodePointer(nodeAccessor.getNetworkAdapter(), msg.getSenderNetworkAddress(), msg.getSenderId());
		
		this.dhtManager.processGetRequest(sender, msg, msgData.getCommandId(), msgData.getKey(), msgData.getCriteriaString(), msgData.isGetFromClosestNode());
		
		
	}
	
	protected void processGetReplyMessage(HyCubeMessage msg) throws ProcessMessageException {
		
		if (devLog.isTraceEnabled()) {
			devLog.trace("Processing GET_REPLY message #" + msg.getSerialNoAndSenderString() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing GET_REPLY message #" + msg.getSerialNoAndSenderString() + ".");
		}
		
		HyCubeGetReplyMessageData msgData = null;
		try {
			msgData = HyCubeGetReplyMessageData.fromBytes(msg.getData());
		} catch (MessageByteConversionException e) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("GET_REPLY message #" + msg.getSerialNoAndSenderString() + " is corrupted.", e);
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("GET_REPLY message #" + msg.getSerialNoAndSenderString() + "is corrupted.");
			}
			return;
		}
		
		NodePointer sender = new NodePointer(nodeAccessor.getNetworkAdapter(), msg.getSenderNetworkAddress(), msg.getSenderId());
		
		this.dhtManager.processGetResponse(sender, msg, msgData.getCommandId(), msgData.getResourceDescriptorStrings(), msgData.getResourcesData());
		
		
	}
	
	protected void processDeleteMessage(HyCubeMessage msg) throws ProcessMessageException {
		
		if (devLog.isTraceEnabled()) {
			devLog.trace("Processing DELETE message #" + msg.getSerialNoAndSenderString() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing DELETE message #" + msg.getSerialNoAndSenderString() + ".");
		}
		
		HyCubeDeleteMessageData msgData = null;
		try {
			msgData = HyCubeDeleteMessageData.fromBytes(msg.getData());
		} catch (MessageByteConversionException e) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("DELETE message #" + msg.getSerialNoAndSenderString() + " is corrupted.", e);
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("DELETE message #" + msg.getSerialNoAndSenderString() + "is corrupted.");
			}
			return;
		}
		
		NodePointer sender = new NodePointer(nodeAccessor.getNetworkAdapter(), msg.getSenderNetworkAddress(), msg.getSenderId());
		
		this.dhtManager.processDeleteRequest(sender, msg, msgData.getCommandId(), msgData.getKey(), msgData.getResourceDescriptorString());
		
		
	}
	
	protected void processDeleteReplyMessage(HyCubeMessage msg) throws ProcessMessageException {
		
		if (devLog.isTraceEnabled()) {
			devLog.trace("Processing DELETE_REPLY message #" + msg.getSerialNoAndSenderString() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing DELETE_REPLY message #" + msg.getSerialNoAndSenderString() + ".");
		}
		
		HyCubeDeleteReplyMessageData msgData = null;
		try {
			msgData = HyCubeDeleteReplyMessageData.fromBytes(msg.getData());
		} catch (MessageByteConversionException e) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("DELETE_REPLY message #" + msg.getSerialNoAndSenderString() + " is corrupted.", e);
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("DELETE_REPLY message #" + msg.getSerialNoAndSenderString() + "is corrupted.");
			}
			return;
		}
		
		NodePointer sender = new NodePointer(nodeAccessor.getNetworkAdapter(), msg.getSenderNetworkAddress(), msg.getSenderId());
		
		this.dhtManager.processDeleteResponse(sender, msg, msgData.getCommandId(), msgData.getDeleteStatus());
		
		
	}

	
	protected void processRefreshPutMessage(HyCubeMessage msg) throws ProcessMessageException {
		
		if (devLog.isTraceEnabled()) {
			devLog.trace("Processing REFRESH_PUT message #" + msg.getSerialNoAndSenderString() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing REFRESH_PUT message #" + msg.getSerialNoAndSenderString() + ".");
		}
		
		HyCubeRefreshPutMessageData msgData = null;
		try {
			msgData = HyCubeRefreshPutMessageData.fromBytes(msg.getData());
		} catch (MessageByteConversionException e) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("REFRES_PUT message #" + msg.getSerialNoAndSenderString() + " is corrupted.", e);
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("REFRESH_PUT message #" + msg.getSerialNoAndSenderString() + "is corrupted.");
			}
			return;
		}
		
		NodePointer sender = new NodePointer(nodeAccessor.getNetworkAdapter(), msg.getSenderNetworkAddress(), msg.getSenderId());
		
		this.dhtManager.processRefreshPutRequest(sender, msg, msgData.getCommandId(), msgData.getKey(), msgData.getResourceDescriptorString(), msgData.getRefreshTime());
		
		
	}
	
	
	protected void processRefreshPutReplyMessage(HyCubeMessage msg) throws ProcessMessageException {
		
		if (devLog.isTraceEnabled()) {
			devLog.trace("Processing REFRESH_PUT_REPLY message #" + msg.getSerialNoAndSenderString() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing REFRESH_PUT_REPLY message #" + msg.getSerialNoAndSenderString() + ".");
		}
		
		HyCubeRefreshPutReplyMessageData msgData = null;
		try {
			msgData = HyCubeRefreshPutReplyMessageData.fromBytes(msg.getData());
		} catch (MessageByteConversionException e) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("REFRESH_PUT_REPLY message #" + msg.getSerialNoAndSenderString() + " is corrupted.", e);
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("REFRESH_PUT_REPLY message #" + msg.getSerialNoAndSenderString() + "is corrupted.");
			}
			return;
		}
		
		NodePointer sender = new NodePointer(nodeAccessor.getNetworkAdapter(), msg.getSenderNetworkAddress(), msg.getSenderId());
		
		this.dhtManager.processRefreshPutResponse(sender, msg, msgData.getCommandId(), msgData.getRefreshPutStatus());
	
		
	}

	protected void processReplicateMessage(HyCubeMessage msg) throws ProcessMessageException {
		
		if (devLog.isTraceEnabled()) {
			devLog.trace("Processing REPLICATE message #" + msg.getSerialNoAndSenderString() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing REPLICATE message #" + msg.getSerialNoAndSenderString() + ".");
		}
		
		HyCubeReplicateMessageData msgData = null;
		try {
			msgData = HyCubeReplicateMessageData.fromBytes(msg.getData());
		} catch (MessageByteConversionException e) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("REPLICATE message #" + msg.getSerialNoAndSenderString() + " is corrupted.", e);
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("REPLICATE message #" + msg.getSerialNoAndSenderString() + "is corrupted.");
			}
			return;
		}
		
		NodePointer sender = new NodePointer(nodeAccessor.getNetworkAdapter(), msg.getSenderNetworkAddress(), msg.getSenderId());
		
		this.dhtManager.processReplicateMessage(sender, msg, msgData.getResourcesNum(), msgData.getKeys(), msgData.getResourceDescriptorStrings(), msgData.getRefreshTimes(), msgData.getReplicationSpreadNodesNums());
		
		
	}
	
	@Override
	public void discard() {	
	}
	
	

}
