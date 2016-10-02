package net.hycube.join.routejoin;

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

public class HyCubeReceivedMessageProcessorRouteJoin implements ReceivedMessageProcessor {

	//private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeReceivedMessageProcessorRouteJoin.class); 
	
	
	protected static final String PROP_KEY_MESSAGE_TYPES = "MessageTypes";
	
	protected NodeAccessor nodeAccessor;
	protected NodeProperties properties;
	
	protected List<Enum<?>> messageTypes;
	
	
	protected HyCubeRouteJoinManager joinManager;
	
	protected int nodeIdDimensions;
	protected int nodeIdDigitsCount;
	
	protected int networkAddressByteLength;
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
	
		if (devLog.isDebugEnabled()) {
			devLog.debug("Initializing HyCubeReceivedMessageProcessorJoin.");
		}
		
		this.nodeAccessor = nodeAccessor;
		this.properties = properties;
		
		
		try {
			
			//load message types processed by this message processor:
			this.messageTypes = properties.getEnumListProperty(PROP_KEY_MESSAGE_TYPES, HyCubeMessageType.class);
			if (this.messageTypes == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES) + ".");
			
			if (nodeAccessor.getJoinManager() instanceof HyCubeRouteJoinManager) {
				this.joinManager = (HyCubeRouteJoinManager) nodeAccessor.getJoinManager();
			}
			else {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize received message processor instance. The join manager is expected to be an instance of: " + HyCubeRouteJoinManager.class.getName() + ".");
			}
			
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize received message processor instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		NodeIdFactory nodeIdFactory = nodeAccessor.getNodeIdFactory();
		if (nodeIdFactory instanceof HyCubeNodeIdFactory) {
			nodeIdDimensions = ((HyCubeNodeIdFactory)(nodeAccessor.getNodeIdFactory())).getDimensions();
			nodeIdDigitsCount = ((HyCubeNodeIdFactory)(nodeAccessor.getNodeIdFactory())).getDigitsCount();
			
			networkAddressByteLength = nodeAccessor.getNetworkAdapter().getAddressByteLength();
			
			
		}
		else {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize received message processor instance. node id factory should be an instance of: " + HyCubeNodeIdFactory.class.getName() + ".");
		}
		
		
	}
	
	
	@Override
	public boolean processMessage(Message message, NetworkNodePointer directSender) throws ProcessMessageException {
		
		HyCubeMessage msg = (HyCubeMessage)message;
		
		if (! messageTypes.contains(msg.getType())) return true;
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Message #" + msg.getSerialNoAndSenderString() + " received. Processing.");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Message #" + msg.getSerialNoAndSenderString() + " received. Processing.");
		}
		
		try {
			switch (msg.getType()) {
				case JOIN:
					processJoinMessage(msg, directSender);
					break;
				case JOIN_REPLY:
					processJoinReplyMessage(msg);
					break;
				default:
					break;
			}
		}
		catch (Exception e) {
			throw new ProcessMessageException("An exception thrown while processing a message.", e);
		}

		return true;
		
	}

	

	protected void processJoinMessage(HyCubeMessage msg, NetworkNodePointer directSender) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Processing JOIN message #" + msg.getSerialNoAndSenderString() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing JOIN message #" + msg.getSerialNoAndSenderString() + ".");
		}
		
		HyCubeRouteJoinMessageData joinData = null;
		try {
			joinData = HyCubeRouteJoinMessageData.fromBytes(msg.getData(), nodeIdDimensions, nodeIdDigitsCount);
		} catch (MessageByteConversionException e) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("JOIN message #" + msg.getSerialNoAndSenderString() + " is corrupted.", e);
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("JOIN message #" + msg.getSerialNoAndSenderString() + "is corrupted.");
			}
			return;
		}
		
		NodePointer sender = new NodePointer(nodeAccessor.getNetworkAdapter(), msg.getSenderNetworkAddress(), msg.getSenderId());
		
		this.joinManager.processJoinRequest(sender, joinData.getJoinId(), joinData.getJoinNodeId(), msg, directSender, joinData.getDiscoverPublicNetworkAddress());
		
	}
	
	
	protected void processJoinReplyMessage(HyCubeMessage msg) {
		if (devLog.isDebugEnabled()) {
			devLog.debug("Processing JOIN_REPLY message #" + msg.getSerialNoAndSenderString() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing JOIN_REPLY message #" + msg.getSerialNoAndSenderString() + ".");
		}
		
		HyCubeRouteJoinReplyMessageData joinReplyData = null;
		try {
			joinReplyData = HyCubeRouteJoinReplyMessageData.fromBytes(msg.getData(), nodeIdDimensions, nodeIdDigitsCount, networkAddressByteLength);
		} catch (MessageByteConversionException e) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("JOIN_REPLY message #" + msg.getSerialNoAndSenderString() + " is corrupted.", e);
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("JOIN_REPLY message #" + msg.getSerialNoAndSenderString() + "is corrupted.");
			}
			return;
		}
		
		NodePointer sender = new NodePointer(nodeAccessor.getNetworkAdapter(), msg.getSenderNetworkAddress(), msg.getSenderId());
		
		NodePointer[] nodesFound = joinReplyData.getNodePointers(nodeAccessor.getNetworkAdapter());
		NetworkNodePointer requestorNetworkNodePointer = joinReplyData.getRequestorNetworkNodePointer(nodeAccessor.getNetworkAdapter());
		
		
		this.joinManager.processJoinResponse(sender, joinReplyData.getJoinId(), requestorNetworkNodePointer, nodesFound, joinReplyData.isFinalJoinReply());
	
		
	}
    

	
	@Override
	public void discard() {	
	}
	
	

}
