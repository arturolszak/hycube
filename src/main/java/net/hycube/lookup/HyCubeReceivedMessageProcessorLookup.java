package net.hycube.lookup;

import java.util.List;

import net.hycube.core.HyCubeNodeIdFactory;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodeIdFactory;
import net.hycube.core.NodePointer;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.environment.NodeProperties;
import net.hycube.logging.LogHelper;
import net.hycube.lookup.HyCubeLookupManager;
import net.hycube.lookup.HyCubeLookupMessageData;
import net.hycube.lookup.HyCubeLookupReplyMessageData;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.HyCubeMessageType;
import net.hycube.messaging.messages.Message;
import net.hycube.messaging.messages.MessageByteConversionException;
import net.hycube.messaging.processing.ProcessMessageException;
import net.hycube.messaging.processing.ReceivedMessageProcessor;
import net.hycube.transport.NetworkNodePointer;

public class HyCubeReceivedMessageProcessorLookup implements ReceivedMessageProcessor {

	//private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeReceivedMessageProcessorLookup.class); 
	
	
	protected static final String PROP_KEY_MESSAGE_TYPES = "MessageTypes";
	
	protected NodeAccessor nodeAccessor;
	protected NodeProperties properties;
	
	protected List<Enum<?>> messageTypes;
	
	
	protected HyCubeLookupManager lookupManager;
	
	protected int nodeIdDimensions;
	protected int nodeIdDigitsCount;
	
	protected int networkAddressByteLength;
	
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
	
		if (devLog.isDebugEnabled()) {
			devLog.debug("Initializing HyCubeReceivedMessageProcessorLookup.");
		}
		
		this.nodeAccessor = nodeAccessor;
		this.properties = properties;
		
		
		try {
			
			//load message types processed by this message processor:
			this.messageTypes = properties.getEnumListProperty(PROP_KEY_MESSAGE_TYPES, HyCubeMessageType.class);
			if (this.messageTypes == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES) + ".");
			
			if (nodeAccessor.getLookupManager() instanceof HyCubeLookupManager) {
				this.lookupManager = (HyCubeLookupManager) nodeAccessor.getLookupManager();
			}
			else {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize received message processor instance. The lookup manager is expected to be an instance of: " + HyCubeLookupManager.class.getName() + ".");
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
				case LOOKUP:
					processLookupMessage(msg);
					break;
				case LOOKUP_REPLY:
					processLookupReplyMessage(msg);
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

	

	protected void processLookupMessage(Message msg) {
		if (devLog.isDebugEnabled()) {
			devLog.debug("Processing LOOKUP message #" + msg.getSerialNoAndSenderString() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing LOOKUP message #" + msg.getSerialNoAndSenderString() + ".");
		}
		
		HyCubeLookupMessageData lookupData = null;
		try {
			lookupData = HyCubeLookupMessageData.fromBytes(msg.getData(), nodeIdDimensions, nodeIdDigitsCount);
		} catch (MessageByteConversionException e) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("LOOKUP message #" + msg.getSerialNoAndSenderString() + " is corrupted.", e);
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("LOOKUP message #" + msg.getSerialNoAndSenderString() + "is corrupted.");
			}
			return;
		}
		
		NodePointer sender = new NodePointer(nodeAccessor.getNetworkAdapter(), msg.getSenderNetworkAddress(), msg.getSenderId());
		
		this.lookupManager.processLookupRequest(sender, lookupData.getLookupId(), lookupData.getLookupNodeId(), lookupData.getParameters());
		
	}
	
	
	protected void processLookupReplyMessage(Message msg) {
		if (devLog.isDebugEnabled()) {
			devLog.debug("Processing LOOKUP_REPLY message #" + msg.getSerialNoAndSenderString() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing LOOKUP_REPLY message #" + msg.getSerialNoAndSenderString() + ".");
		}
		
		HyCubeLookupReplyMessageData lookupReplyData = null;
		try {
			lookupReplyData = HyCubeLookupReplyMessageData.fromBytes(msg.getData(), nodeIdDimensions, nodeIdDigitsCount, networkAddressByteLength);
		} catch (MessageByteConversionException e) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("LOOKUP_REPLY message #" + msg.getSerialNoAndSenderString() + " is corrupted.", e);
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("LOOKUP_REPLY message #" + msg.getSerialNoAndSenderString() + "is corrupted.");
			}
			return;
		}
		
		NodePointer sender = new NodePointer(nodeAccessor.getNetworkAdapter(), msg.getSenderNetworkAddress(), msg.getSenderId());
		
		NodePointer[] nodesFound = lookupReplyData.getNodePointers(nodeAccessor.getNetworkAdapter());
		
		
		this.lookupManager.processLookupResponse(sender, lookupReplyData.getLookupId(), lookupReplyData.getParameters(), nodesFound);
	
		
	}
    

	
	@Override
	public void discard() {	
	}
	
	

}
