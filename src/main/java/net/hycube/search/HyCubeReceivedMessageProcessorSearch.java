package net.hycube.search;

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

public class HyCubeReceivedMessageProcessorSearch implements ReceivedMessageProcessor {

	//private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeReceivedMessageProcessorSearch.class); 
	
	
	protected static final String PROP_KEY_MESSAGE_TYPES = "MessageTypes";
	
	protected NodeAccessor nodeAccessor;
	protected NodeProperties properties;
	
	protected List<Enum<?>> messageTypes;
	
	
	protected HyCubeSearchManager searchManager;
	
	protected int nodeIdDimensions;
	protected int nodeIdDigitsCount;
	
	protected int networkAddressByteLength;
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
	
		if (devLog.isDebugEnabled()) {
			devLog.debug("Initializing HyCubeReceivedMessageProcessorSearch.");
		}
		
		this.nodeAccessor = nodeAccessor;
		this.properties = properties;
		
		
		try {
			
			//load message types processed by this message processor:
			this.messageTypes = properties.getEnumListProperty(PROP_KEY_MESSAGE_TYPES, HyCubeMessageType.class);
			if (this.messageTypes == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES) + ".");
			
			if (nodeAccessor.getSearchManager() instanceof HyCubeSearchManager) {
				this.searchManager = (HyCubeSearchManager) nodeAccessor.getSearchManager();
			}
			else {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize received message processor instance. The search manager is expected to be an instance of: " + HyCubeSearchManager.class.getName() + ".");
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
				case SEARCH:
					processSearchMessage(msg);
					break;
				case SEARCH_REPLY:
					processSearchReplyMessage(msg);
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

	

	protected void processSearchMessage(Message msg) {
		if (devLog.isDebugEnabled()) {
			devLog.debug("Processing SEARCH message #" + msg.getSerialNoAndSenderString() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing SEARCH message #" + msg.getSerialNoAndSenderString() + ".");
		}
		
		HyCubeSearchMessageData searchData = null;
		try {
			searchData = HyCubeSearchMessageData.fromBytes(msg.getData(), nodeIdDimensions, nodeIdDigitsCount);
		} catch (MessageByteConversionException e) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("SEARCH message #" + msg.getSerialNoAndSenderString() + " is corrupted.", e);
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("SEARCH message #" + msg.getSerialNoAndSenderString() + "is corrupted.");
			}
			return;
		}
		
		NodePointer sender = new NodePointer(nodeAccessor.getNetworkAdapter(), msg.getSenderNetworkAddress(), msg.getSenderId());
		
		this.searchManager.processSearchRequest(sender, searchData.getSearchId(), searchData.getSearchNodeId(), searchData.getParameters());
		
	}
	
	
	protected void processSearchReplyMessage(Message msg) {
		if (devLog.isDebugEnabled()) {
			devLog.debug("Processing SEARCH_REPLY message #" + msg.getSerialNoAndSenderString() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing SEARCH_REPLY message #" + msg.getSerialNoAndSenderString() + ".");
		}
		
		HyCubeSearchReplyMessageData searchReplyData = null;
		try {
			searchReplyData = HyCubeSearchReplyMessageData.fromBytes(msg.getData(), nodeIdDimensions, nodeIdDigitsCount, networkAddressByteLength);
		} catch (MessageByteConversionException e) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("SEARCH_REPLY message #" + msg.getSerialNoAndSenderString() + " is corrupted.", e);
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("SEARCH_REPLY message #" + msg.getSerialNoAndSenderString() + "is corrupted.");
			}
			return;
		}
		
		NodePointer sender = new NodePointer(nodeAccessor.getNetworkAdapter(), msg.getSenderNetworkAddress(), msg.getSenderId());
		
		NodePointer[] nodesFound = searchReplyData.getNodePointers(nodeAccessor.getNetworkAdapter());
		
		
		this.searchManager.processSearchResponse(sender, searchReplyData.getSearchId(), searchReplyData.getParameters(), nodesFound);
	
		
	}
    

	
	@Override
	public void discard() {	
	}
	
	
	

}
