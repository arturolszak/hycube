package net.hycube.maintenance;

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

public class HyCubeReceivedMessageProcessorRecovery implements ReceivedMessageProcessor {

	//private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeReceivedMessageProcessorRecovery.class); 
	
	
	protected static final String PROP_KEY_MESSAGE_TYPES = "MessageTypes";
	protected static final String PROP_KEY_RECOVERY_EXTENSION_KEY = "RecoveryExtensionKey";
	
	
	
	protected NodeAccessor nodeAccessor;
	protected NodeProperties properties;
	
	protected List<Enum<?>> messageTypes;
	
	
	protected HyCubeRecoveryExtension recoveryExtension;
	protected HyCubeRecoveryManager recoveryManager;
	
	
	protected int nodeIdDimensions;
	protected int nodeIdDigitsCount;
	
	protected int networkAddressByteLength;
	
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
	
		if (devLog.isDebugEnabled()) {
			devLog.debug("Initializing HyCubeReceivedMessageProcessorRecovery.");
		}
		
		this.nodeAccessor = nodeAccessor;
		this.properties = properties;
		
		
		try {
			//load message types processed by this message processor:
			this.messageTypes = properties.getEnumListProperty(PROP_KEY_MESSAGE_TYPES, HyCubeMessageType.class);
			if (this.messageTypes == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES) + ".");
			
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
		
		
		String recoveryExtensionKey = properties.getProperty(PROP_KEY_RECOVERY_EXTENSION_KEY);
		if (recoveryExtensionKey == null || recoveryExtensionKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_RECOVERY_EXTENSION_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_RECOVERY_EXTENSION_KEY));
		if (!(nodeAccessor.getExtension(recoveryExtensionKey) instanceof HyCubeRecoveryExtension)) throw new InitializationException(InitializationException.Error.MESSAGE_RECEIVER_INITIALIZATION_ERROR, null, "The recovery extension is expected to be an instance of " + HyCubeRecoveryExtension.class.getName());
		recoveryExtension = (HyCubeRecoveryExtension) nodeAccessor.getExtension(recoveryExtensionKey);
		
		recoveryManager = recoveryExtension.getRecoveryManager();
		if (recoveryManager == null) throw new InitializationException(InitializationException.Error.MESSAGE_RECEIVER_INITIALIZATION_ERROR, null, "The recovery manager is null");
		
		
		
	}
	
	
	@Override
	public boolean processMessage(Message message, NetworkNodePointer directSender) throws ProcessMessageException {
		
		HyCubeMessage msg = (HyCubeMessage)message;
		
		if (! messageTypes.contains(msg.getType())) return true;
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Processing message #" + msg.getSerialNoAndSenderString() + ". Message validated.");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Message #" + msg.getSerialNoAndSenderString() + " received. Processing.");
		}
		
		try {
			switch (msg.getType()) {
				case RECOVERY:
					processRecoveryMessage(msg);
					break;
				case RECOVERY_REPLY:
					processRecoveryReplyMessage(msg);
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


	
	protected void processRecoveryMessage(Message msg) {
		if (devLog.isDebugEnabled()) {
			devLog.debug("Processing RECOVERY message #" + msg.getSerialNoAndSenderString() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing RECOVERY message #" + msg.getSerialNoAndSenderString() + ".");
		}
		
		HyCubeRecoveryMessageData recData = null;
		try {
			recData = HyCubeRecoveryMessageData.fromBytes(msg.getData());
		} catch (MessageByteConversionException e) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("RECOVERY message #" + msg.getSerialNoAndSenderString() + " is corrupted.", e);
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("RECOVERY message #" + msg.getSerialNoAndSenderString() + "is corrupted.");
			}
			return;
		}
		
		NodePointer sender = new NodePointer(nodeAccessor.getNetworkAdapter(), msg.getSenderNetworkAddress(), msg.getSenderId());
		
		this.recoveryManager.processRecoveryRequest(sender, recData.isReturnNS(), recData.isReturnRT1(), recData.isReturnRT2());
		
	}
    
	protected void processRecoveryReplyMessage(Message msg) {
		if (devLog.isDebugEnabled()) {
			devLog.debug("Processing RECOVERY_REPLY message #" + msg.getSerialNoAndSenderString() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing RECOVERY_REPLY message #" + msg.getSerialNoAndSenderString() + ".");
		}
		
		HyCubeRecoveryReplyMessageData recReplyData = null;
		try {
			recReplyData = HyCubeRecoveryReplyMessageData.fromBytes(msg.getData(), nodeIdDimensions, nodeIdDigitsCount, networkAddressByteLength);
		} catch (MessageByteConversionException e) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("RECOVERY_REPLY message #" + msg.getSerialNoAndSenderString() + " is corrupted.", e);
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("RECOVERY_REPLY message #" + msg.getSerialNoAndSenderString() + "is corrupted.");
			}
			return;
		}
		
		NodePointer sender = new NodePointer(nodeAccessor.getNetworkAdapter(), msg.getSenderNetworkAddress(), msg.getSenderId());
		
		NodePointer[] nodesReturned = recReplyData.getNodePointers(nodeAccessor.getNetworkAdapter());
		
		
		this.recoveryManager.processRecoveryResponse(sender, nodesReturned);
	
	}

    

	@Override
	public void discard() {	
	}
	
	

}
