package net.hycube.messaging.ack;

import java.util.List;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.environment.NodeProperties;
import net.hycube.logging.LogHelper;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.HyCubeMessageType;
import net.hycube.messaging.messages.Message;
import net.hycube.messaging.processing.MessageSendProcessInfo;
import net.hycube.messaging.processing.ProcessMessageException;
import net.hycube.messaging.processing.ReceivedMessageProcessor;
import net.hycube.transport.NetworkAdapterException;
import net.hycube.transport.NetworkNodePointer;

public class HyCubeReceivedMessageProcessorDataAck implements ReceivedMessageProcessor {

	//private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeReceivedMessageProcessorDataAck.class); 
	
	
	protected static final String PROP_KEY_ACK_EXTENSION_KEY = "AckExtensionKey";
	protected static final String PROP_KEY_MESSAGE_TYPES = "MessageTypes";
	
	
	protected NodeAccessor nodeAccessor;
	protected NodeProperties properties;
	
	protected List<Enum<?>> messageTypes;
	
	protected String ackExtensionKey;
	protected HyCubeAckExtension ackExtension;

	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
	
		if (devLog.isDebugEnabled()) {
			devLog.debug("Initializing HyCubeReceivedMessageProcessorData.");
		}
		
		this.nodeAccessor = nodeAccessor;
		this.properties = properties;
		
		
		try {
			//load message types processed by this message processor:
			this.messageTypes = properties.getEnumListProperty(PROP_KEY_MESSAGE_TYPES, HyCubeMessageType.class);
			if (this.messageTypes == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES) + ".");

			//get the ack extension
			ackExtensionKey = properties.getProperty(PROP_KEY_ACK_EXTENSION_KEY);
			if (ackExtensionKey == null || ackExtensionKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_ACK_EXTENSION_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_ACK_EXTENSION_KEY));
			try {
				ackExtension = (HyCubeAckExtension) nodeAccessor.getExtension(ackExtensionKey);
				if (this.ackExtension == null) throw new InitializationException(InitializationException.Error.MISSING_EXTENSION_ERROR, this.ackExtensionKey, "The AckExtension is missing at the specified key: " + this.ackExtensionKey + ".");
			} catch (ClassCastException e) {
				throw new InitializationException(InitializationException.Error.MISSING_EXTENSION_ERROR, this.ackExtensionKey, "The AckExtension is missing at the specified key: " + this.ackExtensionKey + ".");
			}


			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize received message processor instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		
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
				case DATA_ACK:
					processDataAckMessage(msg);
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



	protected void processDataAckMessage(Message msg) throws NetworkAdapterException, ProcessMessageException {

		if (msg.getRecipientId().equals(nodeAccessor.getNodeId())) {
	
			if (devLog.isDebugEnabled()) {
				devLog.debug("Received DATA_ACK message #" + msg.getSerialNoAndSenderString() + ".");
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("Received DATA_ACK message #" + msg.getSerialNoAndSenderString() + ".");
			}
			
			ackExtension.getAckManager().processDataAckMessage(msg);
			
		}
		else {
			nodeAccessor.sendMessage(new MessageSendProcessInfo(msg), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
		}
		
		
	}
        

	
	@Override
	public void discard() {	
	}
	
	

}
