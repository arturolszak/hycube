package net.hycube.messaging.ack;

import java.util.List;

import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.environment.NodeProperties;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.logging.LogHelper;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.HyCubeMessageType;
import net.hycube.messaging.processing.MessageSendProcessInfo;
import net.hycube.messaging.processing.MessageSendProcessor;
import net.hycube.messaging.processing.ProcessMessageException;

public class HyCubeMessageSendProcessorDataAck implements MessageSendProcessor {

	//private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeMessageSendProcessorDataAck.class); 
	
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
			devLog.debug("Initializing HyCubeMessageSendProcessorData.");
		}
		
		this.nodeAccessor = nodeAccessor;
		this.properties = properties;
		
		try {
			//load message types processed by this message processor:
			this.messageTypes = properties.getEnumListProperty(PROP_KEY_MESSAGE_TYPES, HyCubeMessageType.class);
			if (this.messageTypes == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES) + ".");
			
			
			//get the ack extension:
			ackExtensionKey = properties.getProperty(PROP_KEY_ACK_EXTENSION_KEY);
			if (ackExtensionKey == null || ackExtensionKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_ACK_EXTENSION_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_ACK_EXTENSION_KEY));
			try {
				ackExtension = (HyCubeAckExtension) nodeAccessor.getExtension(ackExtensionKey);
				if (this.ackExtension == null) throw new InitializationException(InitializationException.Error.MISSING_EXTENSION_ERROR, this.ackExtensionKey, "The AckExtension is missing at the specified key: " + this.ackExtensionKey + ".");
			} catch (ClassCastException e) {
				throw new InitializationException(InitializationException.Error.MISSING_EXTENSION_ERROR, this.ackExtensionKey, "The AckExtension is missing at the specified key: " + this.ackExtensionKey + ".");
			}


			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize message send processor instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		
	}

	@Override
	public boolean processSendMessage(MessageSendProcessInfo mspi) throws ProcessMessageException {
		
		if (! mspi.getProcessBeforeSend()) return true;
		
		HyCubeMessage msg = (HyCubeMessage)mspi.getMsg();
		
		if (! messageTypes.contains(msg.getType())) return true;
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Processing message #" + msg.getSerialNoAndSenderString() + " before sending.");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing message #" + msg.getSerialNoAndSenderString() + " before sending.");
		}
		
		try {
			switch (msg.getType()) {
				case DATA:
					processSendDataMessage(mspi);
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
	
	
	
	public void processSendDataMessage(MessageSendProcessInfo mspi) {
		
    	if (devLog.isDebugEnabled()) {
			devLog.debug("Processing DATA message #" + mspi.getMsg().getSerialNoAndSenderString() + " before sending.");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing DATA message #" + mspi.getMsg().getSerialNoAndSenderString() + " before sending.");
		}
		
		ackExtension.getAckManager().processSendDataMessage(mspi);
		
		
	}
	
	

	@Override
	public void discard() {	
	}
	
	
}
