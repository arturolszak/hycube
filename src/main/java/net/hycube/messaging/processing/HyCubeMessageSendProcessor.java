package net.hycube.messaging.processing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.environment.NodeProperties;
import net.hycube.logging.LogHelper;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.HyCubeMessageType;
import net.hycube.utils.ClassInstanceLoadException;
import net.hycube.utils.ClassInstanceLoader;
import net.hycube.utils.HashMapUtils;

public class HyCubeMessageSendProcessor implements MessageSendProcessor {

	//private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeMessageSendProcessor.class); 
	
	
	protected static final int DEFAULT_MESSAGE_PROCESSORS_NUM_PER_MESSAGE_TYPE = 1;
	
	
	protected static final String PROP_KEY_MESSAGE_SEND_PROCESSORS = "MessageSendProcessors";
	protected static final String PROP_KEY_MESSAGE_TYPES = "MessageTypes";
	
	
	protected NodeAccessor nodeAccessor;
	protected NodeProperties properties;
	
	protected HashMap<HyCubeMessageType, List<MessageSendProcessor>> messageProcessors;
	protected List<Enum<?>> messageTypes;
	
	
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Initializing HyCubeMessageSendProcessor.");
		}
		
		this.nodeAccessor = nodeAccessor;
		this.properties = properties;
		
		
		
		try {
			
			//load message types processed by this message processor:
			this.messageTypes = properties.getEnumListProperty(PROP_KEY_MESSAGE_TYPES, HyCubeMessageType.class);
			if (this.messageTypes == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES), "Unable to initialize message send processor instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES) + ".");
			
			//load the message processors
			List<String> messageProcessorKeys = properties.getStringListProperty(PROP_KEY_MESSAGE_SEND_PROCESSORS);
			if (messageProcessorKeys == null) 
				throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_MESSAGE_SEND_PROCESSORS), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_MESSAGE_SEND_PROCESSORS) + ".");
			this.messageProcessors = new HashMap<HyCubeMessageType, List<MessageSendProcessor>>(HashMapUtils.getHashMapCapacityForElementsNum(HyCubeMessageType.values().length, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
			for (Enum<?> messageType : messageTypes) {
				messageProcessors.put((HyCubeMessageType) messageType, new ArrayList<MessageSendProcessor>(DEFAULT_MESSAGE_PROCESSORS_NUM_PER_MESSAGE_TYPE));
			}
			for (String messageProcessorKey : messageProcessorKeys) {
				if (messageProcessorKey == null || messageProcessorKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_MESSAGE_SEND_PROCESSORS), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_MESSAGE_SEND_PROCESSORS));
				try {
					NodeProperties messageProcessorProperties = properties.getNestedProperty(PROP_KEY_MESSAGE_SEND_PROCESSORS, messageProcessorKey);
					String messageProcessorClass = messageProcessorProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
					
					MessageSendProcessor messageProcessor = (MessageSendProcessor)ClassInstanceLoader.newInstance(messageProcessorClass, MessageSendProcessor.class);
					messageProcessor.initialize(nodeAccessor, messageProcessorProperties);
					
					List<Enum<?>> messageProcessorMessageTypes = messageProcessorProperties.getEnumListProperty(PROP_KEY_MESSAGE_TYPES, HyCubeMessageType.class);
					if (messageProcessorMessageTypes == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, messageProcessorProperties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES), "Unable to initialize message send processor instance. Invalid parameter value: " + messageProcessorProperties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES) + ".");
					for (Enum<?> messageType : messageProcessorMessageTypes) {
						messageProcessors.get(messageType).add(messageProcessor);
					}
				} catch (ClassInstanceLoadException e) {
					throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create message send processor instance.", e);
				}
			}
			
			
			
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, e.getKey(), "Unable to initialize message send processor instance. Invalid parameter value: " + e.getKey() + ".", e);
		}

		
		
	}

	@Override
	public boolean processSendMessage(MessageSendProcessInfo mspi) throws ProcessMessageException {
		
		if (! mspi.processBeforeSend) return true;
		
		HyCubeMessage msg = (HyCubeMessage)mspi.getMsg();
		
		if (! messageTypes.contains(msg.getType())) return true;
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Processing message #" + msg.getSerialNoAndSenderString() + " before sending.");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing message #" + msg.getSerialNoAndSenderString() + " before sending.");
		}
		
		boolean result = true;
		
		try {
			if (messageProcessors.containsKey(msg.getType())) {
				for (MessageSendProcessor messageProcessor : messageProcessors.get(msg.getType())) {
					result = messageProcessor.processSendMessage(mspi);
					if (result == false) break; 
				}
			}
		
		}
		catch (Exception e) {
			throw new ProcessMessageException("An exception thrown while processing a message.", e);
		}
		
		if (result == false) return false;
		
		return true;
		
	}

	
	
	@Override
	public void discard() {	
		
		for (List<MessageSendProcessor> rmpList : messageProcessors.values()) {
			for (MessageSendProcessor rmp : rmpList) {
				rmp.discard();
			}
			rmpList.clear();
		}
		messageProcessors.clear();
		
	}
	
	

}
