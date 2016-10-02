package net.hycube.messaging.processing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.environment.NodeProperties;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.logging.LogHelper;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.HyCubeMessageType;
import net.hycube.messaging.messages.Message;
import net.hycube.routing.HyCubeRegisteredRouteInfo;
import net.hycube.routing.HyCubeRoutingManager;
import net.hycube.transport.NetworkAdapterException;
import net.hycube.transport.NetworkNodePointer;
import net.hycube.utils.ClassInstanceLoadException;
import net.hycube.utils.ClassInstanceLoader;
import net.hycube.utils.HashMapUtils;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public class HyCubeReceivedMessageProcessor implements ReceivedMessageProcessor {

	//private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeReceivedMessageProcessor.class); 
	
	
	protected static final int DEFAULT_MESSAGE_PROCESSORS_NUM_PER_MESSAGE_TYPE = 1;
	
	protected static final String PROP_KEY_RECEIVED_MESSAGE_PROCESSORS = "ReceivedMessageProcessors";
	protected static final String PROP_KEY_MESSAGE_TYPES = "MessageTypes";
	protected static final String PROP_KEY_LIMIT_MAX_PROCESSED_MESSAGES_RATE = "LimitMaxProcessedMessagesRate";
	protected static final String PROP_KEY_LIMIT_MAX_PROCESSED_MESSAGES_RATE_FOR_TYPES = "LimitForTypes";
	protected static final String PROP_KEY_LIMIT_MAX_PROCESSED_MESSAGES_RATE_NUM = "Num";
	protected static final String PROP_KEY_LIMIT_MAX_PROCESSED_MESSAGES_RATE_TIME = "Time";
	protected static final String PROP_KEY_PROCESS_ROUTE_BACK_MESSAGES_BY_NODES_ON_ROUTE = "ProcessRouteBackMessagesByNodesOnRoute";
	
	
	
	protected static final int INITIAL_RECENT_MESSAGES_COLLECTION_SIZE = GlobalConstants.DEFAULT_INITIAL_COLLECTION_SIZE;
	
	
	protected NodeAccessor nodeAccessor;
	protected NodeProperties properties;
	
	protected HashMap<HyCubeMessageType, List<ReceivedMessageProcessor>> messageProcessors;
	protected List<Enum<?>> messageTypes;
	
	

	
	//for limiting the number of messages processed in the defined time spans:
	protected int limitMaxProcessedMessagesRateNum;
	protected int limitMaxProcessedMessagesRateTime;
	protected HashMap<HyCubeMessageType, Integer> limitMaxProcessedMessagesRateNums;
	protected HashMap<HyCubeMessageType, Integer> limitMaxProcessedMessagesRateTimes;
	
	protected LinkedList<ReceivedMessageInfo> messagesRecentlyProcessedAll;
	protected HashMap<HyCubeMessageType, LinkedList<ReceivedMessageInfo>> messagesRecentlyProcessedForType;
	
	protected boolean processRouteBackMessagesByNodesOnRoute;
	
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
	
		if (devLog.isDebugEnabled()) {
			devLog.debug("Initializing HyCubeReceivedMessageProcessor.");
		}
		
		this.nodeAccessor = nodeAccessor;
		this.properties = properties;
		
		
		try {
			
			//load message types processed by this message processor:
			this.messageTypes = properties.getEnumListProperty(PROP_KEY_MESSAGE_TYPES, HyCubeMessageType.class);
			if (this.messageTypes == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES) + ".");
			
			
			//load the message processors
			List<String> messageProcessorKeys = properties.getStringListProperty(PROP_KEY_RECEIVED_MESSAGE_PROCESSORS);
			if (messageProcessorKeys == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_RECEIVED_MESSAGE_PROCESSORS), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_RECEIVED_MESSAGE_PROCESSORS) + ".");
			this.messageProcessors = new HashMap<HyCubeMessageType, List<ReceivedMessageProcessor>>(HashMapUtils.getHashMapCapacityForElementsNum(HyCubeMessageType.values().length, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
			for (Enum<?> messageType : messageTypes) {
				messageProcessors.put((HyCubeMessageType) messageType, new ArrayList<ReceivedMessageProcessor>(DEFAULT_MESSAGE_PROCESSORS_NUM_PER_MESSAGE_TYPE));
			}
			for (String messageProcessorKey : messageProcessorKeys) {
				if (messageProcessorKey == null || messageProcessorKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_RECEIVED_MESSAGE_PROCESSORS), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_RECEIVED_MESSAGE_PROCESSORS));
				try {
					NodeProperties messageProcessorProperties = properties.getNestedProperty(PROP_KEY_RECEIVED_MESSAGE_PROCESSORS, messageProcessorKey);
					String messageProcessorClass = messageProcessorProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
					
					ReceivedMessageProcessor messageProcessor = (ReceivedMessageProcessor)ClassInstanceLoader.newInstance(messageProcessorClass, ReceivedMessageProcessor.class);
					messageProcessor.initialize(nodeAccessor, messageProcessorProperties);
					
					List<Enum<?>> messageProcessorMessageTypes = messageProcessorProperties.getEnumListProperty(PROP_KEY_MESSAGE_TYPES, HyCubeMessageType.class);
					if (messageProcessorMessageTypes == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, messageProcessorProperties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES), "Invalid parameter value: " + messageProcessorProperties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES) + ".");
					for (Enum<?> messageType : messageProcessorMessageTypes) {
						messageProcessors.get(messageType).add(messageProcessor);
					}
				} catch (ClassInstanceLoadException e) {
					throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create received message processor instance.", e);
				}
			}
			
			
			
			
			NodeProperties limitMaxProcessedMessagesRateProperties = properties.getNestedProperty(PROP_KEY_LIMIT_MAX_PROCESSED_MESSAGES_RATE); 
			
			this.limitMaxProcessedMessagesRateNum = (Integer) limitMaxProcessedMessagesRateProperties.getProperty(PROP_KEY_LIMIT_MAX_PROCESSED_MESSAGES_RATE_NUM, MappedType.INT);
			this.limitMaxProcessedMessagesRateTime = (Integer) limitMaxProcessedMessagesRateProperties.getProperty(PROP_KEY_LIMIT_MAX_PROCESSED_MESSAGES_RATE_TIME, MappedType.INT);
			
			this.messagesRecentlyProcessedAll = new LinkedList<ReceivedMessageInfo>();
			
			List<Enum<?>> limitMaxProcessedMessagesRateMessageTypes = limitMaxProcessedMessagesRateProperties.getEnumListProperty(PROP_KEY_LIMIT_MAX_PROCESSED_MESSAGES_RATE_FOR_TYPES, HyCubeMessageType.class);
			if (limitMaxProcessedMessagesRateMessageTypes == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, limitMaxProcessedMessagesRateProperties.getAbsoluteKey(PROP_KEY_LIMIT_MAX_PROCESSED_MESSAGES_RATE_FOR_TYPES), "Invalid parameter value: " + limitMaxProcessedMessagesRateProperties.getAbsoluteKey(PROP_KEY_LIMIT_MAX_PROCESSED_MESSAGES_RATE_FOR_TYPES) + ".");
			
			
			this.limitMaxProcessedMessagesRateNums = new HashMap<HyCubeMessageType, Integer>(HashMapUtils.getHashMapCapacityForElementsNum(limitMaxProcessedMessagesRateMessageTypes.size(), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
			this.limitMaxProcessedMessagesRateTimes = new HashMap<HyCubeMessageType, Integer>(HashMapUtils.getHashMapCapacityForElementsNum(limitMaxProcessedMessagesRateMessageTypes.size(), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
			this.messagesRecentlyProcessedForType = new HashMap<HyCubeMessageType, LinkedList<ReceivedMessageInfo>>(HashMapUtils.getHashMapCapacityForElementsNum(limitMaxProcessedMessagesRateMessageTypes.size(), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
			
			
			for (Enum<?> messageType : limitMaxProcessedMessagesRateMessageTypes) {
				HyCubeMessageType mType = (HyCubeMessageType) messageType;
				NodeProperties limitMaxProcessedMessagesRateForTypeProperties = properties.getNestedProperty(PROP_KEY_LIMIT_MAX_PROCESSED_MESSAGES_RATE, mType.name());
				messagesRecentlyProcessedForType.put(mType, new LinkedList<ReceivedMessageInfo>());
				int num = (Integer) limitMaxProcessedMessagesRateForTypeProperties.getProperty(PROP_KEY_LIMIT_MAX_PROCESSED_MESSAGES_RATE_NUM, MappedType.INT);
				int time = (Integer) limitMaxProcessedMessagesRateForTypeProperties.getProperty(PROP_KEY_LIMIT_MAX_PROCESSED_MESSAGES_RATE_TIME, MappedType.INT);
				limitMaxProcessedMessagesRateNums.put(mType, num);
				limitMaxProcessedMessagesRateTimes.put(mType, time);
			}
			

			
			this.processRouteBackMessagesByNodesOnRoute = (Boolean) properties.getProperty(PROP_KEY_PROCESS_ROUTE_BACK_MESSAGES_BY_NODES_ON_ROUTE, MappedType.BOOLEAN);
			
			
			
			
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, e.getKey(), "Unable to initialize received message processor instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		
		
		
	}
	
	
	@Override
	public boolean processMessage(Message message, NetworkNodePointer directSender) throws ProcessMessageException {

		HyCubeMessage msg = (HyCubeMessage)message;
		
		
		if (! messageTypes.contains(msg.getType())) return true;
		
		
		if (msg.getHopCount() == 0) {
			//ignore messages with hop count 0 -> the hop count of received messages should always be != 0, otherwise it is considered incorrect (possible deceit)
			return false;
		}
		
		
		if (msg.isRouteBack()) {
			if (msg.getRecipientId().equals(nodeAccessor.getNodeId())) {
				HyCubeRegisteredRouteInfo registeredRoute = ((HyCubeRoutingManager)(nodeAccessor.getRoutingManager())).getRegisteredRoute(msg.getRouteId());
				if (registeredRoute == null) {
					//discard the message, no reverse route info
					return false;
				}
				//validate
				if (Arrays.equals(registeredRoute.getOutNetworkNodePointer().getAddressBytes(), directSender.getAddressBytes())) {
					
					if (registeredRoute.isRouteStart()) {
						
						//process
						return processReceivedMessage(msg, directSender);
							
					}
					else {
						
						//route the message back:

						//check if route-back messages should be processed by message processors
						
						boolean status = true;
						if (processRouteBackMessagesByNodesOnRoute) {
							status = processReceivedMessage(msg, directSender);
						}
						
						if (status) {
							boolean sent;
							try {
								sent = nodeAccessor.sendMessage(new MessageSendProcessInfo(msg), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
							} catch (NetworkAdapterException e) {
								throw new ProcessMessageException("An exception has been thrown while sending a message.", e);
							}
							return sent;
						}
						else {
							return false;
						}
						
					}
					
				}
				else {
					//discard the message, the message direct sender does not match the node for which the route was registered
					return false;
				}
			
			}
			else {
				//discard the message, the recipient of the message is different than self
				return false;
			}
		}
		else {
			return processReceivedMessage(msg, directSender);
		}
		
		
	}
	
	
	public boolean processReceivedMessage(HyCubeMessage msg, NetworkNodePointer directSender) throws ProcessMessageException {
		
		
		//check if the message should be processed - if it did not exceed the maximum number of messages processed in a time span:
		boolean isWithinLimits = checkIfWithinMessagesProcessedMaxRate(msg);
		if (! isWithinLimits) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("Discarding message #" + msg.getSerialNoAndSenderString() + ". The number of messages processed in a time span exceeded.");
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("Discarding message #" + msg.getSerialNoAndSenderString() + ". The number of messages processed in a time span exceeded.");
			}
			return false;
		}
		
		

		
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Processing message #" + msg.getSerialNoAndSenderString() + ". Message validated.");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Message #" + msg.getSerialNoAndSenderString() + " received. Processing.");
		}
		
		boolean result = true;
		
		try {
			if (messageProcessors.containsKey(msg.getType())) {
				for (ReceivedMessageProcessor messageProcessor : messageProcessors.get(msg.getType())) {
					result = messageProcessor.processMessage(msg, directSender);
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

	
	


	
	public boolean checkIfWithinMessagesProcessedMaxRate(HyCubeMessage msg) {
		
		if ((limitMaxProcessedMessagesRateNum > 0 && limitMaxProcessedMessagesRateTime > 0)
				|| (limitMaxProcessedMessagesRateNums.containsKey(msg.getType()) && limitMaxProcessedMessagesRateTimes.containsKey(msg.getType()) && limitMaxProcessedMessagesRateNums.get(msg.getType()) > 0 && limitMaxProcessedMessagesRateNums.get(msg.getType()) > 0)) {
			
			synchronized (messagesRecentlyProcessedAll) {
				long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
				purgeRecentMessagesForMessagesProcessedMaxRate(currTime, msg.getType());

				long senderIdHash = msg.getSenderId().calculateHash();
				if ((limitMaxProcessedMessagesRateNum > 0 && limitMaxProcessedMessagesRateTime > 0 && messagesRecentlyProcessedAll.size() >= limitMaxProcessedMessagesRateNum)
						|| (messagesRecentlyProcessedForType.containsKey(msg.getType()) && limitMaxProcessedMessagesRateNums.get(msg.getType()) > 0 && limitMaxProcessedMessagesRateTimes.get(msg.getType()) > 0 && messagesRecentlyProcessedForType.get(msg.getType()).size() > limitMaxProcessedMessagesRateNums.get(msg.getType()))) {
					return false;
				}
				else {
					ReceivedMessageInfo rmi = new ReceivedMessageInfo(senderIdHash, msg.getSenderId(), msg.getSerialNo(), msg.getCRC32(), currTime, null);
					messagesRecentlyProcessedAll.add(rmi);
					if (messagesRecentlyProcessedForType.containsKey(msg.getType())) {
						messagesRecentlyProcessedForType.get(msg.getType()).add(rmi);
					}
					return true;
				}
				
			}
			
		}
		return true;

	}
	
	
	public void purgeRecentMessagesForMessagesProcessedMaxRate(long time) {
		purgeRecentMessagesForMessagesProcessedMaxRate(time, null);
	}
	
	public void purgeRecentMessagesForMessagesProcessedMaxRate(long time, HyCubeMessageType messageType) {

		ListIterator<ReceivedMessageInfo> iter = messagesRecentlyProcessedAll.listIterator();
		while (iter.hasNext()) {
			ReceivedMessageInfo rmi = iter.next();
			if (time >= rmi.receiveTime + limitMaxProcessedMessagesRateTime) {
				iter.remove();
			}
			else {
				//the list is sorted by time -> no more elements have to be checked
				break;
			}
		}	
		
		if (messageType != null && messagesRecentlyProcessedForType.containsKey(messageType)) {
			ListIterator<ReceivedMessageInfo> iterForType = messagesRecentlyProcessedForType.get(messageType).listIterator();
			while (iterForType.hasNext()) {
				ReceivedMessageInfo rmi = iterForType.next();
				if (time >= rmi.receiveTime + limitMaxProcessedMessagesRateTimes.get(messageType)) {
					iterForType.remove();
				}
				else {
					//the list is sorted by time -> no more elements have to be checked
					break;
				}
			}
		}
		
	}
	
	
	public void purgeRecentMessagesForMessagesProcessedMaxRateForAllMessageTypes(long time) {

		ListIterator<ReceivedMessageInfo> iter = messagesRecentlyProcessedAll.listIterator();
		while (iter.hasNext()) {
			ReceivedMessageInfo rmi = iter.next();
			if (time >= rmi.receiveTime + limitMaxProcessedMessagesRateTime) {
				iter.remove();
			}
			else {
				//the list is sorted by time -> no more elements have to be checked
				break;
			}
		}	
		
		for (HyCubeMessageType messageType : messagesRecentlyProcessedForType.keySet()) {
			if (messageType != null && messagesRecentlyProcessedForType.containsKey(messageType)) {
				ListIterator<ReceivedMessageInfo> iterForType = messagesRecentlyProcessedForType.get(messageType).listIterator();
				while (iterForType.hasNext()) {
					ReceivedMessageInfo rmi = iterForType.next();
					if (time >= rmi.receiveTime + limitMaxProcessedMessagesRateTimes.get(messageType)) {
						iterForType.remove();
					}
					else {
						//the list is sorted by time -> no more elements have to be checked
						break;
					}
				}
			}
		}		
	}
	
	

	@Override
	public void discard() {
		
		for (List<ReceivedMessageProcessor> rmpList : messageProcessors.values()) {
			for (ReceivedMessageProcessor rmp : rmpList) {
				rmp.discard();
			}
			rmpList.clear();
		}
		messageProcessors.clear();
		
	}
	
	
}
