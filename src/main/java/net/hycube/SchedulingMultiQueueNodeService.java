package net.hycube;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeId;
import net.hycube.environment.Environment;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.environment.NodeProperties;
import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventCategory;
import net.hycube.eventprocessing.EventProcessingErrorCallback;
import net.hycube.eventprocessing.EventQueueProcessingInfo;
import net.hycube.eventprocessing.EventQueueSchedulerProcessor;
import net.hycube.eventprocessing.EventScheduler;
import net.hycube.eventprocessing.EventType;
import net.hycube.eventprocessing.NotifyingBlockingQueue;
import net.hycube.eventprocessing.NotifyingLinkedBlockingQueue;
import net.hycube.eventprocessing.ThreadPoolInfo;
import net.hycube.eventprocessing.WakeableManager;
import net.hycube.join.JoinCallback;
import net.hycube.logging.LogHelper;
import net.hycube.transport.MessageReceiver;
import net.hycube.transport.MessageReceiverException;
import net.hycube.transport.WakeableMessageReceiver;
import net.hycube.utils.ClassInstanceLoadException;
import net.hycube.utils.ClassInstanceLoader;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public abstract class SchedulingMultiQueueNodeService implements NodeService {

	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(SchedulingMultiQueueNodeService.class); 
	

	public static final String PROP_KEY_QUEUES = "Queues";
	public static final String PROP_KEY_THREAD_POOL = "ThreadPool";
	public static final String PROP_KEY_CORE_POOL_SIZE = "PoolSize";
	public static final String PROP_KEY_KEEP_ALIVE_TIME_SEC = "KeepAliveTimeSec";
	public static final String PROP_KEY_EVENT_TYPES = "EventTypes";
	public static final String PROP_KEY_EVENT_CATEGORY = "EventCategory";
	public static final String PROP_KEY_EVENT_TYPE_KEY = "EventTypeKey";
	public static final String PROP_KEY_WAKEABLE = "Wakeable";
	
	
	protected NodeProxyService nodeProxyService;
	
	protected EventQueueSchedulerProcessor eventProcessor;
	protected MessageReceiver messageReceiver;
	
	protected Map<EventType, LinkedBlockingQueue<Event>> eventQueues;
	protected Map<EventType, WakeableManager> wakeableManagersByEventType;
	protected List<NotifyingLinkedBlockingQueue<Event>> notifyingQueuesList;
	
	protected boolean initialized = false;
	protected boolean discarded = false;
	
	
	public static SchedulingMultiQueueNodeService initializeFromConf(Environment environment, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(null, null, environment, null, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, errorCallback, errorCallbackArg);
	}
	
	public static SchedulingMultiQueueNodeService initializeFromConf(Environment environment, NodeId nodeId, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(null, null, environment, nodeId, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, errorCallback, errorCallbackArg);
	}
	
	public static SchedulingMultiQueueNodeService initializeFromConf(Environment environment, String nodeIdString, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(null, null, environment, null, nodeIdString, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, errorCallback, errorCallbackArg);
	}
	
		
	public static SchedulingMultiQueueNodeService initializeFromConf(String nodeServiceConfKey, Environment environment, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(nodeServiceConfKey, null, environment, null, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, errorCallback, errorCallbackArg);
	}
	
	public static SchedulingMultiQueueNodeService initializeFromConf(String nodeServiceConfKey, Environment environment, NodeId nodeId, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(nodeServiceConfKey, null, environment, nodeId, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, errorCallback, errorCallbackArg);
	}
	
	public static SchedulingMultiQueueNodeService initializeFromConf(String nodeServiceConfKey, Environment environment, String nodeIdString, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(nodeServiceConfKey, null, environment, null, nodeIdString, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, errorCallback, errorCallbackArg);
	}
	
	protected static SchedulingMultiQueueNodeService initializeFromConf(String nodeServiceConfKey, SchedulingMultiQueueNodeService nodeService, Environment environment, NodeId nodeId, String nodeIdString, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {

		EventQueueProcessingInfo[] eventQueuesProcessingInfo;
		
		try {
			String nodeServiceKey;
			if (nodeServiceConfKey != null && (!nodeServiceConfKey.trim().isEmpty())) nodeServiceKey = nodeServiceConfKey;
			else nodeServiceKey = environment.getNodeProperties().getProperty(PROP_KEY_NODE_SERVICE);
			if (nodeServiceKey == null || nodeServiceKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, environment.getNodeProperties().getAbsoluteKey(PROP_KEY_NODE_SERVICE), "Invalid parameter value: " + environment.getNodeProperties().getAbsoluteKey(PROP_KEY_NODE_SERVICE));
			
			NodeProperties nodeServiceProperties = environment.getNodeProperties().getNestedProperty(PROP_KEY_NODE_SERVICE, nodeServiceKey);
			List<String> queueKeys = nodeServiceProperties.getStringListProperty(PROP_KEY_QUEUES);
			if (queueKeys == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, nodeServiceProperties.getAbsoluteKey(PROP_KEY_QUEUES), "Invalid parameter value: " + nodeServiceProperties.getAbsoluteKey(PROP_KEY_QUEUES) + ".");
			
			eventQueuesProcessingInfo = new EventQueueProcessingInfo[queueKeys.size()];
			
			for (int queueIndex = 0; queueIndex < queueKeys.size(); queueIndex++) {
	
				if (queueKeys.get(queueIndex) == null || queueKeys.get(queueIndex).trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, nodeServiceProperties.getAbsoluteKey(PROP_KEY_QUEUES), "Invalid parameter value: " + nodeServiceProperties.getAbsoluteKey(PROP_KEY_QUEUES));
				NodeProperties queueProperties = nodeServiceProperties.getNestedProperty(PROP_KEY_QUEUES, queueKeys.get(queueIndex));
				NodeProperties threadPoolProperties = nodeServiceProperties.getNestedProperty(PROP_KEY_THREAD_POOL, queueKeys.get(queueIndex));
				
				//read thread pool parameters for the queue:
				int poolSize = (Integer) threadPoolProperties.getProperty(PROP_KEY_CORE_POOL_SIZE, MappedType.INT);
				int keepAliveTimeSec = (Integer) threadPoolProperties.getProperty(PROP_KEY_KEEP_ALIVE_TIME_SEC, MappedType.INT);
				ThreadPoolInfo threadPoolInfo = new ThreadPoolInfo(poolSize, keepAliveTimeSec);

				//get the event types for the queue:
				List<String> eventTypeKeys = queueProperties.getStringListProperty(PROP_KEY_EVENT_TYPES);
				if (eventTypeKeys == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, queueProperties.getAbsoluteKey(PROP_KEY_EVENT_TYPES), "Invalid parameter value: " + queueProperties.getAbsoluteKey(PROP_KEY_EVENT_TYPES) + ".");
				EventType[] eventTypes = new EventType[eventTypeKeys.size()];
				for (int eventTypeIndex = 0; eventTypeIndex < eventTypeKeys.size(); eventTypeIndex++) {
					if (eventTypeKeys.get(eventTypeIndex) == null || eventTypeKeys.get(eventTypeIndex).trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, queueProperties.getAbsoluteKey(PROP_KEY_EVENT_TYPES), "Invalid parameter value: " + queueProperties.getAbsoluteKey(PROP_KEY_EVENT_TYPES));
					NodeProperties eventTypeProperties = queueProperties.getNestedProperty(PROP_KEY_EVENT_TYPES, eventTypeKeys.get(eventTypeIndex));
					EventCategory ec = (EventCategory) eventTypeProperties.getEnumProperty(PROP_KEY_EVENT_CATEGORY, EventCategory.class);
					String eventTypeKey = eventTypeProperties.getProperty(PROP_KEY_EVENT_TYPE_KEY);
					EventType et = new EventType(ec, eventTypeKey);
					eventTypes[eventTypeIndex] = et;
				}
				
				
				//get the wakeable parameter for the queue
				boolean wakeable = (Boolean) queueProperties.getProperty(PROP_KEY_WAKEABLE, MappedType.BOOLEAN);
				
				EventQueueProcessingInfo eqpi = new EventQueueProcessingInfo(threadPoolInfo, eventTypes, wakeable);
				eventQueuesProcessingInfo[queueIndex] = eqpi;
				
			}
			
		
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, "Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		return initialize(nodeService, environment, nodeId, nodeIdString, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, eventQueuesProcessingInfo, errorCallback, errorCallbackArg);
	}


	
	
	
	public static SchedulingMultiQueueNodeService initialize(Environment environment, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventQueueProcessingInfo[] eventQueuesProcessingInfo, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initialize(null, environment, null, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, eventQueuesProcessingInfo, errorCallback, errorCallbackArg);
	}
	
	public static SchedulingMultiQueueNodeService initialize(Environment environment, NodeId nodeId, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventQueueProcessingInfo[] eventQueuesProcessingInfo, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initialize(null, environment, nodeId, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, eventQueuesProcessingInfo, errorCallback, errorCallbackArg);
	}
	
	public static SchedulingMultiQueueNodeService initialize(Environment environment, String nodeIdString, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventQueueProcessingInfo[] eventQueuesProcessingInfo, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initialize(null, environment, null, nodeIdString, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, eventQueuesProcessingInfo, errorCallback, errorCallbackArg);
	}
	
	@SuppressWarnings("unchecked")
	protected static SchedulingMultiQueueNodeService initialize(SchedulingMultiQueueNodeService nodeService, Environment environment, NodeId nodeId, String nodeIdString, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventQueueProcessingInfo[] eventQueuesProcessingInfo, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		
		devLog.info("Initializing multiple queue node service.");
		userLog.info("Initializing multiple queue node service.");
		
		if (nodeService == null) {
			throw new InitializationException(InitializationException.Error.NODE_SERVICE_INITIALIZATION_ERROR, null, "The nodeService argument is null.");
		}
		
		
		//properties:
		NodeProperties properties = environment.getNodeProperties();
		
		
		

		//queues:
		
		devLog.info("Initializing event queues.");
		userLog.info("Initializing event queues.");
		
		ArrayList<LinkedBlockingQueue<Event>> queues = new ArrayList<LinkedBlockingQueue<Event>>(eventQueuesProcessingInfo.length);
		ArrayList<ThreadPoolInfo> threadPoolInfos = new ArrayList<ThreadPoolInfo>(eventQueuesProcessingInfo.length);
		Set<EventType> eventTypeSet = new HashSet<EventType>();
		nodeService.eventQueues = new HashMap<EventType, LinkedBlockingQueue<Event>>();
		nodeService.wakeableManagersByEventType = new HashMap<EventType, WakeableManager>();
		nodeService.notifyingQueuesList = new ArrayList<NotifyingLinkedBlockingQueue<Event>>();
		for (EventQueueProcessingInfo eqpi : eventQueuesProcessingInfo) {
			if (eqpi == null) continue;
			
			LinkedBlockingQueue<Event> queue;
			
			if (eqpi.getWakeable()) {
				queue = new NotifyingLinkedBlockingQueue<Event>();
				nodeService.notifyingQueuesList.add((NotifyingLinkedBlockingQueue<Event>) queue);
			}
			else {
				queue = new LinkedBlockingQueue<Event>();
			}
			
			for (EventType et : eqpi.getEventTypes()) {
				if (eventTypeSet.contains(et)) {
					//error, event Type already registered
					throw new InitializationException(InitializationException.Error.EVENT_PROCESSOR_INITIALIZATION_ERROR, null, "A queue for event type " + et + " is already covered by another queue.");
				}
				
				eventTypeSet.add(et);
				
				nodeService.eventQueues.put(et, queue);

			}
			
			queues.add(queue);
			threadPoolInfos.add(eqpi.getThreadPoolInfo());
		
			//wakeable managers will be set when creating the event processor (event processor runnable instances)
		
		}
		
		
		
		//event processor:
		
		EventQueueSchedulerProcessor eventProcessor = new EventQueueSchedulerProcessor(); 
		eventProcessor.initialize(environment, (BlockingQueue<Event>[])(queues.toArray(new BlockingQueue[0])), eventQueuesProcessingInfo, errorCallback, errorCallbackArg);
		//(ThreadPoolInfo[])(threadPoolInfos.toArray(new ThreadPoolInfo[0]))
		nodeService.eventProcessor = eventProcessor;
		
		for (Entry<EventType, LinkedBlockingQueue<Event>> entry : nodeService.eventQueues.entrySet()) {
			nodeService.wakeableManagersByEventType.put(entry.getKey(), eventProcessor.getWakeableManagerByQueue(entry.getValue()));

		}
		
		//event scheduler -> event processor
		EventScheduler eventScheduler = eventProcessor;
		
		
		
		//node:

		nodeService.nodeProxyService = nodeService.initializeNodeProxyService(nodeId, nodeIdString, networkAddress, environment, nodeService.eventQueues, eventScheduler);
		
		
		
		
		//message receiver:
		
		EventType receiveMessageEventType = new EventType(EventCategory.receiveMessageEvent);
		
		String messageReceiverKey = properties.getProperty(HyCubeNodeProxyService.PROP_KEY_MESSAGE_RECEIVER);
		if (messageReceiverKey == null || messageReceiverKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(HyCubeNodeProxyService.PROP_KEY_MESSAGE_RECEIVER), "Invalid parameter value: " + properties.getAbsoluteKey(HyCubeNodeProxyService.PROP_KEY_MESSAGE_RECEIVER));
		NodeProperties messageReceiverProperties = properties.getNestedProperty(HyCubeNodeProxyService.PROP_KEY_MESSAGE_RECEIVER, messageReceiverKey);
		String messageReceiverClass = messageReceiverProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
		
		WakeableMessageReceiver messageReceiver;
		try {
			messageReceiver = (WakeableMessageReceiver) ClassInstanceLoader.newInstance(messageReceiverClass, WakeableMessageReceiver.class);
		} catch (ClassInstanceLoadException e) {
			throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, messageReceiverClass, "An error occured while creating the wakeable message receiver instance.", e);
		}
		
		try {
			messageReceiver.initialize(environment, (NotifyingBlockingQueue<Event>) nodeService.eventQueues.get(receiveMessageEventType), nodeService.wakeableManagersByEventType.get(receiveMessageEventType), messageReceiverProperties);
		} catch (MessageReceiverException e) {
			throw new InitializationException(InitializationException.Error.MESSAGE_RECEIVER_INITIALIZATION_ERROR, null, "An exception thrown while initializing the message receiver.", e);
		}
		
		try {
			messageReceiver.registerNetworkAdapter(nodeService.getNode().getNetworkAdapter());
		} catch (MessageReceiverException e) {
			throw new InitializationException(InitializationException.Error.MESSAGE_RECEIVER_INITIALIZATION_ERROR, null, "An exception thrown while registering the network adapter for the message receiver.", e);
		}
		
		nodeService.messageReceiver = messageReceiver;
		
		
		
		//start:
		
		devLog.info("Starting node service.");
		userLog.info("Starting node service.");
		
		messageReceiver.startMessageReceiver();
		eventProcessor.start();

//		if (bootstrapNodeAddress != null) {
			nodeService.join(bootstrapNodeAddress, joinCallback, callbackArg);
//		}
		
		nodeService.initialized = true;
		
		devLog.info("Multiple queue node service initialized.");
		userLog.info("Multiple queue node service initialized.");
		
		
		return nodeService;
		
		
	}
	
	
	@Override
	public void discard() {
		
		devLog.info("Discarding multiple queue node service.");
		userLog.info("Discarding multiple queue node service.");
		
		//discard the notifying queues
		for (NotifyingLinkedBlockingQueue<Event> q : notifyingQueuesList) {
			q.discard();
		}
		
		eventProcessor.stop();
		eventProcessor.clear();
		try {
			messageReceiver.discard();
		} catch (MessageReceiverException e) {
			devLog.warn("MessageReceiverException thrown while discarding the node service.", e);
		}
		
		nodeProxyService.discard();
		
		this.eventQueues = null;
		this.wakeableManagersByEventType = null;
		
		this.notifyingQueuesList = null;
		
		this.eventProcessor = null;
		this.messageReceiver = null;
		
		this.nodeProxyService = null;
		
		initialized = false;
		discarded = true;
		
		devLog.info("Discarded multiple queue node service.");
		userLog.info("Discarded multiple queue node service.");
		
	}
	
	
	protected abstract NodeProxyService initializeNodeProxyService(
			NodeId nodeId, String nodeIdString, String networkAddress,
			Environment environment,
			Map<EventType, LinkedBlockingQueue<Event>> eventQueues,
			EventScheduler eventScheduler) 
					throws InitializationException;
	
	
	
	
}
