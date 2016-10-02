package net.hycube;

import java.util.HashMap;
import java.util.Map;
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
import net.hycube.eventprocessing.EventQueueProcessor;
import net.hycube.eventprocessing.EventScheduler;
import net.hycube.eventprocessing.EventType;
import net.hycube.eventprocessing.ThreadPoolEventQueueProcessor;
import net.hycube.eventprocessing.ThreadPoolInfo;
import net.hycube.join.JoinCallback;
import net.hycube.logging.LogHelper;
import net.hycube.transport.MessageReceiver;
import net.hycube.transport.MessageReceiverException;
import net.hycube.utils.ClassInstanceLoadException;
import net.hycube.utils.ClassInstanceLoader;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public abstract class SingleQueueNodeServiceNonWakeable implements NodeService {

	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(SingleQueueNodeServiceNonWakeable.class); 
	
	
	public static final String PROP_KEY_THREAD_POOL = "ThreadPool";
	public static final String PROP_KEY_CORE_POOL_SIZE = "PoolSize";
	public static final String PROP_KEY_KEEP_ALIVE_TIME_SEC = "KeepAliveTimeSec";
	
	
	protected NodeProxyService nodeProxyService;
	protected EventQueueProcessor eventProcessor;
	protected MessageReceiver messageReceiver;
	protected LinkedBlockingQueue<Event> queue;
	
	protected EventScheduler eventScheduler;
	
	protected boolean initialized = false;
	protected boolean discarded = false;
	

	
	public static SingleQueueNodeServiceNonWakeable initializeFromConf(Environment environment, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(null, null, environment, null, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, errorCallback, errorCallbackArg);
	}
	
	public static SingleQueueNodeServiceNonWakeable initializeFromConf(Environment environment, NodeId nodeId, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(null, null, environment, nodeId, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, errorCallback, errorCallbackArg);
	}
	
	public static SingleQueueNodeServiceNonWakeable initializeFromConf(Environment environment, String nodeIdString, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(null, null, environment, null, nodeIdString, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, errorCallback, errorCallbackArg);
	}
	
	
	public static SingleQueueNodeServiceNonWakeable initializeFromConf(String nodeServiceConfKey, Environment environment, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(nodeServiceConfKey, null, environment, null, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, errorCallback, errorCallbackArg);
	}
	
	public static SingleQueueNodeServiceNonWakeable initializeFromConf(String nodeServiceConfKey, Environment environment, NodeId nodeId, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(nodeServiceConfKey, null, environment, nodeId, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, errorCallback, errorCallbackArg);
	}
	
	public static SingleQueueNodeServiceNonWakeable initializeFromConf(String nodeServiceConfKey, Environment environment, String nodeIdString, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(nodeServiceConfKey, null, environment, null, nodeIdString, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, errorCallback, errorCallbackArg);
	}
	
	protected static SingleQueueNodeServiceNonWakeable initializeFromConf(String nodeServiceConfKey, SingleQueueNodeServiceNonWakeable nodeService, Environment environment, NodeId nodeId, String nodeIdString, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {

		int poolSize = 0;
		int keepAliveTimeSec = 0;
		
		try {
			String nodeServiceKey;
			if (nodeServiceConfKey != null && (!nodeServiceConfKey.trim().isEmpty())) nodeServiceKey = nodeServiceConfKey;
			else nodeServiceKey = environment.getNodeProperties().getProperty(PROP_KEY_NODE_SERVICE);
			if (nodeServiceKey == null || nodeServiceKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, environment.getNodeProperties().getAbsoluteKey(PROP_KEY_NODE_SERVICE), "Invalid parameter value: " + environment.getNodeProperties().getAbsoluteKey(PROP_KEY_NODE_SERVICE));
			
			NodeProperties nodeServiceProperties = environment.getNodeProperties().getNestedProperty(PROP_KEY_NODE_SERVICE, nodeServiceKey);
			NodeProperties threadPoolProperties = nodeServiceProperties.getNestedProperty(PROP_KEY_THREAD_POOL);
			
			poolSize = (Integer) threadPoolProperties.getProperty(PROP_KEY_CORE_POOL_SIZE, MappedType.INT);
			keepAliveTimeSec = (Integer) threadPoolProperties.getProperty(PROP_KEY_KEEP_ALIVE_TIME_SEC, MappedType.INT);

			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, "Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		ThreadPoolInfo threadPoolInfo = new ThreadPoolInfo(poolSize, keepAliveTimeSec);
		
		return initialize(nodeService, environment, nodeId, nodeIdString, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, threadPoolInfo, errorCallback, errorCallbackArg);
	}

	
	
	
	
	public static SingleQueueNodeServiceNonWakeable initialize(Environment environment, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, ThreadPoolInfo threadPoolInfo, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initialize(null, environment, null, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, threadPoolInfo, errorCallback, errorCallbackArg);
	}
	
	public static SingleQueueNodeServiceNonWakeable initialize(Environment environment, NodeId nodeId, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, ThreadPoolInfo threadPoolInfo, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initialize(null, environment, nodeId, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, threadPoolInfo, errorCallback, errorCallbackArg);
	}
	
	public static SingleQueueNodeServiceNonWakeable initialize(Environment environment, String nodeIdString, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, ThreadPoolInfo threadPoolInfo, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initialize(null, environment, null, nodeIdString, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, threadPoolInfo, errorCallback, errorCallbackArg);
	}
	
	protected static SingleQueueNodeServiceNonWakeable initialize(SingleQueueNodeServiceNonWakeable nodeService, Environment environment, NodeId nodeId, String nodeIdString, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, ThreadPoolInfo threadPoolInfo, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		
		devLog.info("Initializing single queue node service.");
		userLog.info("Initializing single queue node service.");
		
		if (nodeService == null) {
			throw new InitializationException(InitializationException.Error.NODE_SERVICE_INITIALIZATION_ERROR, null, "The nodeService argument is null.");
		}
		
		
		
		//properties:
		NodeProperties properties = environment.getNodeProperties();
		
				
		
		//queue:
		
		LinkedBlockingQueue<Event> queue = new LinkedBlockingQueue<Event>();
		
		@SuppressWarnings("unchecked")
		LinkedBlockingQueue<Event>[] queues = (LinkedBlockingQueue<Event>[]) new LinkedBlockingQueue<?>[] {queue};

		
		Map<EventType, LinkedBlockingQueue<Event>> queuesMap = new HashMap<EventType, LinkedBlockingQueue<Event>>();
		
		queuesMap.put(new EventType(EventCategory.receiveMessageEvent), queue);
		queuesMap.put(new EventType(EventCategory.processReceivedMessageEvent), queue);
		
		queuesMap.put(new EventType(EventCategory.pushMessageEvent), queue);
		queuesMap.put(new EventType(EventCategory.pushSystemMessageEvent), queue);
		
		queuesMap.put(new EventType(EventCategory.processAckCallbackEvent), queue); 
		queuesMap.put(new EventType(EventCategory.processMsgReceivedCallbackEvent), queue);
		
		queuesMap.put(new EventType(EventCategory.executeBackgroundProcessEvent), queue);
		queuesMap.put(new EventType(EventCategory.extEvent), queue);
		
		nodeService.queue = queue;



		//event processor:
		
		ThreadPoolInfo queuePoolInfo = new ThreadPoolInfo(threadPoolInfo.getPoolSize(), threadPoolInfo.getKeepAliveTimeSec());
		ThreadPoolInfo[] threadPoolInfos = new ThreadPoolInfo[] {queuePoolInfo};

		ThreadPoolEventQueueProcessor eventProcessor = new ThreadPoolEventQueueProcessor();
		eventProcessor.initialize(queues, threadPoolInfos, errorCallback, errorCallbackArg);
		
		nodeService.eventProcessor = eventProcessor;
		
		
		
		//event scheduler:
		
		nodeService.eventScheduler = environment.getEventScheduler();
		
		
		
		//node:
		
		nodeService.nodeProxyService = nodeService.initializeNodeProxyService(nodeId, nodeIdString, networkAddress, environment, queuesMap, nodeService.eventScheduler);
		
		
		
		//message receiver:

		String messageReceiverKey = properties.getProperty(HyCubeNodeProxyService.PROP_KEY_MESSAGE_RECEIVER);
		if (messageReceiverKey == null || messageReceiverKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(HyCubeNodeProxyService.PROP_KEY_MESSAGE_RECEIVER), "Invalid parameter value: " + properties.getAbsoluteKey(HyCubeNodeProxyService.PROP_KEY_MESSAGE_RECEIVER));
		NodeProperties messageReceiverProperties = properties.getNestedProperty(HyCubeNodeProxyService.PROP_KEY_MESSAGE_RECEIVER, messageReceiverKey);
		String messageReceiverClass = messageReceiverProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
		
		MessageReceiver messageReceiver;
		try {
			messageReceiver = (MessageReceiver) ClassInstanceLoader.newInstance(messageReceiverClass, MessageReceiver.class);
		} catch (ClassInstanceLoadException e) {
			throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, messageReceiverClass, "An error occured while creating the message receiver instance.", e);
		}
		
		try {
			messageReceiver.initialize(environment, queue, messageReceiverProperties);
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
		messageReceiver.startMessageReceiver();
		eventProcessor.start();

//		if (bootstrapNodeAddress != null) {
			nodeService.join(bootstrapNodeAddress, joinCallback, callbackArg);
//		}
		
		nodeService.initialized = true;
		
		devLog.info("Initialized single queue node service.");
		userLog.info("Initialized single queue node service.");
		
		return nodeService;
		
		
	}
	
	
	@Override
	public void discard() {
		
		devLog.info("Discarding single queue node service.");
		userLog.info("Discarding single queue node service.");
		
		eventProcessor.stop();
		eventProcessor.clear();
		try {
			messageReceiver.discard();
		} catch (MessageReceiverException e) {
			devLog.warn("MessageReceiverException thrown while discarding the node service.", e);
		}
		
		nodeProxyService.discard();
		
		this.queue = null;
		this.eventProcessor = null;
		this.messageReceiver = null;
		this.nodeProxyService = null;
		
		initialized = false;
		discarded = true;
		
		devLog.info("Discarded single queue node service.");
		userLog.info("Discarded single queue node service.");
		
	}
	
	
	protected abstract NodeProxyService initializeNodeProxyService(
			NodeId nodeId, String nodeIdString, String networkAddress,
			Environment environment,
			Map<EventType, LinkedBlockingQueue<Event>> eventQueues,
			EventScheduler eventScheduler) 
					throws InitializationException;
	
	
	
}
