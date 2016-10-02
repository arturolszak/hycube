package net.hycube;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.InitializationException;
import net.hycube.environment.Environment;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.environment.NodeProperties;
import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventCategory;
import net.hycube.eventprocessing.EventProcessingErrorCallback;
import net.hycube.eventprocessing.EventQueueProcessingInfo;
import net.hycube.eventprocessing.EventQueueWakeableManager;
import net.hycube.eventprocessing.EventType;
import net.hycube.eventprocessing.NotifyingLinkedBlockingQueue;
import net.hycube.eventprocessing.NotifyingQueue;
import net.hycube.eventprocessing.ThreadPoolEventQueueProcessor;
import net.hycube.eventprocessing.ThreadPoolInfo;
import net.hycube.eventprocessing.WakeableManager;
import net.hycube.eventprocessing.WakeableManagerQueueListener;
import net.hycube.logging.LogHelper;
import net.hycube.transport.MessageReceiver;
import net.hycube.transport.MessageReceiverException;
import net.hycube.utils.HashMapUtils;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public abstract class MultiQueueMultipleNodeService extends AbstractMultipleNodeService {

	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(MultiQueueMultipleNodeService.class); 
	
	

	public static MultipleNodeService initializeFromConf(Environment environment, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(null, null, environment, DEFAULT_INIIAL_NODE_PROXY_SERVICES_COLLECTIONS_SIZE, errorCallback, errorCallbackArg);
	}
	
	public static MultipleNodeService initializeFromConf(Environment environment, int initialNodeProxyServicesCollectionsSize, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(null, null, environment, initialNodeProxyServicesCollectionsSize, errorCallback, errorCallbackArg);
	}
	
	public static MultipleNodeService initializeFromConf(String nodeServiceConfKey, Environment environment, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(nodeServiceConfKey, null, environment, DEFAULT_INIIAL_NODE_PROXY_SERVICES_COLLECTIONS_SIZE, errorCallback, errorCallbackArg);
	}
	
	public static MultipleNodeService initializeFromConf(String nodeServiceConfKey, Environment environment, int initialNodeProxyServicesCollectionsSize, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(nodeServiceConfKey, null, environment, initialNodeProxyServicesCollectionsSize, errorCallback, errorCallbackArg);
	}

	protected static MultipleNodeService initializeFromConf(String nodeServiceConfKey, MultiQueueMultipleNodeService nodeService, Environment environment, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(nodeServiceConfKey, nodeService, environment, DEFAULT_INIIAL_NODE_PROXY_SERVICES_COLLECTIONS_SIZE, errorCallback, errorCallbackArg);
	}
	
	protected static MultipleNodeService initializeFromConf(String nodeServiceConfKey, MultiQueueMultipleNodeService nodeService, Environment environment, int initialNodeProxyServicesCollectionsSize, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {

		EventQueueProcessingInfo[] eventQueuesProcessingInfo;
		
		try {
			String nodeServiceKey;
			if (nodeServiceConfKey != null && (!nodeServiceConfKey.trim().isEmpty())) nodeServiceKey = nodeServiceConfKey;
			else nodeServiceKey = environment.getNodeProperties().getProperty(NodeService.PROP_KEY_NODE_SERVICE);
			if (nodeServiceKey == null || nodeServiceKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, environment.getNodeProperties().getAbsoluteKey(NodeService.PROP_KEY_NODE_SERVICE), "Invalid parameter value: " + environment.getNodeProperties().getAbsoluteKey(NodeService.PROP_KEY_NODE_SERVICE));
			
			NodeProperties nodeServiceProperties = environment.getNodeProperties().getNestedProperty(NodeService.PROP_KEY_NODE_SERVICE, nodeServiceKey);
			List<String> queueKeys = nodeServiceProperties.getStringListProperty(PROP_KEY_QUEUES);
			if (queueKeys == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, nodeServiceProperties.getAbsoluteKey(PROP_KEY_QUEUES), "Invalid parameter value: " + nodeServiceProperties.getAbsoluteKey(PROP_KEY_QUEUES) + ".");
			
			eventQueuesProcessingInfo = new EventQueueProcessingInfo[queueKeys.size()];
			
			for (int queueIndex = 0; queueIndex < queueKeys.size(); queueIndex++) {
				
				if (queueKeys.get(queueIndex) == null || queueKeys.get(queueIndex).trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, nodeServiceProperties.getAbsoluteKey(PROP_KEY_QUEUES), "Invalid parameter value: " + nodeServiceProperties.getAbsoluteKey(PROP_KEY_QUEUES));
				NodeProperties queueProperties = nodeServiceProperties.getNestedProperty(PROP_KEY_QUEUES, queueKeys.get(queueIndex));
				NodeProperties threadPoolProperties = queueProperties.getNestedProperty(PROP_KEY_THREAD_POOL);
				
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
		
		
		
		return initialize(nodeService, environment, eventQueuesProcessingInfo, initialNodeProxyServicesCollectionsSize, errorCallback, errorCallbackArg);
		
		
	}

	
	
	
	public static MultipleNodeService initialize(Environment environment, EventQueueProcessingInfo[] eventQueuesProcessingInfo, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initialize(null, environment, eventQueuesProcessingInfo, DEFAULT_INIIAL_NODE_PROXY_SERVICES_COLLECTIONS_SIZE, errorCallback, errorCallbackArg);
	}
	
	
	public static MultipleNodeService initialize(Environment environment, EventQueueProcessingInfo[] eventQueuesProcessingInfo, int initialNodeProxyServicesCollectionsSize, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initialize(null, environment, eventQueuesProcessingInfo, initialNodeProxyServicesCollectionsSize, errorCallback, errorCallbackArg);
	}
	
	
	protected static MultipleNodeService initialize(MultiQueueMultipleNodeService multiNodeService, Environment environment, EventQueueProcessingInfo[] eventQueuesProcessingInfo, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initialize(multiNodeService, environment, eventQueuesProcessingInfo, DEFAULT_INIIAL_NODE_PROXY_SERVICES_COLLECTIONS_SIZE, errorCallback, errorCallbackArg);
	}
	
	
	@SuppressWarnings("unchecked")
	protected static MultipleNodeService initialize(MultiQueueMultipleNodeService multiNodeService, Environment environment, EventQueueProcessingInfo[] eventQueuesProcessingInfo, int initialNodeProxyServicesCollectionsSize, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {

		userLog.info("Initializing MultipleNodeService...");
		devLog.info("Initializing MultipleNodeService...");
		
		if (multiNodeService == null) {
			throw new InitializationException(InitializationException.Error.NODE_SERVICE_INITIALIZATION_ERROR, null, "The multiNodeService argument is null.");
		}
		
		//environment:
		multiNodeService.environment = environment;
		multiNodeService.properties = environment.getNodeProperties();
		
		
		//queues:
		
		devLog.info("Initializing event queues...");
		userLog.info("Initializing event queues...");
		
		ArrayList<LinkedBlockingQueue<Event>> queues = new ArrayList<LinkedBlockingQueue<Event>>(eventQueuesProcessingInfo.length);
		ArrayList<ThreadPoolInfo> threadPoolInfos = new ArrayList<ThreadPoolInfo>(eventQueuesProcessingInfo.length);
		Set<EventType> eventTypesSet = new HashSet<EventType>();
		multiNodeService.eventQueues = new HashMap<EventType, LinkedBlockingQueue<Event>>();
		multiNodeService.wakeableManagers = new HashMap<EventType, WakeableManager>();
		for (EventQueueProcessingInfo eqpi : eventQueuesProcessingInfo) {
			if (eqpi == null) continue;
			
			LinkedBlockingQueue<Event> queue;
			WakeableManager wakeableManager;
			if (eqpi.getWakeable()) {
				wakeableManager = new EventQueueWakeableManager(eqpi.getThreadPoolInfo().getPoolSize());
				queue = new NotifyingLinkedBlockingQueue<Event>(wakeableManager.getWakeableManagerLock());
				WakeableManagerQueueListener<Event> wakeupQueueListener = new WakeableManagerQueueListener<Event>(wakeableManager);
				((NotifyingLinkedBlockingQueue<Event>)queue).addListener(wakeupQueueListener);
			}
			else {
				queue = new LinkedBlockingQueue<Event>();
				wakeableManager = null;
			}
			
			for (EventType et : eqpi.getEventTypes()) {
				if (eventTypesSet.contains(et)) {
					//error, event Type already registered
					throw new InitializationException(InitializationException.Error.EVENT_PROCESSOR_INITIALIZATION_ERROR, null, "A queue for event type " + et + " is already covered by another queue.");
				}
				eventTypesSet.add(et);
				
				multiNodeService.eventQueues.put(et, queue);
				multiNodeService.wakeableManagers.put(et, wakeableManager);
				
			}
			
			queues.add(queue);
			threadPoolInfos.add(eqpi.getThreadPoolInfo());
		}
		
		
		//event processor:
		
		devLog.info("Initializing event processor.");
		userLog.info("Initializing event processor.");
		
		ThreadPoolEventQueueProcessor eventProcessor = new ThreadPoolEventQueueProcessor(); 
		eventProcessor.initialize((BlockingQueue<Event>[])queues.toArray(new BlockingQueue<?>[queues.size()]), (ThreadPoolInfo[])threadPoolInfos.toArray(new ThreadPoolInfo[threadPoolInfos.size()]), errorCallback, errorCallbackArg);
		multiNodeService.eventProcessor = eventProcessor;
		
		
		
		//event scheduler
		
		devLog.info("Initializing event scheduler.");
		userLog.info("Initializing event scheduler.");
		
		multiNodeService.eventScheduler = environment.getEventScheduler();
		
		
		
		
		//initialize collections:
		multiNodeService.messageReceivers = new HashSet<MessageReceiver>();
		multiNodeService.messageReceiverNodeServiceMap = new HashMap<MessageReceiver, HashSet<NodeProxyService>>();
		multiNodeService.nodeProxyServices = new HashSet<NodeProxyService>(HashMapUtils.getHashMapCapacityForElementsNum(initialNodeProxyServicesCollectionsSize, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		multiNodeService.nodeServiceMessageReceiverMap = new HashMap<NodeProxyService, MessageReceiver>(HashMapUtils.getHashMapCapacityForElementsNum(initialNodeProxyServicesCollectionsSize, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		
		
		//start:
		devLog.info("Starting event processor.");
		userLog.info("Starting event processor.");
		
		eventProcessor.start();

		
		multiNodeService.initialized = true;
		
		userLog.info("MultipleNodeService initialized.");
		devLog.info("MultipleNodeService initialized.");

		return multiNodeService;

		
	}
	
	



	@Override
	@SuppressWarnings("unchecked")
	public void discard() {
		
		devLog.info("Discarding MultipleNodeService.");
		userLog.info("Discarding MultipleNodeService.");
		
		//discard the notifying queues
		for (LinkedBlockingQueue<Event> q : eventQueues.values()) {
			if (q instanceof NotifyingQueue) ((NotifyingQueue<Event>)q).discard();
		}

		//discard the WakeableManager:
		for (WakeableManager wm : wakeableManagers.values()) {
			if (wm != null) wm.discard();
		}

		eventProcessor.stop();
		eventProcessor.clear();

		//discard message receivers:
		for (MessageReceiver mr : messageReceivers) {
			try {
				mr.discard();
			} catch (MessageReceiverException e) {
				devLog.warn("MessageReceiverException thrown while discarding the node service.", e);
			}
		}

		//discard node instances:
		for (NodeProxyService ns : nodeProxyServices) {
			ns.discard();
		}

		this.eventQueues = null;
		this.wakeableManagers = null;

		this.eventProcessor = null;
		this.messageReceivers = null;
		this.nodeProxyServices = null;
		this.messageReceiverNodeServiceMap = null;
		this.nodeServiceMessageReceiverMap = null;
						
		initialized = false;
		discarded = true;

		environment = null;
		
		devLog.info("Discarded MultipleNodeService.");
		userLog.info("Discarded MultipleNodeService.");

	}

	

	
}
