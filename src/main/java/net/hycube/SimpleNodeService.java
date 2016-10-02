package net.hycube;

import java.util.ArrayList;
import java.util.List;

import net.hycube.core.InitializationException;
import net.hycube.core.NodeId;
import net.hycube.environment.Environment;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.environment.NodeProperties;
import net.hycube.eventprocessing.EventCategory;
import net.hycube.eventprocessing.EventProcessingErrorCallback;
import net.hycube.eventprocessing.EventQueueProcessingInfo;
import net.hycube.eventprocessing.EventType;
import net.hycube.eventprocessing.ThreadPoolInfo;
import net.hycube.join.JoinCallback;
import net.hycube.logging.LogHelper;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public abstract class SimpleNodeService extends MultiQueueNodeService implements NodeService {

	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(SimpleNodeService.class); 
	
	public static final String PROP_KEY_THREAD_POOL = "ThreadPool";
	public static final String PROP_KEY_BLOCKING_EXT_EVENTS_NUM = "BlockingExtEventsNum";
	public static final String PROP_KEY_WAKEUP = "Wakeup";
	
	
	
	public static final int PROCESSING_THREAD_KEEP_ALIVE_TIME = 60;
	public static final int DEFAULT_BLOCKING_EXT_EVENTS_NUM = 0;
	
	
	protected NodeProperties properties;
	
	
	
	
	
	public static SimpleNodeService initializeFromConf(Environment environment, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(null, null, environment, null, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, errorCallback, errorCallbackArg);
	}
	
	public static SimpleNodeService initializeFromConf(Environment environment, NodeId nodeId, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(null, null, environment, nodeId, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, errorCallback, errorCallbackArg);
	}
	
	public static SimpleNodeService initializeFromConf(Environment environment, String nodeIdString, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(null, null, environment, null, nodeIdString, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, errorCallback, errorCallbackArg);
	}
	
	
	public static SimpleNodeService initializeFromConf(String nodeServiceConfKey, Environment environment, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(nodeServiceConfKey, null, environment, null, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, errorCallback, errorCallbackArg);
	}
	
	public static SimpleNodeService initializeFromConf(String nodeServiceConfKey, Environment environment, NodeId nodeId, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(nodeServiceConfKey, null, environment, nodeId, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, errorCallback, errorCallbackArg);
	}
	
	public static SimpleNodeService initializeFromConf(String nodeServiceConfKey, Environment environment, String nodeIdString, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(nodeServiceConfKey, null, environment, null, nodeIdString, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, errorCallback, errorCallbackArg);
	}
	
	protected static SimpleNodeService initializeFromConf(String nodeServiceConfKey, SimpleNodeService nodeService, Environment environment, NodeId nodeId, String nodeIdString, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		
		int blockingExtEventsNum;
		boolean wakeup;
		
		try {
			String nodeServiceKey;
			if (nodeServiceConfKey != null && (!nodeServiceConfKey.trim().isEmpty())) nodeServiceKey = nodeServiceConfKey;
			else nodeServiceKey = environment.getNodeProperties().getProperty(PROP_KEY_NODE_SERVICE);
			if (nodeServiceKey == null || nodeServiceKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, environment.getNodeProperties().getAbsoluteKey(PROP_KEY_NODE_SERVICE), "Invalid parameter value: " + environment.getNodeProperties().getAbsoluteKey(PROP_KEY_NODE_SERVICE));
			
			NodeProperties nodeServiceProperties = environment.getNodeProperties().getNestedProperty(PROP_KEY_NODE_SERVICE, nodeServiceKey);
			NodeProperties threadPoolProperties = nodeServiceProperties.getNestedProperty(PROP_KEY_THREAD_POOL);

			blockingExtEventsNum = (Integer) threadPoolProperties.getProperty(PROP_KEY_BLOCKING_EXT_EVENTS_NUM, MappedType.INT);
			wakeup = (Boolean) threadPoolProperties.getProperty(PROP_KEY_WAKEUP, MappedType.BOOLEAN);

			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, "Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		return initialize(nodeService, environment, nodeId, nodeIdString, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, blockingExtEventsNum, wakeup, errorCallback, errorCallbackArg);
	}

	
	
	
	
	
	public static SimpleNodeService initialize(Environment environment, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, 
			boolean wakeup, 
			EventProcessingErrorCallback errorCallback, Object errorCallbackArg) 
					throws InitializationException {
		return initialize(null, environment, null, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, DEFAULT_BLOCKING_EXT_EVENTS_NUM, wakeup, errorCallback, errorCallbackArg);
	}
	
	
	public static SimpleNodeService initialize(Environment environment, NodeId nodeId, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, 
			boolean wakeup, 
			EventProcessingErrorCallback errorCallback, Object errorCallbackArg) 
					throws InitializationException {
		return initialize(null, environment, nodeId, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, DEFAULT_BLOCKING_EXT_EVENTS_NUM, wakeup, errorCallback, errorCallbackArg);
	}
	
	
	public static SimpleNodeService initialize(Environment environment, String nodeIdString, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, 
			boolean wakeup, 
			EventProcessingErrorCallback errorCallback, Object errorCallbackArg) 
					throws InitializationException {
		return initialize(null, environment, null, nodeIdString, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, DEFAULT_BLOCKING_EXT_EVENTS_NUM, wakeup, errorCallback, errorCallbackArg);
	}

	
	
	public static SimpleNodeService initialize(Environment environment, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, 
			int blockingExtEventsNum, boolean wakeup, 
			EventProcessingErrorCallback errorCallback, Object errorCallbackArg) 
					throws InitializationException {
		return initialize(null, environment, null, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, blockingExtEventsNum, wakeup, errorCallback, errorCallbackArg);
	}
	
	
	
	public static SimpleNodeService initialize(Environment environment, NodeId nodeId, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, 
			int blockingExtEventsNum, boolean wakeup, 
			EventProcessingErrorCallback errorCallback, Object errorCallbackArg) 
					throws InitializationException {
		return initialize(null, environment, nodeId, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, blockingExtEventsNum, wakeup, errorCallback, errorCallbackArg);
	}
	
	
	
	public static SimpleNodeService initialize(Environment environment, String nodeIdString, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, 
			int blockingExtEventsNum, boolean wakeup, 
			EventProcessingErrorCallback errorCallback, Object errorCallbackArg) 
					throws InitializationException {
		return initialize(null, environment, null, nodeIdString, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, blockingExtEventsNum, wakeup, errorCallback, errorCallbackArg);
	}
		
	
		

	protected static SimpleNodeService initialize(SimpleNodeService nodeService, Environment environment, NodeId nodeId, String nodeIdString, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, 
			int blockingExtEventsNum, boolean wakeup, 
			EventProcessingErrorCallback errorCallback, Object errorCallbackArg) 
					throws InitializationException {
	
		userLog.info("Initializing SimpleNodeService...");
		devLog.info("Initializing SimpleNodeService...");

		

				
		if (nodeService == null) {
			throw new InitializationException(InitializationException.Error.NODE_SERVICE_INITIALIZATION_ERROR, null, "The nodeService argument is null.");
		}
		

		
		//properties:
		nodeService.properties = environment.getNodeProperties();
		
		
		EventQueueProcessingInfo[] eventQueuesProcessingInfo = null;
		
		
		if (wakeup) {	//all events in one queue
			//one queue, maxPoolThreads = blockingExtEventsNum + 1 (one thread will be almost always blocked on receiving messages)
			
			List<EventType> eventTypes = new ArrayList<EventType>(EventCategory.values().length);
			for (EventCategory ec : EventCategory.values()) {
				EventType et = new EventType(ec);
				eventTypes.add(et);
			}
			
			int maxPoolThreads = blockingExtEventsNum + 1;
			
			ThreadPoolInfo tpi = new ThreadPoolInfo(maxPoolThreads, PROCESSING_THREAD_KEEP_ALIVE_TIME);
			EventQueueProcessingInfo eqpi = new EventQueueProcessingInfo(tpi, (EventType[])eventTypes.toArray(new EventType[eventTypes.size()]), true);
			
			eventQueuesProcessingInfo = new EventQueueProcessingInfo[] {eqpi};
			
		}
		else {	//separate queue for message receiver events
			//two queues:
			//- message receiver event: maxPoolThreads = 1
			//- other events: maxPoolThreads = blockingExtEventsNum + 1
			
			List<EventType> eventTypesOE = new ArrayList<EventType>(EventCategory.values().length);
			for (EventCategory ec : EventCategory.values()) {
				if (ec != EventCategory.receiveMessageEvent) {
					EventType et = new EventType(ec);
					eventTypesOE.add(et);
				}
			}
			
			ThreadPoolInfo tpiMR = new ThreadPoolInfo(1, 0);
			EventQueueProcessingInfo eqpiMR = new EventQueueProcessingInfo(tpiMR, new EventType[] {new EventType(EventCategory.receiveMessageEvent)}, false);
			
			int maxPoolThreadsOE = blockingExtEventsNum + 1;
			
			ThreadPoolInfo tpiOE = new ThreadPoolInfo(maxPoolThreadsOE, PROCESSING_THREAD_KEEP_ALIVE_TIME);
			EventQueueProcessingInfo eqpiOE = new EventQueueProcessingInfo(tpiOE, (EventType[])eventTypesOE.toArray(), false);
			
			eventQueuesProcessingInfo = new EventQueueProcessingInfo[] {eqpiMR, eqpiOE};
		}
		
		
		
		MultiQueueNodeService.initialize(nodeService, environment, nodeId, nodeIdString, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, eventQueuesProcessingInfo, errorCallback, errorCallbackArg);
		
		
		userLog.info("Initialized SimpleNodeService...");
		devLog.info("Initialized SimpleNodeService...");
		
		return nodeService;
		
	}
		
	
}
