package net.hycube;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import net.hycube.core.InitializationException;
import net.hycube.core.NodeId;
import net.hycube.environment.Environment;
import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventProcessingErrorCallback;
import net.hycube.eventprocessing.EventQueueProcessingInfo;
import net.hycube.eventprocessing.EventScheduler;
import net.hycube.eventprocessing.EventType;
import net.hycube.join.JoinCallback;
import net.hycube.transport.MessageReceiver;

public class HyCubeMultiQueueMultipleNodeService extends MultiQueueMultipleNodeService implements HyCubeMultipleNodeService {

//	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
//	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeMultipleNodeService.class); 
	
	
	public static HyCubeMultiQueueMultipleNodeService initializeFromConf(Environment environment, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(null, null, environment, errorCallback, errorCallbackArg);
	}
	
	public static HyCubeMultiQueueMultipleNodeService initializeFromConf(Environment environment, int initialNodeProxyServicesCollectionsSize, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(null, null, environment, initialNodeProxyServicesCollectionsSize, errorCallback, errorCallbackArg);
	}
	
	public static HyCubeMultiQueueMultipleNodeService initializeFromConf(String nodeServiceConfKey, Environment environment, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(nodeServiceConfKey, null, environment, errorCallback, errorCallbackArg);
	}

	public static HyCubeMultiQueueMultipleNodeService initializeFromConf(String nodeServiceConfKey, Environment environment, int initialNodeProxyServicesCollectionsSize, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(nodeServiceConfKey, null, environment, initialNodeProxyServicesCollectionsSize, errorCallback, errorCallbackArg);
	}
	
	protected static HyCubeMultiQueueMultipleNodeService initializeFromConf(String nodeServiceConfKey, HyCubeMultiQueueMultipleNodeService nodeService, Environment environment, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		if (nodeService == null) {
			nodeService = new HyCubeMultiQueueMultipleNodeService();
		}
		return (HyCubeMultiQueueMultipleNodeService) MultiQueueMultipleNodeService.initializeFromConf(nodeServiceConfKey, nodeService, environment, errorCallback, errorCallbackArg);
	}
	
	protected static HyCubeMultiQueueMultipleNodeService initializeFromConf(String nodeServiceConfKey, HyCubeMultiQueueMultipleNodeService nodeService, Environment environment, int initialNodeProxyServicesCollectionsSize, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		if (nodeService == null) {
			nodeService = new HyCubeMultiQueueMultipleNodeService();
		}
		return (HyCubeMultiQueueMultipleNodeService) MultiQueueMultipleNodeService.initializeFromConf(nodeServiceConfKey, nodeService, environment, initialNodeProxyServicesCollectionsSize, errorCallback, errorCallbackArg);
	}

	
	
	
	
	public static HyCubeMultiQueueMultipleNodeService initialize(Environment environment, EventQueueProcessingInfo[] eventQueuesProcessingInfo, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initialize(null, environment, eventQueuesProcessingInfo, errorCallback, errorCallbackArg);
	}
	
	
	public static HyCubeMultiQueueMultipleNodeService initialize(Environment environment, EventQueueProcessingInfo[] eventQueuesProcessingInfo, int initialNodeProxyServicesCollectionsSize, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initialize(null, environment, eventQueuesProcessingInfo, initialNodeProxyServicesCollectionsSize, errorCallback, errorCallbackArg);
	}
	

	protected static HyCubeMultiQueueMultipleNodeService initialize(HyCubeMultiQueueMultipleNodeService multiNodeService, Environment environment, EventQueueProcessingInfo[] eventQueuesProcessingInfo, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		if (multiNodeService == null) {
			multiNodeService = new HyCubeMultiQueueMultipleNodeService();
		}
		return (HyCubeMultiQueueMultipleNodeService) MultiQueueMultipleNodeService.initialize(multiNodeService, environment, eventQueuesProcessingInfo, errorCallback, errorCallbackArg);
	}
	
	protected static HyCubeMultiQueueMultipleNodeService initialize(HyCubeMultiQueueMultipleNodeService multiNodeService, Environment environment, EventQueueProcessingInfo[] eventQueuesProcessingInfo, int initialNodeProxyServicesCollectionsSize, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		if (multiNodeService == null) {
			multiNodeService = new HyCubeMultiQueueMultipleNodeService();
		}
		return (HyCubeMultiQueueMultipleNodeService) MultiQueueMultipleNodeService.initialize(multiNodeService, environment, eventQueuesProcessingInfo, initialNodeProxyServicesCollectionsSize, errorCallback, errorCallbackArg);
	}
	
	
	
	
	
	@Override
	public HyCubeNodeService initializeNode(String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, MessageReceiver messageReceiver) throws InitializationException {
		return initializeNode(null, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, messageReceiver);
	}
	
	@Override
	public HyCubeNodeService initializeNode(NodeId nodeId, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, MessageReceiver messageReceiver) throws InitializationException {
		return initializeNode(nodeId, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, messageReceiver);
	}
	
	@Override
	public HyCubeNodeService initializeNode(String nodeIdString, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, MessageReceiver messageReceiver) throws InitializationException {
		return initializeNode(null, nodeIdString, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, messageReceiver);
	}
	
	@Override
	public HyCubeNodeService initializeNode(NodeId nodeId, String nodeIdString, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, MessageReceiver messageReceiver) throws InitializationException {
		return (HyCubeNodeProxyService) super.initializeNode(nodeId, nodeIdString, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, messageReceiver);
	}
	
	
	
	public void discard() {
		super.discard();

	}

	@Override
	protected HyCubeNodeProxyService initializeNodeProxyService(NodeId nodeId,
			String nodeIdString, String networkAddress,
			Environment environment,
			Map<EventType, LinkedBlockingQueue<Event>> eventQueues,
			EventScheduler eventScheduler)
			throws InitializationException {
		
		if (nodeId != null) return HyCubeNodeProxyService.initialize(nodeId, networkAddress, environment, eventQueues, eventScheduler);
		else if (nodeIdString != null && (!nodeIdString.isEmpty())) return HyCubeNodeProxyService.initialize(nodeIdString, networkAddress, environment, eventQueues, eventScheduler);
		else return HyCubeNodeProxyService.initialize(networkAddress, environment, eventQueues, eventScheduler);
		
	}



	
	
	
}
