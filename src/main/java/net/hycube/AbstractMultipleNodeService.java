package net.hycube;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeId;
import net.hycube.environment.Environment;
import net.hycube.environment.NodeProperties;
import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventCategory;
import net.hycube.eventprocessing.EventQueueProcessor;
import net.hycube.eventprocessing.EventScheduler;
import net.hycube.eventprocessing.EventType;
import net.hycube.eventprocessing.NotifyingBlockingQueue;
import net.hycube.eventprocessing.WakeableManager;
import net.hycube.join.JoinCallback;
import net.hycube.logging.LogHelper;
import net.hycube.transport.MessageReceiver;
import net.hycube.transport.MessageReceiverException;
import net.hycube.transport.WakeableMessageReceiver;
import net.hycube.utils.ClassInstanceLoadException;
import net.hycube.utils.ClassInstanceLoader;

public abstract class AbstractMultipleNodeService implements MultipleNodeService {

	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(AbstractMultipleNodeService.class);
	
	
	protected static final int DEFAULT_INIIAL_NODE_PROXY_SERVICES_COLLECTIONS_SIZE = 10;
	
	
	public static final String PROP_KEY_NODE_ID = "NodeId";
	public static final String PROP_KEY_NODE_NETWORK_ADDRESS = "NodeNetworkAddress";
	public static final String PROP_KEY_BOOTSTRAP_NODE_NETWORK_ADDRESS = "BootstrapNodeNetworkAddress";
	
	public static final String PROP_KEY_QUEUES = "Queues";
	public static final String PROP_KEY_THREAD_POOL = "ThreadPool";
	public static final String PROP_KEY_CORE_POOL_SIZE = "PoolSize";
	public static final String PROP_KEY_KEEP_ALIVE_TIME_SEC = "KeepAliveTimeSec";
	public static final String PROP_KEY_EVENT_TYPES = "EventTypes";
	public static final String PROP_KEY_EVENT_CATEGORY = "EventCategory";
	public static final String PROP_KEY_EVENT_TYPE_KEY = "EventTypeKey";
	public static final String PROP_KEY_WAKEABLE = "Wakeable";
	
	

	protected EventQueueProcessor eventProcessor;
	
	protected Map<EventType, LinkedBlockingQueue<Event>> eventQueues;
	protected Map<EventType, WakeableManager> wakeableManagers;
	
	protected boolean initialized = false;
	protected boolean discarded = false;
	
	protected Environment environment;
	protected NodeProperties properties;
	
	protected HashSet<MessageReceiver> messageReceivers;
	protected HashSet<NodeProxyService> nodeProxyServices;
	protected HashMap<NodeProxyService, MessageReceiver> nodeServiceMessageReceiverMap;
	protected HashMap<MessageReceiver, HashSet<NodeProxyService>> messageReceiverNodeServiceMap;
	
	protected EventScheduler eventScheduler;


	
	/* (non-Javadoc)
	 * @see net.hycube.MultipleNodeService#getEventQueues()
	 */
	@Override
	public Map<EventType, LinkedBlockingQueue<Event>> getEventQueues() {
		return eventQueues;
	}
	
	

	
	/* (non-Javadoc)
	 * @see net.hycube.MultipleNodeService#initializeMessageReceiver()
	 */
	@Override
	@SuppressWarnings("unchecked")
	public MessageReceiver initializeMessageReceiver() throws InitializationException {
		
		devLog.info("Initializing message receiver.");
		userLog.info("Initializing message receiver.");
		
		WakeableMessageReceiver messageReceiver;
		
		
		EventType receiveMessageEventType = new EventType(EventCategory.receiveMessageEvent);
		
		String messageReceiverKey = properties.getProperty(NodeService.PROP_KEY_MESSAGE_RECEIVER);
		if (messageReceiverKey == null || messageReceiverKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeService.PROP_KEY_MESSAGE_RECEIVER), "Invalid parameter value: " + properties.getAbsoluteKey(NodeService.PROP_KEY_MESSAGE_RECEIVER));
		NodeProperties messageReceiverProperties = properties.getNestedProperty(NodeService.PROP_KEY_MESSAGE_RECEIVER, messageReceiverKey);
		String messageReceiverClass = messageReceiverProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
		
		
		try {
			messageReceiver = (WakeableMessageReceiver) ClassInstanceLoader.newInstance(messageReceiverClass, WakeableMessageReceiver.class);
		} catch (ClassInstanceLoadException e) {
			throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, messageReceiverClass, "An error occured while creating the wakeable message receiver instance.", e);
		}
		
		try {
			messageReceiver.initialize(environment, (NotifyingBlockingQueue<Event>) eventQueues.get(receiveMessageEventType), wakeableManagers.get(receiveMessageEventType), messageReceiverProperties);
		} catch (MessageReceiverException e) {
			throw new InitializationException(InitializationException.Error.MESSAGE_RECEIVER_INITIALIZATION_ERROR, null, "An exception thrown while initializing the message receiver.", e);
		}
		
		
		messageReceiver.startMessageReceiver();

		//add to receivers
		messageReceivers.add(messageReceiver);
		messageReceiverNodeServiceMap.put(messageReceiver, new HashSet<NodeProxyService>());
		
		devLog.info("Initialized message receiver.");
		userLog.info("Initialized message receiver.");
		
		return messageReceiver;
		
	}
	


	/* (non-Javadoc)
	 * @see net.hycube.MultipleNodeService#initializeNode(java.lang.String, java.lang.String, net.hycube.join.JoinCallback, java.lang.Object, net.hycube.transport.MessageReceiver)
	 */
	@Override
	public NodeService initializeNode(String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, MessageReceiver messageReceiver) throws InitializationException {
		return initializeNode(null, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, messageReceiver);
	}
	


	/* (non-Javadoc)
	 * @see net.hycube.MultipleNodeService#initializeNode(net.hycube.core.NodeId, java.lang.String, java.lang.String, net.hycube.join.JoinCallback, java.lang.Object, net.hycube.transport.MessageReceiver)
	 */
	@Override
	public NodeService initializeNode(NodeId nodeId, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, MessageReceiver messageReceiver) throws InitializationException {
		return initializeNode(nodeId, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, messageReceiver);
	}
	


	/* (non-Javadoc)
	 * @see net.hycube.MultipleNodeService#initializeNode(java.lang.String, java.lang.String, java.lang.String, net.hycube.join.JoinCallback, java.lang.Object, net.hycube.transport.MessageReceiver)
	 */
	@Override
	public NodeService initializeNode(String nodeIdString, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, MessageReceiver messageReceiver) throws InitializationException {
		return initializeNode(null, nodeIdString, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, messageReceiver);
	}
	
	

	/* (non-Javadoc)
	 * @see net.hycube.MultipleNodeService#initializeNode(net.hycube.core.NodeId, java.lang.String, java.lang.String, java.lang.String, net.hycube.join.JoinCallback, java.lang.Object, net.hycube.transport.MessageReceiver)
	 */
	@Override
	public NodeService initializeNode(NodeId nodeId, String nodeIdString, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, MessageReceiver messageReceiver) throws InitializationException {

		devLog.info("Initializing node.");
		userLog.info("Initializing node.");
		
		if (! messageReceivers.contains(messageReceiver)) {
			throw new IllegalArgumentException("Invalid message receiver specified - not managed by this service instance.");
		}

		NodeProxyService nodeService = null;
		nodeService = initializeNodeProxyService(nodeId, nodeIdString, networkAddress, environment, eventQueues, eventScheduler);
				
		try {
			messageReceiver.registerNetworkAdapter(nodeService.getNode().getNetworkAdapter());
		} catch (MessageReceiverException e) {
			throw new InitializationException(InitializationException.Error.MESSAGE_RECEIVER_INITIALIZATION_ERROR, null, "An exception thrown while registering the network adapter for the message receiver.", e);
		}
				
//		if (bootstrapNodeAddress != null) {
			nodeService.join(bootstrapNodeAddress, joinCallback, callbackArg);
//		}
	

		//add to nodeServices
		nodeProxyServices.add(nodeService);
		nodeServiceMessageReceiverMap.put(nodeService, messageReceiver);
		messageReceiverNodeServiceMap.get(messageReceiver).add(nodeService);
		
		devLog.info("Node initialized.");
		userLog.info("Node initialized.");
		
		return nodeService;

	}

	


	/* (non-Javadoc)
	 * @see net.hycube.MultipleNodeService#discardMessageReceiver(net.hycube.transport.MessageReceiver)
	 */
	@Override
	public void discardMessageReceiver(MessageReceiver messageReceiver) throws MessageReceiverException {
		
		devLog.info("Discarding message receiver.");
		userLog.info("Discarding message receiver.");
		
		if (! messageReceivers.contains(messageReceiver)) {
			throw new IllegalArgumentException("Invalid message receiver specified - not managed by this service instance.");
		}
		
		messageReceiver.discard();
		
		messageReceivers.remove(messageReceiver);
		messageReceiverNodeServiceMap.remove(messageReceiver);

		devLog.info("Discarded message receiver.");
		userLog.info("Discarded message receiver.");
		
	}




	/* (non-Javadoc)
	 * @see net.hycube.MultipleNodeService#discardNode(net.hycube.NodeService)
	 */
	@Override
	public void discardNode(NodeService nodeService) {
		
		devLog.info("Discarding node.");
		userLog.info("Discarding node.");
		
		if (! nodeProxyServices.contains(nodeService)) {
			throw new IllegalArgumentException("Invalid node service specified - not managed by this service instance.");
		}
		
		MessageReceiver messageReceiver = nodeServiceMessageReceiverMap.get(nodeService);
		
		messageReceiver.unregisterNetworkAdapter(nodeService.getNode().getNetworkAdapter());
		
		
		nodeService.discard();
		
		nodeProxyServices.remove(nodeService);

		
		nodeServiceMessageReceiverMap.remove(nodeService);
		messageReceiverNodeServiceMap.get(messageReceiver).remove(nodeService);
		
		devLog.info("Node discarded.");
		userLog.info("Node discarded.");
	}


	
	
	
	/* (non-Javadoc)
	 * @see net.hycube.MultipleNodeService#discard()
	 */
	@Override
	public abstract void discard();

	
	
	
	
	protected abstract NodeProxyService initializeNodeProxyService(NodeId nodeId,
			String nodeIdString, String networkAddress,
			Environment environment,
			Map<EventType, LinkedBlockingQueue<Event>> eventQueues,
			EventScheduler eventScheduler)
			throws InitializationException;
	
	
	
	
	
}