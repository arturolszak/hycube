/**
 * 
 */
package net.hycube.core;

import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.hycube.backgroundprocessing.BackgroundProcess;
import net.hycube.backgroundprocessing.BackgroundProcessEntryPoint;
import net.hycube.backgroundprocessing.BackgroundProcessException;
import net.hycube.common.EntryPoint;
import net.hycube.configuration.GlobalConstants;
import net.hycube.dht.DHTManager;
import net.hycube.dht.DeleteCallback;
import net.hycube.dht.GetCallback;
import net.hycube.dht.PutCallback;
import net.hycube.dht.RefreshPutCallback;
import net.hycube.environment.Environment;
import net.hycube.environment.NodeProperties;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.environment.TimeProvider;
import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventCategory;
import net.hycube.eventprocessing.EventProcessException;
import net.hycube.eventprocessing.EventQueuesInitializationException;
import net.hycube.eventprocessing.EventScheduler;
import net.hycube.eventprocessing.EventType;
import net.hycube.eventprocessing.ProcessEventProxy;
import net.hycube.extensions.Extension;
import net.hycube.join.JoinCallback;
import net.hycube.join.JoinManager;
import net.hycube.leave.LeaveManager;
import net.hycube.logging.LogHelper;
import net.hycube.lookup.LookupCallback;
import net.hycube.lookup.LookupManager;
import net.hycube.maintenance.NotifyProcessor;
import net.hycube.messaging.ack.AckProcessInfo;
import net.hycube.messaging.ack.MessageAckCallback;
import net.hycube.messaging.ack.MessageAckCallbackEvent;
import net.hycube.messaging.ack.MessageAckCallbackType;
import net.hycube.messaging.callback.MessageReceivedCallback;
import net.hycube.messaging.data.DataMessageSendProcessInfo;
import net.hycube.messaging.data.MessageReceivedCallbackEvent;
import net.hycube.messaging.data.ReceivedDataMessage;
import net.hycube.messaging.messages.Message;
import net.hycube.messaging.messages.MessageErrorException;
import net.hycube.messaging.messages.MessageFactory;
import net.hycube.messaging.processing.MessageSendInfo;
import net.hycube.messaging.processing.MessageSendProcessInfo;
import net.hycube.messaging.processing.MessageSendProcessor;
import net.hycube.messaging.processing.ProcessMessageException;
import net.hycube.messaging.processing.ProcessReceivedMessageEvent;
import net.hycube.messaging.processing.PushMessageEvent;
import net.hycube.messaging.processing.ReceivedMessageProcessor;
import net.hycube.nexthopselection.NextHopSelector;
import net.hycube.routing.RoutingManager;
import net.hycube.search.SearchCallback;
import net.hycube.search.SearchManager;
import net.hycube.transport.NetworkAdapter;
import net.hycube.transport.NetworkAdapterException;
import net.hycube.transport.NetworkNodePointer;
import net.hycube.transport.ReceivedMessageProcessProxy;
import net.hycube.utils.ClassInstanceLoadException;
import net.hycube.utils.ClassInstanceLoader;
import net.hycube.utils.HashMapUtils;
import net.hycube.utils.ObjectToStringConverter.MappedType;

/**
 * @author Artur Olszak
 *
 */
public class Node {


	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(Node.class); 
	
	
	
	
	//protected members:
	
	protected boolean discarded;
	
	
	/**
	 * Node ID factory
	 */
	protected NodeIdFactory nodeIdFactory;
	
	
	/**
	 * Node ID
	 */
	protected NodeId nodeId;

	
	/**
	 * Message factory
	 */
	protected MessageFactory messageFactory;
	
	
	
	protected NodeProperties properties;
	protected NodeParameterSet parameters;
	
	protected Environment environment;
	
	
	/**
	 * Routing table - all operations should be synchronized on this object - reading data, modifying the routing table (adding/removing nodes)
	 */
	protected RoutingTable routingTable;    
    
	
	
    protected LinkedBlockingQueue<Event>[] eventQueues;
    protected HashMap<EventCategory, LinkedBlockingQueue<Event>> eventCategoryQueueMap;
    protected HashMap<String, LinkedBlockingQueue<Event>> backgroundEventQueueMap;
	protected HashMap<String, LinkedBlockingQueue<Event>> extEventQueueMap;
    
    protected LinkedBlockingQueue<Event> retrieveMessageEventQueue;
    protected LinkedBlockingQueue<Event> processReceivedMessageEventQueue;
    protected LinkedBlockingQueue<Event> pushMessageEventQueue;
    protected LinkedBlockingQueue<Event> pushSystemMessageEventQueue;
    protected LinkedBlockingQueue<Event> processAckCallbackEventQueue;
    protected LinkedBlockingQueue<Event> processMsgReceivedCallbackEventQueue;

    protected EventScheduler eventScheduler;
    
    protected LinkedBlockingQueue<ReceivedDataMessage> appMessageInQueue;
    protected Map<Short, LinkedBlockingQueue<ReceivedDataMessage>> appPortMessageInQueues;
    
    protected MessageReceivedCallback appMessageReceivedCallback;
    protected Map<Short, MessageReceivedCallback> appPortMessageReceivedCallbacks;

    
    protected Map<String, ReceivedMessageProcessor> receivedMessageProcessors;
    protected Map<String, MessageSendProcessor> messageSendProcessors;
    
    protected Map<String, NextHopSelector> nextHopSelectors;
    
    protected RoutingManager routingManager;
    protected LookupManager lookupManager;
    protected SearchManager searchManager;
    protected JoinManager joinManager;
    protected LeaveManager leaveManager;
    protected DHTManager dhtManager;
    
    protected NotifyProcessor notifyProcessor;
    
    protected NetworkAdapter networkAdapter;
    
    protected NodePointer nodePointer;
		
    protected TimeProvider timeProvider;
	
    protected int msgSerialNoCounter;
        

	
    //lock objects for synchronization of threads
    protected final Object nodeLock = new Object();
    protected final Object msgSerialNoCounterLock = new Object();			//message counters reads/updates should be synchronized with this lock 
    protected final Object appMessageInLock = new Object();			//this lock should be used for registering, unregistering and operations on queues/callbacks for ports
    
    protected final ReentrantReadWriteLock discardLock = new ReentrantReadWriteLock(true);
    
    
    
    //accessors (inner classes instances)
    protected NodeAccessor nodeAccessor;
	protected NodeProcessEventProxy nodeProcessEventProxy;
	protected ReceivedMessageProcessProxy nodeProcessReceivedMessageProxy;
    
    
	
	//extensions
	protected Map<String, Extension> extensions;
	
	
	
	//background processes
	protected Map<String, BackgroundProcess> backgroundProcesses;
	
	
    
    //algorithm specific data:
    protected HashMap<String, Object> dataMap;
    
    
  
    
    
    
    //other fields:
    protected int maxMessageLength;
    protected int maxMessageDataLength;
    
    
    
    
    
    
    
    protected Node() {
		
	}
	
	

    
    //getters, setters:
    
    
    public NodeId getNodeId() {
    	return nodeId;
    }
    
    public NodePointer getNodePointer() {
    	return nodePointer;
    }
    
	public Environment getEnvironment() {
		return environment;
	}
    
	public NetworkAdapter getNetworkAdapter() {
		return Node.this.networkAdapter;
	}
	
    
	protected LinkedBlockingQueue<Event> getEventQueue(EventType eventType) {
		switch (eventType.getEventCategory()) {
			case executeBackgroundProcessEvent:
				if (backgroundEventQueueMap.containsKey(eventType.getEventTypeKey())) return backgroundEventQueueMap.get(eventType.getEventTypeKey());
				else return backgroundEventQueueMap.get("");
			case extEvent:
				if (extEventQueueMap.containsKey(eventType.getEventTypeKey())) return extEventQueueMap.get(eventType.getEventTypeKey());
				else return extEventQueueMap.get("");
			default:
				return eventCategoryQueueMap.get(eventType.getEventCategory());
		}
	}
	
	protected LinkedBlockingQueue<Event> getEventQueue(EventCategory eventCategory) {
		return eventCategoryQueueMap.get(eventCategory);
	}
	
	
	
	//algorithm specific data:
	
	public Object getData(String key, Object defaultValue) {
		if (dataMap.containsKey(key)) return dataMap.get(key);
		else return defaultValue;
	}
	
	public Object getData(String key) {
		return dataMap.get(key);
	}
	
	public void setData(String key, Object data) {
		this.dataMap.put(key, data);
	}
	
	public void removeData(String key) {
		this.dataMap.remove(key);
	}
	
	
	
    
	

	//initialization:
	
	public static Node initializeNode(Environment environment, String networkAddress, Map<EventType, LinkedBlockingQueue<Event>> eventQueues, EventScheduler eventScheduler) throws InitializationException {
		return initializeNode(environment, null, null, networkAddress, eventQueues, eventScheduler);
	}
	
	public static Node initializeNode(Environment environment, NodeId id, String networkAddress, Map<EventType, LinkedBlockingQueue<Event>> eventQueues, EventScheduler eventScheduler) throws InitializationException {
		return initializeNode(environment, id, null, networkAddress, eventQueues, eventScheduler);
	}
	
	public static Node initializeNode(Environment environment, String idString, String networkAddress, Map<EventType, LinkedBlockingQueue<Event>> eventQueues, EventScheduler eventScheduler) throws InitializationException {
		return initializeNode(environment, null, idString, networkAddress, eventQueues, eventScheduler);
	}
	
	
	protected static Node initializeNode(Environment environment, NodeId id, String idString, String networkAddress, Map<EventType, LinkedBlockingQueue<Event>> eventQueues, EventScheduler eventScheduler) throws InitializationException {

		if (userLog.isInfoEnabled()) {
			userLog.info("Initializing node. Node id: " + id.toHexString() + ", Network address: " + networkAddress + ".");
		}
		if (devLog.isInfoEnabled()) {
			devLog.info("Initializing node. Node id: " + id.toHexString() + ", Network address: " + networkAddress + ".");
		}
		
		
		
		//*** INITIALIZE ***
		
		
		Node node = new Node();
		NodeProperties properties = environment.getNodeProperties();
		
		
		node.discarded = false;
		
		
		//environment
		node.environment = environment;
		
		
		//initialize accessor 
		node.nodeAccessor = node.new NodeAccessorImpl();
		
		//parameters:
		node.properties = properties;
		node.parameters = new NodeParameterSet();
		node.parameters.readParameters(properties);
		
		
		
		
		//initialize extensions so the references are accessible for all other classes
		//Extension.initialize() method expects only the node accessor instance to be a valid reference to the node accessor, but should consider the node object as not yet initialized
		//all subsequent customizations may assume all the extensions are already initialized
		//Extension.postInitialize() as a last step of the node initialization
		List<String> extensionKeys;
		try {
			extensionKeys = properties.getStringListProperty(NodeParameterSet.PROP_KEY_EXTENSIONS);
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_EXTENSIONS), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_EXTENSIONS) + ". Unable to create Extension objects.", e);
		}
		if (extensionKeys == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_EXTENSIONS), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_EXTENSIONS) + ". Unable to create Extension objects.");
		
		node.extensions = new HashMap<String, Extension>(HashMapUtils.getHashMapCapacityForElementsNum(extensionKeys.size(), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		
		for (String extensionKey : extensionKeys) {
			try {
				if (extensionKey == null || extensionKey.trim().isEmpty()) {
					//throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_EXTENSIONS), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_EXTENSIONS));
					continue;
				}
				NodeProperties extensionProperties = properties.getNestedProperty(NodeParameterSet.PROP_KEY_EXTENSIONS, extensionKey);
				String extensionClass = extensionProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
				
				Extension extension = (Extension) ClassInstanceLoader.newInstance(extensionClass, Extension.class);
				extension.initialize(node.nodeAccessor, extensionProperties);
				
				node.extensions.put(extensionKey, extension);
				
			} catch (ClassInstanceLoadException e) {
				throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create Extension instance.", e);
			}
			
		}
			
		
		
		
		//nodeId factory:
		try {
			String nodeIdFactoryKey = properties.getProperty(NodeParameterSet.PROP_KEY_NODE_ID_FACTORY);
			if (nodeIdFactoryKey == null || nodeIdFactoryKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_NODE_ID_FACTORY), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_NODE_ID_FACTORY));
			NodeProperties nodeIdFactoryProperties = properties.getNestedProperty(NodeParameterSet.PROP_KEY_NODE_ID_FACTORY, nodeIdFactoryKey);
			String nodeIdFactoryClass = nodeIdFactoryProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
			
			node.nodeIdFactory = (NodeIdFactory) ClassInstanceLoader.newInstance(nodeIdFactoryClass, NodeIdFactory.class);
			node.nodeIdFactory.initialize(nodeIdFactoryProperties);
		} catch (ClassInstanceLoadException e) {
			throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create node id factory instance.", e);
		}
		
		
		//nodeId
		if (id != null) {
			//check the node id type
			if (! node.nodeIdFactory.getNodeIdType().isInstance(id)) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, id, "The type of node id object is not supported by the configured node id factory.");
			}
		}
		else if (idString != null && (!idString.isEmpty())) {
			try {
				id = node.nodeIdFactory.parseNodeId(idString);
			} catch (Exception e) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, id, "The node id string could not be converted to a NodeId instance.", e);
			}
		}
		else {	//id and idString are null
			id = node.nodeIdFactory.generateRandomNodeId();
			
			if (userLog.isInfoEnabled()) {
				userLog.info("Generated node id: " + id.toHexString() + ".");
			}
			if (devLog.isInfoEnabled()) {
				devLog.info("Generated node id: " + id.toHexString() + ".");
			}
		}
		node.nodeId = id;
		
		
		
		//Message factory:
		try {
			String messageFactoryKey = properties.getProperty(NodeParameterSet.PROP_KEY_MESSAGE_FACTORY);
			if (messageFactoryKey == null || messageFactoryKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_MESSAGE_FACTORY), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_MESSAGE_FACTORY));
			NodeProperties messageFactoryProperties = properties.getNestedProperty(NodeParameterSet.PROP_KEY_MESSAGE_FACTORY, messageFactoryKey);
			String messageFactoryClass = messageFactoryProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);

			node.messageFactory = (MessageFactory) ClassInstanceLoader.newInstance(messageFactoryClass, MessageFactory.class);
			node.messageFactory.initialize(messageFactoryProperties);
		} catch (ClassInstanceLoadException e) {
			throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create message factory instance.", e);
		}
		
		
		//initialize the message serial number counter;
		node.msgSerialNoCounter = Integer.MIN_VALUE;
		
		
		      
		//initialize message queues:
		
		if (node.parameters.usePorts) {
			//messages send to particular ports will be stored in corresponding queues
			node.appPortMessageInQueues = new HashMap<Short, LinkedBlockingQueue<ReceivedDataMessage>>();
			node.appPortMessageReceivedCallbacks = new HashMap<Short, MessageReceivedCallback>();
		}
		else {
			//one queue for all messages
			node.appMessageInQueue = new LinkedBlockingQueue<ReceivedDataMessage>();
		}
		
		
		
		
		//initialize the event processor proxy:
		node.nodeProcessEventProxy = node.new NodeProcessEventProxy();
		
		
		
		//initialize event queues:
		try {
			node.initializeEventQueues(eventQueues);
		} catch (Exception e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Event queues initialization failed.", e);
		}
		
		
		
		//initialize event scheduler:
		node.eventScheduler = eventScheduler;
		if (node.eventScheduler == null) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "The event scheduler specified is null.");
		}
		
		
		
		//initialize the routing tables structure:
		try {
			String routingTableKey = properties.getProperty(NodeParameterSet.PROP_KEY_ROUTING_TABLE);
			if (routingTableKey == null || routingTableKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_ROUTING_TABLE), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_ROUTING_TABLE));
			NodeProperties routingTableProperties = properties.getNestedProperty(NodeParameterSet.PROP_KEY_ROUTING_TABLE, routingTableKey);
			String routingTableClass = routingTableProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
			
			node.routingTable = (RoutingTable) ClassInstanceLoader.newInstance(routingTableClass, RoutingTable.class);
			node.routingTable.initialize(routingTableProperties);
		} catch (ClassInstanceLoadException e) {
			throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create routing table class instance.", e);
		}
		        
		
		
		//initialize algorithms classes instances:
		
		
		//initialize the network adapter:
		try {
			String networkAdapterKey = properties.getProperty(NodeParameterSet.PROP_KEY_NETWORK_ADAPTER);
			if (networkAdapterKey == null || networkAdapterKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_NETWORK_ADAPTER), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_NETWORK_ADAPTER));
			NodeProperties networkAdapterProperties = properties.getNestedProperty(NodeParameterSet.PROP_KEY_NETWORK_ADAPTER, networkAdapterKey);
			String networkAdapterClass = networkAdapterProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
			
			node.networkAdapter = (NetworkAdapter) ClassInstanceLoader.newInstance(networkAdapterClass, NetworkAdapter.class);
			
			node.nodeProcessReceivedMessageProxy = node.new NodeReceivedMessageProcessProxy();
			node.networkAdapter.initialize(networkAddress, node.nodeProcessReceivedMessageProxy, node.nodeAccessor, networkAdapterProperties);
			
		} 
		catch (ClassInstanceLoadException e) {
			throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create network adapter class instance.", e);
		}
		
		node.nodePointer = new NodePointer(node.networkAdapter, node.networkAdapter.getPublicAddressString(), node.nodeId);
		
		
		
		
		//next hop selectors:
		List<String> nextHopSelectorKeys = null;
		try {
			nextHopSelectorKeys = properties.getStringListProperty(NodeParameterSet.PROP_KEY_NEXT_HOP_SELECTORS);
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_NEXT_HOP_SELECTORS), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_NEXT_HOP_SELECTORS) + ". Unable to create NextHopSelector objects.", e);
		}
		if (nextHopSelectorKeys == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_NEXT_HOP_SELECTORS), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_NEXT_HOP_SELECTORS) + ". Unable to create NextHopSelector objects.");
		node.nextHopSelectors = new HashMap<String, NextHopSelector>(HashMapUtils.getHashMapCapacityForElementsNum(nextHopSelectorKeys.size(), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		for (String nextHopSelectorKey : nextHopSelectorKeys) {
			if (nextHopSelectorKey == null || nextHopSelectorKey.trim().isEmpty()) {
				//throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_NEXT_HOP_SELECTORS), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_NEXT_HOP_SELECTORS));
				continue;
			}
			try {
				NodeProperties nextHopSelectorProperties = properties.getNestedProperty(NodeParameterSet.PROP_KEY_NEXT_HOP_SELECTORS, nextHopSelectorKey);
				String nextHopSelectorClass = nextHopSelectorProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
				
				NextHopSelector nextHopSelector = (NextHopSelector) ClassInstanceLoader.newInstance(nextHopSelectorClass, NextHopSelector.class);
				nextHopSelector.initialize(node.nodeAccessor, nextHopSelectorProperties);
				
				node.nextHopSelectors.put(nextHopSelectorKey, nextHopSelector);
			} catch (ClassInstanceLoadException e) {
				throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create next hop selector class instance.", e);
			}
		}
		if (node.nextHopSelectors.isEmpty()) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "No next hop selectors defined.");
		}
		
		
		
		//notify processor:
		try {
			String notifyProcessorKey = properties.getProperty(NodeParameterSet.PROP_KEY_NOTIFY_PROCESSOR);
			if (notifyProcessorKey == null || notifyProcessorKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_NOTIFY_PROCESSOR), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_NOTIFY_PROCESSOR));
			NodeProperties notifyProcessorProperties = properties.getNestedProperty(NodeParameterSet.PROP_KEY_NOTIFY_PROCESSOR, notifyProcessorKey);
			String notifyProcessorClass = notifyProcessorProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
			
			node.notifyProcessor = (NotifyProcessor) ClassInstanceLoader.newInstance(notifyProcessorClass, NotifyProcessor.class);
			node.notifyProcessor.initialize(node.nodeAccessor, notifyProcessorProperties);
		} catch (ClassInstanceLoadException e) {
			throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create notify processor class instance.", e);
		}
		
		
		
		//routing manager:
		try {
			String routingManagerKey = properties.getProperty(NodeParameterSet.PROP_KEY_ROUTING_MANAGER);
			if (routingManagerKey == null || routingManagerKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_ROUTING_MANAGER), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_ROUTING_MANAGER));
			NodeProperties routingManagerProperties = properties.getNestedProperty(NodeParameterSet.PROP_KEY_ROUTING_MANAGER, routingManagerKey);
			String routingManagerClass = routingManagerProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
			
			node.routingManager = (RoutingManager) ClassInstanceLoader.newInstance(routingManagerClass, RoutingManager.class);
			node.routingManager.initialize(node.nodeAccessor, routingManagerProperties);
		} catch (ClassInstanceLoadException e) {
			throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create routing manager class instance.", e);
		}
		
		
		//lookup manager:
		try {
			String lookupManagerKey = properties.getProperty(NodeParameterSet.PROP_KEY_LOOKUP_MANAGER);
			if (lookupManagerKey == null || lookupManagerKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_LOOKUP_MANAGER), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_LOOKUP_MANAGER));
			NodeProperties lookupManagerProperties = properties.getNestedProperty(NodeParameterSet.PROP_KEY_LOOKUP_MANAGER, lookupManagerKey);
			String lookupManagerClass = lookupManagerProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
			
			node.lookupManager = (LookupManager) ClassInstanceLoader.newInstance(lookupManagerClass, LookupManager.class);
			node.lookupManager.initialize(node.nodeAccessor, lookupManagerProperties);
		} catch (ClassInstanceLoadException e) {
			throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create lookup manager class instance.", e);
		}

		
		//search manager:
		try {
			String searchManagerKey = properties.getProperty(NodeParameterSet.PROP_KEY_SEARCH_MANAGER);
			if (searchManagerKey == null || searchManagerKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_SEARCH_MANAGER), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_SEARCH_MANAGER));
			NodeProperties searchManagerProperties = properties.getNestedProperty(NodeParameterSet.PROP_KEY_SEARCH_MANAGER, searchManagerKey);
			String searchManagerClass = searchManagerProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
			
			node.searchManager = (SearchManager) ClassInstanceLoader.newInstance(searchManagerClass, SearchManager.class);
			node.searchManager.initialize(node.nodeAccessor, searchManagerProperties);
		} catch (ClassInstanceLoadException e) {
			throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create search manager class instance.", e);
		}
		
		
		
		//join manager:
		try {
			String joinManagerKey = properties.getProperty(NodeParameterSet.PROP_KEY_JOIN_MANAGER);
			if (joinManagerKey == null || joinManagerKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_JOIN_MANAGER), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_JOIN_MANAGER));
			NodeProperties joinManagerProperties = properties.getNestedProperty(NodeParameterSet.PROP_KEY_JOIN_MANAGER, joinManagerKey);
			String joinManagerClass = joinManagerProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
			
			node.joinManager = (JoinManager) ClassInstanceLoader.newInstance(joinManagerClass, JoinManager.class);
			node.joinManager.initialize(node.nodeAccessor, joinManagerProperties);
		} catch (ClassInstanceLoadException e) {
			throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create join manager class instance.", e);
		}


		//leave manager:
		try {
			String leaveManagerKey = properties.getProperty(NodeParameterSet.PROP_KEY_LEAVE_MANAGER);
			if (leaveManagerKey == null || leaveManagerKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_LEAVE_MANAGER), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_LEAVE_MANAGER));
			NodeProperties leaveManagerProperties = properties.getNestedProperty(NodeParameterSet.PROP_KEY_LEAVE_MANAGER, leaveManagerKey);
			String leaveManagerClass = leaveManagerProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
			
			node.leaveManager = (LeaveManager) ClassInstanceLoader.newInstance(leaveManagerClass, LeaveManager.class);
			node.leaveManager.initialize(node.nodeAccessor, leaveManagerProperties);
		} catch (ClassInstanceLoadException e) {
			throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create leave manager class instance.", e);
		}

		
		
		
		//dht manager:
		try {
			String dhtManagerKey = properties.getProperty(NodeParameterSet.PROP_KEY_DHT_MANAGER);
			//if (dhtManagerKey == null || dhtManagerKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_DHT_MANAGER), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_DHT_MANAGER));
			if (dhtManagerKey == null || dhtManagerKey.trim().isEmpty()) {
				//do nothing
			}
			NodeProperties dhtManagerProperties = properties.getNestedProperty(NodeParameterSet.PROP_KEY_DHT_MANAGER, dhtManagerKey);
			String dhtManagerClass = dhtManagerProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
			
			node.dhtManager = (DHTManager) ClassInstanceLoader.newInstance(dhtManagerClass, DHTManager.class);
			node.dhtManager.initialize(node.nodeAccessor, dhtManagerProperties);
		} catch (ClassInstanceLoadException e) {
			throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create DHT manager class instance.", e);
		}
		
		
		

		
		
		
		
		//received message processors
		List<String> receivedMessageProcessorKeys = null;
		try {
			receivedMessageProcessorKeys = properties.getStringListProperty(NodeParameterSet.PROP_KEY_RECEIVED_MESSAGE_PROCESSORS);
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_RECEIVED_MESSAGE_PROCESSORS), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_RECEIVED_MESSAGE_PROCESSORS) + ". Unable to create ReceivedMessageProcessor objects.", e);
		}
		if (receivedMessageProcessorKeys == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_RECEIVED_MESSAGE_PROCESSORS), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_RECEIVED_MESSAGE_PROCESSORS) + ". Unable to create ReceivedMessageProcessor objects.");
		node.receivedMessageProcessors = new HashMap<String, ReceivedMessageProcessor>(HashMapUtils.getHashMapCapacityForElementsNum(receivedMessageProcessorKeys.size(), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		for (String messageProcessorKey : receivedMessageProcessorKeys) {
			if (messageProcessorKey == null || messageProcessorKey.trim().isEmpty()) {
				//throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_RECEIVED_MESSAGE_PROCESSORS), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_RECEIVED_MESSAGE_PROCESSORS));
				continue;
			}
			try {
				NodeProperties messageProcessorProperties = properties.getNestedProperty(NodeParameterSet.PROP_KEY_RECEIVED_MESSAGE_PROCESSORS, messageProcessorKey);
				String messageProcessorClass = messageProcessorProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
				
				ReceivedMessageProcessor messageProcessor = (ReceivedMessageProcessor)ClassInstanceLoader.newInstance(messageProcessorClass, ReceivedMessageProcessor.class);
				messageProcessor.initialize(node.nodeAccessor, messageProcessorProperties);
				
				node.receivedMessageProcessors.put(messageProcessorKey, messageProcessor);
			} catch (ClassInstanceLoadException e) {
				throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create received message processor instance.", e);
			}
		}

		
		//message send processors:
		List<String> messageSendProcessorKeys = null;
		try {
			messageSendProcessorKeys = properties.getStringListProperty(NodeParameterSet.PROP_KEY_MESSAGE_SEND_PROCESSORS);
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_MESSAGE_SEND_PROCESSORS), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_MESSAGE_SEND_PROCESSORS) + ". Unable to create MessageSendProcessor objects.", e);
		}
		if (messageSendProcessorKeys == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_MESSAGE_SEND_PROCESSORS), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_MESSAGE_SEND_PROCESSORS) + ". Unable to create MessageSendProcessor objects.");
		node.messageSendProcessors = new HashMap<String, MessageSendProcessor>(HashMapUtils.getHashMapCapacityForElementsNum(messageSendProcessorKeys.size(), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		for (String messageProcessorKey : messageSendProcessorKeys) {
			if (messageProcessorKey == null || messageProcessorKey.trim().isEmpty()) {
				//throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_MESSAGE_SEND_PROCESSORS), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_MESSAGE_SEND_PROCESSORS));
				continue;
			}
			try {
				NodeProperties messageProcessorProperties = properties.getNestedProperty(NodeParameterSet.PROP_KEY_MESSAGE_SEND_PROCESSORS, messageProcessorKey);
				String messageProcessorClass = messageProcessorProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
				
				MessageSendProcessor messageProcessor = (MessageSendProcessor)ClassInstanceLoader.newInstance(messageProcessorClass, MessageSendProcessor.class);
				messageProcessor.initialize(node.nodeAccessor, messageProcessorProperties);
				
				node.messageSendProcessors.put(messageProcessorKey, messageProcessor);
			} catch (ClassInstanceLoadException e) {
				throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create message send processor instance.", e);
			}
		}
		
		
		
		
		
		//initialize the time provider:
		node.timeProvider = environment.getTimeProvider();

		
		
	
		
		
		//initialize background processes:
		
		List<String> backgroundProcessKeys = null;
		try {
			backgroundProcessKeys = properties.getStringListProperty(NodeParameterSet.PROP_KEY_BACKGROUND_PROCESSES);			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_BACKGROUND_PROCESSES), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_BACKGROUND_PROCESSES) + ". Unable to create background process objects.", e);
		}
		if (backgroundProcessKeys == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_BACKGROUND_PROCESSES), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_BACKGROUND_PROCESSES) + ". Unable to create background process objects.");
		
		node.backgroundProcesses = new HashMap<String, BackgroundProcess>(HashMapUtils.getHashMapCapacityForElementsNum(backgroundProcessKeys.size(), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		
		HashSet<String> backgroundProcessesToScheduleImmediately = new HashSet<String>(HashMapUtils.getHashMapCapacityForElementsNum(backgroundProcessKeys.size(), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		for (String backgroundProcessKey : backgroundProcessKeys) {
			if (backgroundProcessKey == null || backgroundProcessKey.trim().isEmpty()) {
				//throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_BACKGROUND_PROCESSES), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_BACKGROUND_PROCESSES));
				continue;
			}
			try {
				NodeProperties backgroundProcessProperties = properties.getNestedProperty(NodeParameterSet.PROP_KEY_BACKGROUND_PROCESSES, backgroundProcessKey);
				String backgroundProcessClass = backgroundProcessProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
				
				BackgroundProcess backgroundProcess = (BackgroundProcess) ClassInstanceLoader.newInstance(backgroundProcessClass, BackgroundProcess.class);
				backgroundProcess.initialize(node.nodeAccessor, backgroundProcessProperties);
				
				node.backgroundProcesses.put(backgroundProcessKey, backgroundProcess);
				
				boolean scheduleImmediately = (Boolean) backgroundProcessProperties.getProperty(BackgroundProcess.PROP_KEY_SCHEDULE_IMMEDIATELY, MappedType.BOOLEAN);
				if (scheduleImmediately) backgroundProcessesToScheduleImmediately.add(backgroundProcessKey);
				
			} catch (ClassInstanceLoadException e) {
				throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create BackgroundProcess instance.", e);
			} catch (NodePropertiesConversionException e) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize node. Invalid parameter value: " + e.getKey() + ".", e);
			}
			
		}
		
		
		
		
		//*** VERIFICATION ***
		
		//verify the event queues (also the background process event queues):
		try {
			node.verifyEventQueues();
		} catch (Exception e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Event queues initialization failed.", e);
		}
		
		
		
		
		
		
		//*** REMAINING TASKS PERFORMED AFTER THE INITIALIZATION AND VERIFICATION ***
		
		//Extension.postInitialize()
		for (Extension extension : node.extensions.values()) {
			extension.postInitialize();
		}
		
		//schedule the backround process executions:
		for (String backgroundProcessKey : node.backgroundProcesses.keySet()) {
			
			BackgroundProcess backgroundProcess = node.backgroundProcesses.get(backgroundProcessKey);
			
			if (backgroundProcessesToScheduleImmediately.contains(backgroundProcessKey)) {
				backgroundProcess.start();
			}
			
		}
		
		
		
		
		
		//calculate max message and max message data lengths:

		node.maxMessageLength = node.calcuateMaxMessageLength(); 
	    node.maxMessageDataLength = node.calcuateMaxMessageDataLength();
		
		
		
		
		
		//node initialized
		if (userLog.isInfoEnabled()) {
			userLog.info("Initialized node. Node id: " + id.toHexString() + ", Network address: " + networkAddress + ".");
		}
		if (devLog.isInfoEnabled()) {
			devLog.info("Initialized node. Node id: " + id.toHexString() + ", Network address: " + networkAddress + ".");
		}
		
		
		return node;
		
	}


	
   
    
	protected void initializeEventQueues(Map<EventType, LinkedBlockingQueue<Event>> eventQueues) {

		if (devLog.isDebugEnabled()) {
			devLog.debug("Initializing event queues.");
		}
		
		
		eventCategoryQueueMap = new HashMap<EventCategory, LinkedBlockingQueue<Event>>();
		backgroundEventQueueMap = new HashMap<String, LinkedBlockingQueue<Event>>();
		extEventQueueMap = new HashMap<String, LinkedBlockingQueue<Event>>();
		
		for (EventType et : eventQueues.keySet()) {
			LinkedBlockingQueue<Event> queue = eventQueues.get(et);
			EventCategory ec = et.getEventCategory();

			if (eventCategoryQueueMap.containsKey(ec)) {
				throw new EventQueuesInitializationException("Every event type should be assigned to just one queue.");
			}
			
			switch (ec) {
			case processReceivedMessageEvent:
				eventCategoryQueueMap.put(ec, queue);
				processReceivedMessageEventQueue = queue;
				break;
			case receiveMessageEvent:
				eventCategoryQueueMap.put(ec, queue);
				retrieveMessageEventQueue = queue;
				break;
			case pushMessageEvent:
				eventCategoryQueueMap.put(ec, queue);
				pushMessageEventQueue = queue;
				break;
			case pushSystemMessageEvent:
				eventCategoryQueueMap.put(ec, queue);
				pushSystemMessageEventQueue = queue;
				break;
			case processAckCallbackEvent:
				eventCategoryQueueMap.put(ec, queue);
				processAckCallbackEventQueue = queue;
				break;
			case processMsgReceivedCallbackEvent:
				eventCategoryQueueMap.put(ec, queue);
				processMsgReceivedCallbackEventQueue = queue;
				break;
			
			case executeBackgroundProcessEvent:
				backgroundEventQueueMap.put(et.getEventTypeKey(), queue);
				break;
				
			case extEvent:
				extEventQueueMap.put(et.getEventTypeKey(), queue);
				break;
				
			default:
				break;


			}
	
		}
		
		
	}
	
	
	protected void discardEventQueues () {
		
		eventCategoryQueueMap.clear();
		eventCategoryQueueMap = null;
	
		backgroundEventQueueMap.clear();
		backgroundEventQueueMap = null;
		
		extEventQueueMap.clear();
		extEventQueueMap = null;


		processReceivedMessageEventQueue = null;
		retrieveMessageEventQueue = null;
		pushMessageEventQueue = null;
		pushSystemMessageEventQueue = null;
		processAckCallbackEventQueue = null;
		processMsgReceivedCallbackEventQueue = null;

		
	}
	
	
	protected void verifyEventQueues() {
		if (processReceivedMessageEventQueue == null
				|| retrieveMessageEventQueue == null
				|| pushMessageEventQueue == null
				|| pushSystemMessageEventQueue == null
				|| processAckCallbackEventQueue == null
				|| processMsgReceivedCallbackEventQueue == null) {
		
			throw new EventQueuesInitializationException("Every event type should be assigned to an event queue.");
		}
		
		for (String backgroundProcessKey : backgroundProcesses.keySet()) {
			BackgroundProcess bp = backgroundProcesses.get(backgroundProcessKey);
			if (((! backgroundEventQueueMap.containsKey(bp.getEventTypeKey())) || backgroundEventQueueMap.get(bp.getEventTypeKey()) == null)
					&& (! backgroundEventQueueMap.containsKey(""))) {
				throw new EventQueuesInitializationException("Event queue not defined for a background process: " + backgroundProcessKey);
			}
		}
		
	}
	
	
    
    
    
    
    //methods executed by the API classes:
    
	public void join(String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg) {
    	joinManager.join(bootstrapNodeAddress, joinCallback, callbackArg);
    	
    }
	
    public void join(String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, Object[] joinParameters) {
    	joinManager.join(bootstrapNodeAddress, joinCallback, callbackArg, joinParameters);
    	
    }

    public void leave() {
    	leaveManager.leave();
    	
    }

    
	public LookupCallback lookup(NodeId lookupNodeId, LookupCallback lookupCallback, Object callbackArg) {
		return lookupManager.lookup(lookupNodeId, lookupCallback, callbackArg);

	}
	
	public LookupCallback lookup(NodeId lookupNodeId, LookupCallback lookupCallback, Object callbackArg, Object[] parameters) {
		return lookupManager.lookup(lookupNodeId, lookupCallback, callbackArg, parameters);

	}
	

	public SearchCallback search(NodeId seachNodeId, short k, SearchCallback searchCallback, Object callbackArg) {
		return searchManager.search(seachNodeId, k, searchCallback, callbackArg);
		
	}
	

	public SearchCallback search(NodeId seachNodeId, NodePointer[] initialNodes, short k, SearchCallback searchCallback, Object callbackArg) {
		return searchManager.search(seachNodeId, initialNodes, k, searchCallback, callbackArg);
		
	}
	

	public SearchCallback search(NodeId seachNodeId, short k, boolean ignoreTargetNode, SearchCallback searchCallback, Object callbackArg) {
		return searchManager.search(seachNodeId, k, ignoreTargetNode, searchCallback, callbackArg);
	}
	

	public SearchCallback search(NodeId seachNodeId, NodePointer[] initialNodes, short k, boolean ignoreTargetNode, SearchCallback searchCallback, Object callbackArg) {
		return searchManager.search(seachNodeId, initialNodes, k, ignoreTargetNode, searchCallback, callbackArg);
	}
	

	
	public SearchCallback search(NodeId seachNodeId, short k, SearchCallback searchCallback, Object callbackArg, Object[] parameters) {
		return searchManager.search(seachNodeId, k, searchCallback, callbackArg, parameters);
		
	}
	

	public SearchCallback search(NodeId seachNodeId, NodePointer[] initialNodes, short k, SearchCallback searchCallback, Object callbackArg, Object[] parameters) {
		return searchManager.search(seachNodeId, initialNodes, k, searchCallback, callbackArg, parameters);
		
	}
	

	public SearchCallback search(NodeId seachNodeId, short k, boolean ignoreTargetNode, SearchCallback searchCallback, Object callbackArg, Object[] parameters) {
		return searchManager.search(seachNodeId, k, ignoreTargetNode, searchCallback, callbackArg, parameters);
	}
	

	public SearchCallback search(NodeId seachNodeId, NodePointer[] initialNodes, short k, boolean ignoreTargetNode, SearchCallback searchCallback, Object callbackArg, Object[] parameters) {
		return searchManager.search(seachNodeId, initialNodes, k, ignoreTargetNode, searchCallback, callbackArg, parameters);
	}

	
	
	
	
	public PutCallback put(NodePointer np, BigInteger key, Object value, PutCallback putCallback, Object putCallbackArg) {
		return dhtManager.put(np, key, value, putCallback, putCallbackArg);
	}
	
	
	public RefreshPutCallback refreshPut(NodePointer np, BigInteger key, Object value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg) {
		return dhtManager.refreshPut(np, key, value, refreshPutCallback, refreshPutCallbackArg);
	}
	
	
	public GetCallback get(NodePointer np, BigInteger key, Object detail, GetCallback getCallback, Object getCallbackArg) {
		return dhtManager.get(np, key, detail, getCallback, getCallbackArg);
	}
	
	public PutCallback put(BigInteger key, Object value, PutCallback putCallback, Object putCallbackArg) {
		return dhtManager.put(key, value, putCallback, putCallbackArg);
	}
	
	
	public RefreshPutCallback refreshPut(BigInteger key, Object value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg) {
		return dhtManager.refreshPut(key, value, refreshPutCallback, refreshPutCallbackArg);
	}
	
	
	public GetCallback get(BigInteger key, Object detail, GetCallback getCallback, Object getCallbackArg) {
		return dhtManager.get(key, detail, getCallback, getCallbackArg);
	}
	
	
	public DeleteCallback delete(NodePointer np, BigInteger key, Object detail, DeleteCallback deleteCallback, Object deleteCallbackArg) {
		return dhtManager.delete(np, key, detail, deleteCallback, deleteCallbackArg);
	}
    
	public PutCallback put(NodePointer np, BigInteger key, Object value, PutCallback putCallback, Object putCallbackArg, Object[] parameters) {
		return dhtManager.put(np, key, value, putCallback, putCallbackArg, parameters);
	}
	
	
	public RefreshPutCallback refreshPut(NodePointer np, BigInteger key, Object value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg, Object[] parameters) {
		return dhtManager.refreshPut(np, key, value, refreshPutCallback, refreshPutCallbackArg, parameters);
	}
	
	
	public GetCallback get(NodePointer np, BigInteger key, Object detail, GetCallback getCallback, Object getCallbackArg, Object[] parameters) {
		return dhtManager.get(np, key, detail, getCallback, getCallbackArg, parameters);
	}
	
	public PutCallback put(BigInteger key, Object value, PutCallback putCallback, Object putCallbackArg, Object[] parameters) {
		return dhtManager.put(key, value, putCallback, putCallbackArg, parameters);
	}
	
	
	public RefreshPutCallback refreshPut(BigInteger key, Object value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg, Object[] parameters) {
		return dhtManager.refreshPut(key, value, refreshPutCallback, refreshPutCallbackArg, parameters);
	}
	
	
	public GetCallback get(BigInteger key, Object detail, GetCallback getCallback, Object getCallbackArg, Object[] parameters) {
		return dhtManager.get(key, detail, getCallback, getCallbackArg, parameters);
	}
	
	
	public DeleteCallback delete(NodePointer np, BigInteger key, Object detail, DeleteCallback deleteCallback, Object deleteCallbackArg, Object[] parameters) {
		return dhtManager.delete(np, key, detail, deleteCallback, deleteCallbackArg, parameters);
	}
	

    

    
    public void discard() {
    	
    	if (userLog.isInfoEnabled()) {
    		userLog.info("Discarding node. Node id: " + nodeId.toHexString() + ", Network address: " + nodePointer.getNetworkNodePointer().getAddressString() + ".");
    	}
    	if (devLog.isInfoEnabled()) {
    		devLog.info("Discarding node. Node id: " + nodeId.toHexString() + ", Network address: " + nodePointer.getNetworkNodePointer().getAddressString() + ".");
    	}
    	

    	this.discardLock.writeLock().lock();
    	
    	
    	this.discarded = true;
    	
    	
		
		//discard background processes:
		for (BackgroundProcess backgroundProcess : backgroundProcesses.values()) {
			backgroundProcess.discard();
			
		}
		backgroundProcesses.clear();
		backgroundProcesses = null;

		
		
		    	
    	
		//received message processors
		for (ReceivedMessageProcessor rmp : receivedMessageProcessors.values()) {
			rmp.discard();
		}
		receivedMessageProcessors.clear();
		receivedMessageProcessors = null;				
		
		
		//message send processors:
		for (MessageSendProcessor msp : messageSendProcessors.values()) {
			msp.discard();
		}
		messageSendProcessors.clear();
		messageSendProcessors = null;
		

	
		
		//dht manager:
		dhtManager.discard();
		dhtManager = null;
		

		//routing manager:
		routingManager.discard();
		routingManager = null;
		
				
		//lookup manager:
		lookupManager.discard();
		lookupManager = null;

		
		//search manager:
		searchManager.discard();
		searchManager = null;		
		
		
		//join manager:
		joinManager.discard();
		joinManager = null;

		
		
		//leave manager:
		leaveManager.discard();
		leaveManager = null;
		
		

		//next hop selectors:
		for (NextHopSelector nhs : nextHopSelectors.values()) {
			nhs.discard();
		}
		nextHopSelectors.clear();
		nextHopSelectors = null;
		
		
		
		//notify processor:
		notifyProcessor.discard();
		notifyProcessor = null;
		
		
		
		//network adapter:
		try {
			networkAdapter.discard();
		} catch (NetworkAdapterException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while discarding the network adapter.", e);
		}
		networkAdapter = null;
		
		
		//event scheduler:
		//the event scheduler should not be discarded, it may be shared by many nodes
		eventScheduler = null;
		
		
		//event queues:
		discardEventQueues();
		
		
		//message queues:
		appPortMessageInQueues = null;
		appPortMessageReceivedCallbacks = null;
		appMessageInQueue = null;
		
		
		//routing tables
		routingTable.discard();
		routingTable = null;

		
		//message factory:
		messageFactory.discard();
		messageFactory = null;
				
		
		//nodeId factory:
		nodeIdFactory.discard();
		nodeIdFactory = null;
		

		
		
		
		//extensions:
		for (Extension ext : extensions.values()) {
			ext.discard();
		}
		extensions.clear();
		extensions = null;
		
		
		

		
		nodeAccessor = null;		

		
		nodeId = null;
		nodePointer = null;
		timeProvider = null;
		nodeProcessEventProxy = null;
		
		properties = null;
		parameters = null;
		
		
		
		environment = null;
		
		
		this.discardLock.writeLock().unlock();
		

    }
    
    
    
    
	
    
    //methods for registering and unregistering ports and message received callbacks - these methods should be used where ports are used:

	public LinkedBlockingQueue<ReceivedDataMessage> registerPort(short port) {
		return registerPort(port, null);
	}
    
	public LinkedBlockingQueue<ReceivedDataMessage> registerPort(short port, MessageReceivedCallback callback) {
		if (devLog.isInfoEnabled()) {
			devLog.info("Registering queue for port " + port + ".");
		}
		if (userLog.isInfoEnabled()) {
			userLog.info("Registering queue for port " + port + ".");
		}
		synchronized (appMessageInLock) {
			if (callback != null) {
				appPortMessageReceivedCallbacks.put(port, callback);
			}
			if (appPortMessageInQueues.containsKey(port)) return appPortMessageInQueues.get(port);
			else {
				LinkedBlockingQueue<ReceivedDataMessage> portQueue = new LinkedBlockingQueue<ReceivedDataMessage>();
				appPortMessageInQueues.put(port, portQueue);
				return portQueue;
			}
		}
	}
	
	public void registerMessageReceivedCallbackForPort(short port, MessageReceivedCallback callback) {
		appPortMessageReceivedCallbacks.put(port, callback);
	}
	
	public void unregisterMessageReceivedCallbackForPort(short port) {
		if (appPortMessageReceivedCallbacks.containsKey(port)) appPortMessageReceivedCallbacks.remove(port);
	}
	
	public void unregisterPort(short port) {
		if (devLog.isInfoEnabled()) {
			devLog.info("Unregistering queue for port " + port + ".");
		}
		if (userLog.isInfoEnabled()) {
			userLog.info("Unregistering queue for port " + port + ".");
		}
		synchronized (appMessageInLock) {
			if (appPortMessageInQueues.containsKey(port)) appPortMessageInQueues.remove(port);
			if (appPortMessageReceivedCallbacks.containsKey(port)) appPortMessageReceivedCallbacks.remove(port);
		}
	}

	
	
	
	//methods for registering and unregistering message received callback - these methods should be used where ports are NOTused:
	
	public void registerMessageReceivedCallback(MessageReceivedCallback callback) {
		if (devLog.isInfoEnabled()) {
			devLog.info("Registering message received callback.");
		}
		if (userLog.isInfoEnabled()) {
			userLog.info("Registering message received callback.");
		}
		appMessageReceivedCallback = callback;
	}
	
	public void unregisterMessageReceivedCallback() {
		if (devLog.isInfoEnabled()) {
			devLog.info("Unregistering message received callback.");
		}
		if (userLog.isInfoEnabled()) {
			userLog.info("Unregistering message received callback.");
		}
		appMessageReceivedCallback = null;
	}

	
	
	
	
	
	//methods sending messages:

    /*
     * Sends a message
     */
	protected boolean sendMessage(MessageSendProcessInfo info, boolean wait) throws NetworkAdapterException, ProcessMessageException {
    	if (devLog.isDebugEnabled()) {
			devLog.debug("Sending message #" + info.getMsg().getSerialNoAndSenderString() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Sending message #" + info.getMsg().getSerialNoAndSenderString() + ".");
		}
		
		
		
		boolean processSendResult = processSendMessage(info);
		
		if (processSendResult == false) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("Message #" + info.getMsg().getSerialNoAndSenderString() + " dropped. A message send processor discarded the message.");
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("Message #" + info.getMsg().getSerialNoAndSenderString() + " dropped. A message send processor discarded the message.");
			}
			return false;
		}
		

		
//    	if (info.getDirectRecipient() != null) {
//    		//send message directly to the node
//    		return sendMessageToNode(info, wait);
//    	}
//    	else {
//    		//route message
//    		return routeMessage(info, wait);
//    	}
    	
    	
    	//route message
		return routeMessage(info, wait);
		
    	
    }
	
	
	protected boolean sendMessageToNode(MessageSendProcessInfo info, boolean wait) throws NetworkAdapterException, ProcessMessageException {
    	
		info.getMsg().updateCRC32();
		
    	if (wait) {
    		pushMessage(info);
    	}
    	else {
    		enqueuePushMessage(info);
    	}

    	return true;
    	
    }
	
	//puts the message to the out queue, return immediately and let the queue processing thread send it
	protected void enqueuePushMessage(MessageSendProcessInfo info) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Enqueueing pushMessage event#" + info.getMsg().getSerialNoAndSenderString() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Enqueueing pushMessage event #" + info.getMsg().getSerialNoAndSenderString() + ".");
		}
		
    	if (info.getMsg().isSystemMessage()) {	
    		pushSystemMessageEventQueue.add(new PushMessageEvent(timeProvider.getCurrentTime(), this.nodeProcessEventProxy, (MessageSendProcessInfo) info, true));
    	}
    	else {
    		pushMessageEventQueue.add(new PushMessageEvent(timeProvider.getCurrentTime(), this.nodeProcessEventProxy, (MessageSendProcessInfo) info, false));
    	}
    	
    }

	
	

	
	
	protected boolean routeMessage(MessageSendProcessInfo info, boolean wait) throws NetworkAdapterException, ProcessMessageException {
		
		return this.routingManager.routeMessage(info, wait);
		
    }
    
    



    /*
     * Processes the message before sending
     */
	protected boolean processSendMessage(MessageSendProcessInfo info) throws ProcessMessageException {

		if (info.getProcessBeforeSend()) {

			boolean result = true;
			
    		for (MessageSendProcessor msp : messageSendProcessors.values()) {
    			result = msp.processSendMessage(info);
    			if (result == false) break;
    		}
    		
    		if (result == false) return false;
    		
    	}
		
		return true;

		
	}

	
	
	
    /*
     * pushes the message to the transport layer
     */

	protected void pushMessage(MessageSendProcessInfo info) throws NetworkAdapterException {
		pushMessage(info.getMsg(), info.getDirectRecipient());
	}
	
    
    /*
     * pushes the message to the transport layer
     */
    protected void pushMessage(Message msg, NetworkNodePointer directRecipient) throws NetworkAdapterException {
    	if (devLog.isDebugEnabled()) {
			devLog.debug("Pushing message #" + msg.getSerialNoAndSenderString() + " to the transport layer.");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Pushing message #" + msg.getSerialNoAndSenderString() + " to the transport layer.");
		}
		networkAdapter.sendMessage(msg, directRecipient);
    }

 
    
    
    
    
    
    //methods from sending data messages (will be called by the API classes):
    
    public MessageSendInfo sendDataMessage(NodeId recipientId, short sourcePort, short destinationPort, byte[] data, MessageAckCallback ackCallback, Object callbackArg, boolean wait) throws NetworkAdapterException, ProcessMessageException {
    	return sendDataMessage(recipientId, sourcePort, destinationPort, (NetworkNodePointer)null, data, ackCallback, callbackArg, wait);
    }
    
    
    public MessageSendInfo sendDataMessage(NodeId recipientId, short sourcePort, short destinationPort, String directRecipientNetworkAddress, byte[] data, MessageAckCallback ackCallback, Object callbackArg, boolean wait) throws NetworkAdapterException, ProcessMessageException {
    	return sendDataMessage(recipientId, sourcePort, destinationPort, directRecipientNetworkAddress, data, ackCallback, callbackArg, wait, null);
    }
    
    public MessageSendInfo sendDataMessage(NodeId recipientId, short sourcePort, short destinationPort, String directRecipientNetworkAddress, byte[] data, MessageAckCallback ackCallback, Object callbackArg, boolean wait, Object[] routingParameters) throws NetworkAdapterException, ProcessMessageException {
    	NetworkNodePointer nnp = null;
    	if (directRecipientNetworkAddress != null) {
    		nnp = networkAdapter.createNetworkNodePointer(directRecipientNetworkAddress);
    	}
    	return sendDataMessage(recipientId, sourcePort, destinationPort, nnp, data, ackCallback, callbackArg, wait, routingParameters);
    }
    
    public MessageSendInfo sendDataMessage(NodeId recipientId, short sourcePort, short destinationPort, NetworkNodePointer directRecipient, byte[] data, MessageAckCallback ackCallback, Object callbackArg, boolean wait) throws NetworkAdapterException, ProcessMessageException {
    	return sendDataMessage(recipientId, sourcePort, destinationPort, directRecipient, data, ackCallback, callbackArg, wait, null);
    }
    
    public MessageSendInfo sendDataMessage(NodeId recipientId, short sourcePort, short destinationPort, NetworkNodePointer directRecipient, byte[] data, MessageAckCallback ackCallback, Object callbackArg, boolean wait, Object[] routingParameters) throws NetworkAdapterException, ProcessMessageException {
    	Message msg = messageFactory.newDataMessage(getNextMessageSerialNo(), this.nodeId, recipientId, this.networkAdapter.getPublicAddressBytes(), parameters.getMessageTTL(), (short)0, sourcePort, destinationPort, data);
    	
		int sendAttempts = 1;
		if (parameters.isResendIfNoAck()) {
			sendAttempts = 1 + nodeAccessor.getNodeParameterSet().getSendRetries();
		}
		
    	AckProcessInfo appAckProcessInfo = new AckProcessInfo(msg, directRecipient, ackCallback, callbackArg, sendAttempts, parameters.getAckTimeout(), routingParameters);
    	appAckProcessInfo.setSendCounter(1);
    	
    	return sendDataMessage(msg, directRecipient, appAckProcessInfo, wait, routingParameters);
    	
    }
    
    public MessageSendInfo sendDataMessage(Message msg, NetworkNodePointer directRecipient, AckProcessInfo appAckProcessInfo, boolean wait, Object[] routingParameters) throws NetworkAdapterException, ProcessMessageException {
    	
    	if (appAckProcessInfo.getAckTimeout() < 0) {
    		throw new UnrecoverableRuntimeException("Ack timeout should be set to a non-negative value.");
    	}
    	if (appAckProcessInfo.getAckTimeout() == 0) appAckProcessInfo.setAckTimeout(parameters.getAckTimeout());
    	
    	//check the message size:
    	int maxMessageLength = getMaxMessageLength();
    	if (maxMessageLength != -1 && msg.getByteLength() > maxMessageLength) {
    		throw new IllegalArgumentException("The message byte length exceeds the maximum allowed value.");
    	}
    	
    	DataMessageSendProcessInfo dmspi = new DataMessageSendProcessInfo(msg, directRecipient, appAckProcessInfo, true, false, routingParameters);
    	
    	sendMessage(dmspi, wait);
    	
    	return new MessageSendInfo(msg.getSerialNo());
    	
    }
    
    
    
    
    
    
    
    
    /*
     * Resends a data message (when ack message was not received)
     */
    protected boolean resendMessage(AckProcessInfo prevAckProcessInfo) throws NetworkAdapterException, ProcessMessageException {
    	
    	Message msg = prevAckProcessInfo.getMessage();
    	
    	msg.setTtl(parameters.getMessageTTL());
    	msg.setHopCount((short) 0);
    	
    	if (devLog.isDebugEnabled()) {
			devLog.debug("Resending message #" + msg.getSerialNoAndSenderString() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Resending message #" + msg.getSerialNoAndSenderString() + ".");
		}

		Object[] routingParameters = prevAckProcessInfo.getRoutingParameters();
		
		AckProcessInfo newAckProcessInfo = new AckProcessInfo(prevAckProcessInfo.getMessage(), prevAckProcessInfo.getDirectRecipient(), prevAckProcessInfo.getAckCallback(), prevAckProcessInfo.getAckCallbackArg(), prevAckProcessInfo.getSendAttempts(), prevAckProcessInfo.getAckTimeout(), routingParameters);
		newAckProcessInfo.setSendCounter(prevAckProcessInfo.getSendCounter() + 1);
    	return sendMessage(new DataMessageSendProcessInfo(msg, newAckProcessInfo.getDirectRecipient(), newAckProcessInfo, true, true, routingParameters), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
    	
    }
    
    
    
    
    
    
    //method called by networkAdapter when a message is received:
    
	protected void messageReceived(Message msg, NetworkNodePointer directSender) {
		if (devLog.isDebugEnabled()) {
			devLog.debug("Enqueue the received message for processing.");
		}

		processReceivedMessageEventQueue.add(new ProcessReceivedMessageEvent(environment.getTimeProvider().getCurrentTime(), this.nodeProcessEventProxy, msg, directSender));
		
	}
    
    
    
    
    
    
    
    //processing events (events run by background thread(s)):
    
    //processes an event
    protected void processEvent(Event event) throws EventProcessException {

    	//guarantee that the node will not be discarded while processing the event, this is a readwritelock, so multiple events may be processed simultanously
    	//this lock must be unlocked before any exit point from the method -> all the code in a try-finally section
    	this.discardLock.readLock().lock();
    	try {
    	
	    	if (discarded) {
	    		if (devLog.isDebugEnabled()) {
					devLog.debug("The node is discarded. The event will not be processed.");
				}
				if (msgLog.isInfoEnabled()) {
					msgLog.info("The node is discarded. The event will not be processed.");
				}
	    		return;
	    		
	    	}
	    	
	    	if (devLog.isDebugEnabled()) {
	    		devLog.debug("Processing event.");
	    	}
	    	
		   	EventCategory ec = event.getEventCategory();
//		   	Object[] eventArgs = event.getEventArgs();
	
		   	try {
		   	
			   	switch (ec) {
			   	case receiveMessageEvent:
			   		//this event type will not be executed in the node context, such events will be processed directly by MessageReceiver instances
					break;
				case processReceivedMessageEvent:
					ProcessReceivedMessageEvent prme = (ProcessReceivedMessageEvent) event;
					processReceivedMessage(prme.getMessage(), prme.getDirectSender());
					break;
			   	case pushMessageEvent:
			   		PushMessageEvent pme = (PushMessageEvent) event;
			   		pushMessage(pme.getMessageSendProcessInfo());
			   		break;
			   	case pushSystemMessageEvent:
			   		PushMessageEvent spme = (PushMessageEvent) event;
			   		pushMessage(spme.getMessageSendProcessInfo());
			   		break;
				case processAckCallbackEvent:
					MessageAckCallbackEvent mace = (MessageAckCallbackEvent) event;
					if (mace.getMessageAckCallbackType() == MessageAckCallbackType.DELIVERED) {
						mace.getMessageAckCallback().notifyDelivered(mace.getCallbackArg());
					}
					else if (mace.getMessageAckCallbackType() == MessageAckCallbackType.UNDELIVERED) {
						mace.getMessageAckCallback().notifyUndelivered(mace.getCallbackArg());
					}
					break;
				case processMsgReceivedCallbackEvent:
					MessageReceivedCallbackEvent mrce = (MessageReceivedCallbackEvent) event;
					if (mrce.getPort() == null) {
						mrce.getMessageReceivedCallback().notifyReceived(mrce.getMessage());
					}
					else {
						mrce.getMessageReceivedCallback().notifyReceived(mrce.getMessage(), mrce.getPort());
					}
					break;
				case extEvent:
					//processing logic for events of this category is not implemented here and should be executed by a different ProcessEventProxy instance, registered when adding the event to the queue
					break;
				default:
					break;
					
			   	}
			   	
		   	}
		   	catch (Exception e) {
		   		throw new EventProcessException("An exception thrown while processing an event.", e);
		   	}
    	}
    	finally {
    		//unlock the discard lock
    		this.discardLock.readLock().unlock();
    	}
    	
    }


    
    
    protected void executeBackgroundProcess(BackgroundProcess backgroundProcess) throws BackgroundProcessException {
    	backgroundProcess.process();
    	
    }
    
    

	//methods processing messages received from the network layer
    
    /**
     * processes messages received from the network layer
     * @param msg
     * @throws ProcessMessageException 
     */
	protected void processReceivedMessage(Message msg, NetworkNodePointer directSender) throws ProcessMessageException {
    	try {
			validateMessage(msg);
		} catch (MessageErrorException e) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("Message error. The message was dropped.", e);
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("Message dropped. " + e.getMessage());
			}
			return;
		}
    	
    	for (ReceivedMessageProcessor rmp : receivedMessageProcessors.values()) {
    		rmp.processMessage(msg, directSender);
    	}
    	
    }
    
    
    

	
	public EntryPoint getExtensionEntryPoint(String extensionKey) {
		if (extensions != null && extensions.containsKey(extensionKey)) {
			return extensions.get(extensionKey).getExtensionEntryPoint();
		}
		else return null;
		
	}
	
	
	public BackgroundProcessEntryPoint getBackgroundProcessEntryPoint(String backgroundProcessKey) {
		if (backgroundProcesses != null && backgroundProcesses.containsKey(backgroundProcessKey)) {
			return backgroundProcesses.get(backgroundProcessKey).getBackgroundProcessEntryPoint();
		}
		else return null;
		
	}
	
	
	public EntryPoint getRoutingManagerEntryPoint() {
		return routingManager.getEntryPoint();
		
	}
	
	
	public EntryPoint getLookupManagerEntryPoint() {
		return lookupManager.getEntryPoint();
		
	}
	
	
	public EntryPoint getSearchManagerEntryPoint() {
		return searchManager.getEntryPoint();
		
	}
	
	
	public EntryPoint getJoinManagerEntryPoint() {
		return joinManager.getEntryPoint();
		
	}
	
	
	public EntryPoint getDHTManagerEntryPoint() {
		return dhtManager.getEntryPoint();
		
	}
	
	
    
	
	
	//returns the message size limit based on the network adapter limit and the message factory limit
	public int getMaxMessageLength() {
		return maxMessageLength;
	}
	
	
	//returns the message size limit based on the network adapter limit and the message factory limit
	public int calcuateMaxMessageLength() {
		
		int messageFactoryLimit = messageFactory.getMaxMessageLength();
		int networkAdapterLimit = networkAdapter.getMaxMessageLength();
		
		int maxMessageLength = 0;
		
		if (messageFactoryLimit != 0) {
			if (messageFactoryLimit < maxMessageLength || maxMessageLength == 0) maxMessageLength = messageFactoryLimit;
		}
		
		if (networkAdapterLimit != 0) {
			if (networkAdapterLimit < maxMessageLength || maxMessageLength == 0) maxMessageLength = networkAdapterLimit;
		}
		
		int headerLength = messageFactory.getMessageHeaderLength();
		
		int maxDataLength = maxMessageLength - headerLength; 
		
		if (maxMessageLength == 0) {
			maxMessageLength = -1;	//no limit
			maxDataLength = -1;	//no limit
		}
		
		
		if (networkAdapter.isFragmentMessages()) {
			int fragmentLength = networkAdapter.getMessageFragmentLength();
			int maxFragmentsCount = networkAdapter.getMaxMassageFragmentsCount();
			int dataFragmentSize = fragmentLength - headerLength;
			
			if (fragmentLength <= headerLength) {
				if (maxMessageLength == -1) return fragmentLength;
				else return Math.min(maxMessageLength, fragmentLength);
			}
			
			if (maxFragmentsCount == 0) {
				if (maxMessageLength == -1) return -1;	//no limit
				else return maxMessageLength;
			}

			long maxFragmentedDataLength = dataFragmentSize * maxFragmentsCount;
			maxFragmentedDataLength = Math.min(Integer.MAX_VALUE, maxFragmentedDataLength);
			
			if (maxDataLength == -1 || (maxFragmentedDataLength != 0 && maxFragmentedDataLength < maxDataLength)) maxDataLength = (int) maxFragmentedDataLength;
			
			if (maxMessageLength == -1 || (maxDataLength < maxMessageLength - headerLength)) maxMessageLength = maxDataLength + headerLength;
				
		}	
		
		return maxMessageLength;
		
	}
	
	
	
	//returns the message data size limit based on the network adapter limit and the message factory limit
	public int calcuateMaxMessageDataLength() {
		return maxMessageDataLength;
	}
	
	
	//returns the message data size limit based on the network adapter limit and the message factory limit
	public int getMaxMessageDataLength() {
		
		
		int messageFactoryLimit = messageFactory.getMaxMessageLength();
		int networkAdapterLimit = networkAdapter.getMaxMessageLength();
		
		int maxMessageLength = 0;
		
		if (messageFactoryLimit != 0) {
			if (messageFactoryLimit < maxMessageLength || maxMessageLength == 0) maxMessageLength = messageFactoryLimit;
		}
		
		if (networkAdapterLimit != 0) {
			if (networkAdapterLimit < maxMessageLength || maxMessageLength == 0) maxMessageLength = networkAdapterLimit;
		}
		
		int headerLength = messageFactory.getMessageHeaderLength(); 
		
		int maxDataLength = maxMessageLength - headerLength;
		
		if (maxMessageLength == 0) {
			maxMessageLength = -1;	//no limit
			maxDataLength = -1;	//no limit
		}
		
		if (networkAdapter.isFragmentMessages()) {
			int fragmentLength = networkAdapter.getMessageFragmentLength();
			int maxFragmentsCount = networkAdapter.getMaxMassageFragmentsCount();
			int dataFragmentSize = fragmentLength - headerLength;
			
			if (fragmentLength <= headerLength) return 0;
			
			if (maxFragmentsCount == 0) {
				if (maxMessageLength == -1) return -1;	//no limit
				else return maxMessageLength;
			}
			
			long maxFragmentedDataLength = dataFragmentSize * maxFragmentsCount;
			maxFragmentedDataLength = Math.min(Integer.MAX_VALUE, maxFragmentedDataLength);
			
			if (maxDataLength == -1 || (maxFragmentedDataLength != 0 && maxFragmentedDataLength < maxDataLength)) maxDataLength = (int) maxFragmentedDataLength;
				
		}		
		
		return maxDataLength;
		
	}
	
	
	
	public int getMessageHeaderLength() {
		return messageFactory.getMessageHeaderLength();
	}
	
	
	
    
    //helper methods:

    protected void validateMessage(Message msg) throws MessageErrorException {
    	
    	if (msg == null) {
    		//message corrupted
    		throw new MessageErrorException("Message corrupted");
    	}
    	
    	msg.validate();
    	
    	if (networkAdapter.validateNetworkAddress(msg.getSenderNetworkAddress()) == null) {
    		//invalid network address
    		throw new MessageErrorException("Sender network address is invalid. Message #" + msg.getSerialNoAndSenderString());
    	}

    	
    }
	
	
    
    
	public long getCurrentTimestamp() {
		return timeProvider.getCurrentTime();
	}
	

    protected int getNextMessageSerialNo() {
    	synchronized (msgSerialNoCounterLock) {
	    	int currMsgSerialNo = msgSerialNoCounter;
	    	msgSerialNoCounter++;
	    	return currMsgSerialNo;
    	}
    }
	
	
	

	protected class NodeAccessorImpl implements NodeAccessor {

		@Override
		public Node getNode() {
			return Node.this;
		}

		@Override
		public NodeId getNodeId() {
			return Node.this.getNodeId();
		}

		@Override
		public NodeIdFactory getNodeIdFactory() {
			return Node.this.nodeIdFactory;
		}

		@Override
		public RoutingTable getRoutingTable() {
			return Node.this.routingTable;
		}

		@Override
		public Environment getEnvironment() {
			return Node.this.getEnvironment();
		}

		@Override
		public MessageFactory getMessageFactory() {
			return Node.this.messageFactory;
		}

		@Override
		public NodeProperties getProperties() {
			return Node.this.properties;
		}

		@Override
		public NodeParameterSet getNodeParameterSet() {
			return Node.this.parameters;
		}

		@Override
		public LinkedBlockingQueue<ReceivedDataMessage> getAppMessageInQueue() {
			return Node.this.appMessageInQueue;
		}

		@Override
		public Map<Short, LinkedBlockingQueue<ReceivedDataMessage>> getAppPortMessageInQueues() {
			return Node.this.appPortMessageInQueues;
		}

		@Override
		public MessageReceivedCallback getAppMessageReceivedCallback() {
			return Node.this.appMessageReceivedCallback;
		}

		@Override
		public Map<Short, MessageReceivedCallback> getAppPortMessageReceivedCallbacks() {
			return Node.this.appPortMessageReceivedCallbacks;
		}

		@Override
		public Object getAppMessageInLock() {
			return Node.this.appMessageInLock;
		}
		
		@Override
		public Collection<ReceivedMessageProcessor> getReceivedMessageProcessors() {
			return Node.this.receivedMessageProcessors.values();
		}
		
		@Override
		public Collection<MessageSendProcessor> getMessageSendProcessors() {
			return Node.this.messageSendProcessors.values();
		}

		@Override
		public NextHopSelector getNextHopSelector(String nextHopSelectorKey) {
			return Node.this.nextHopSelectors.get(nextHopSelectorKey);
		}
		
		@Override
		public RoutingManager getRoutingManager() {
			return Node.this.routingManager;
		}

		@Override
		public LookupManager getLookupManager() {
			return Node.this.lookupManager;
		}
		
		@Override
		public SearchManager getSearchManager() {
			return Node.this.searchManager;
		}
		
		@Override
		public JoinManager getJoinManager() {
			return Node.this.joinManager;
		}
		
		@Override
		public LeaveManager getLeaveManager() {
			return Node.this.leaveManager;
		}
		
		@Override
		public DHTManager getDHTManager() {
			return Node.this.dhtManager;
		}
		
		@Override
		public NotifyProcessor getNotifyProcessor() {
			return Node.this.notifyProcessor;
		}

		@Override
		public NetworkAdapter getNetworkAdapter() {
			return Node.this.networkAdapter;
		}

		@Override
		public NodePointer getNodePointer() {
			return Node.this.nodePointer;
		}

		@Override
		public ProcessEventProxy getProcessEventProxy() {
			return Node.this.nodeProcessEventProxy;
		}
		
		@Override
		public ReceivedMessageProcessProxy getNodeProcessReceivedMessageProxy() {
			return Node.this.nodeProcessReceivedMessageProxy;
		}
		
		@Override
		public Object getData(String key, Object defaultValue) {
			return Node.this.getData(key, defaultValue);
		}

		@Override
		public Object getData(String key) {
			return Node.this.getData(key);
		}

		@Override
		public void setData(String key, Object data) {
			Node.this.setData(key, data);
		}

		@Override
		public void removeData(String key) {
			Node.this.removeData(key);
		}

		@Override
		public Extension getExtension(String key) {
			return Node.this.extensions.get(key);
		}
		
		@Override
		public EntryPoint getExtensionEntryPoint(String extensionKey) {
			return Node.this.getExtensionEntryPoint(extensionKey);
		}
		
		@Override
		public BackgroundProcessEntryPoint getBackgroundProcessEntryPoint(String backgroundProcessKey) {
			return Node.this.getBackgroundProcessEntryPoint(backgroundProcessKey);
		}
		
		@Override
		public EntryPoint getRoutingManagerEntryPoint() {
			return Node.this.getRoutingManagerEntryPoint();
		}
		
		@Override
		public EntryPoint getLookupManagerEntryPoint() {
			return Node.this.getLookupManagerEntryPoint();
		}
		
		@Override
		public EntryPoint getSearchManagerEntryPoint() {
			return Node.this.getSearchManagerEntryPoint();
		}
		
		@Override
		public EntryPoint getJoinManagerEntryPoint() {
			return Node.this.getJoinManagerEntryPoint();
		}
		
		@Override
		public EntryPoint getDHTManagerEntryPoint() {
			return Node.this.getDHTManagerEntryPoint();
		}
		
		@Override
		public boolean sendMessage(MessageSendProcessInfo info, boolean wait) throws NetworkAdapterException, ProcessMessageException {
			return Node.this.sendMessage(info, wait);
			
		}
		
		@Override
		public boolean sendMessageToNode(MessageSendProcessInfo info, boolean wait) throws NetworkAdapterException, ProcessMessageException {
			return Node.this.sendMessageToNode(info, wait);
			
		}

		@Override
		public boolean resendMessage(AckProcessInfo ackProcessInfo) throws NetworkAdapterException, ProcessMessageException {
			return Node.this.resendMessage(ackProcessInfo);
			
		}

		@Override
		public int getNextMessageSerialNo() {
			return Node.this.getNextMessageSerialNo();
		}
		
		@Override
		public BlockingQueue<Event> getEventQueue(EventCategory eventCategory) {
			return Node.this.getEventQueue(eventCategory);
		}
		
		@Override
		public BlockingQueue<Event> getEventQueue(EventType eventType) {
			return Node.this.getEventQueue(eventType);
		}

		@Override
		public EventScheduler getEventScheduler() {
			return Node.this.eventScheduler;
		}
		
		@Override
		public ReentrantReadWriteLock getDiscardLock() {
			return discardLock;
		}
		
	}
	
	
	
	protected class NodeProcessEventProxy implements ProcessEventProxy {

		@Override
		public void processEvent(Event event) throws EventProcessException {
			Node.this.processEvent(event);
			
		}
		
	}
	
	protected class NodeReceivedMessageProcessProxy implements ReceivedMessageProcessProxy {

		@Override
		public void messageReceived(Message msg, NetworkNodePointer directSender) {
			Node.this.messageReceived(msg, directSender);
			
		}
		
	}


//	public static Object[] createMessageReceivedCallbackEventArg(MessageReceivedCallback callback, ReceivedDataMessage message, Short port) {
//		Object[] arg = new Object[] {callback, message, port};
//		return arg;
//	}
//	
//	public static Object[] createMessageReceivedCallbackEventArg(MessageReceivedCallback callback, ReceivedDataMessage message) {
//		Object[] arg = new Object[] {callback, message};
//		return arg;
//	}
//	
//	public static Object[] createAckCallbackEventArg(MessageAckCallbackType callbackType, MessageAckCallback callback, Object callbackArg) {
//		Object[] arg = new Object[] {callbackType, callback, callbackArg};
//		return arg;
//	}
//
//	public static Object[] createProcessReceivedMessageEventArg(Message message, NetworkNodePointer directSender) {
//		Object[] arg = new Object[] {message, directSender};
//		return arg;
//	}
	
    
}