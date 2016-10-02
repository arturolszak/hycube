package net.hycube.dht;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.HyCubeNodeId;
import net.hycube.core.HyCubeRoutingTable;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodeId;
import net.hycube.core.NodePointer;
import net.hycube.core.RoutingTableEntry;
import net.hycube.core.UnrecoverableRuntimeException;
import net.hycube.environment.NodeProperties;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventCategory;
import net.hycube.eventprocessing.EventProcessException;
import net.hycube.eventprocessing.EventScheduler;
import net.hycube.eventprocessing.EventType;
import net.hycube.eventprocessing.ProcessEventProxy;
import net.hycube.logging.LogHelper;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.HyCubeMessageFactory;
import net.hycube.messaging.messages.HyCubeMessageType;
import net.hycube.messaging.messages.Message;
import net.hycube.messaging.processing.MessageSendProcessInfo;
import net.hycube.messaging.processing.ProcessMessageException;
import net.hycube.metric.Metric;
import net.hycube.routing.HyCubeRoutingManager;
import net.hycube.transport.NetworkAdapterException;
import net.hycube.transport.NetworkNodePointer;
import net.hycube.utils.ClassInstanceLoadException;
import net.hycube.utils.ClassInstanceLoader;
import net.hycube.utils.HashMapUtils;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public class HyCubeRoutingDHTManager implements HyCubeDHTManager, HyCubeDHTManagerEntryPoint {

	
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeRoutingDHTManager.class); 
	
	
	protected static final String PROP_KEY_PUT_CALLBACK_EVENT_TYPE_KEY = "PutCallbackEventTypeKey";
	protected static final String PROP_KEY_REFRESH_PUT_CALLBACK_EVENT_TYPE_KEY = "RefreshPutCallbackEventTypeKey";
	protected static final String PROP_KEY_GET_CALLBACK_EVENT_TYPE_KEY = "GetCallbackEventTypeKey";
	protected static final String PROP_KEY_DELETE_CALLBACK_EVENT_TYPE_KEY = "DeleteCallbackEventTypeKey";
	
	protected static final String PROP_KEY_PUT_REQUEST_TIMEOUT_EVENT_TYPE_KEY = "PutRequestTimeoutEventTypeKey";
	protected static final String PROP_KEY_REFRESH_PUT_REQUEST_TIMEOUT_EVENT_TYPE_KEY = "RefreshPutRequestTimeoutEventTypeKey";
	protected static final String PROP_KEY_GET_REQUEST_TIMEOUT_EVENT_TYPE_KEY = "GetRequestTimeoutEventTypeKey";
	protected static final String PROP_KEY_DELETE_REQUEST_TIMEOUT_EVENT_TYPE_KEY = "DeleteRequestTimeoutEventTypeKey";
	
	protected static final String PROP_KEY_PUT_REQUEST_TIMEOUT = "PutRequestTimeout";
	protected static final String PROP_KEY_REFRESH_PUT_REQUEST_TIMEOUT = "RefreshPutRequestTimeout";
	protected static final String PROP_KEY_GET_REQUEST_TIMEOUT = "GetRequestTimeout";
	protected static final String PROP_KEY_DELETE_REQUEST_TIMEOUT = "DeleteRequestTimeout";
	
	protected static final String PROP_KEY_RESOURCE_STORE_TIME = "ResourceStoreTime";
	
	protected static final String PROP_KEY_DHT_STORAGE_MANAGER = "DHTStorageManager";
	
	protected static final String PROP_KEY_CHECK_IF_REPLICA_BEFORE_STORING = "CheckIfResourceReplicaBeforeStoring";
	protected static final String PROP_KEY_RESOURCE_STORE_NODES_NUM = "ResourceStoreNodesNum";
	protected static final String PROP_KEY_REPLICATION_NODES_NUM = "ReplicationNodesNum";
	
	protected static final String PROP_KEY_REPLICATE = "Replicate";
	protected static final String PROP_KEY_ANONYMOUS_REPLICATE = "AnonymousReplicate";
	protected static final String PROP_KEY_MAX_REPLICATION_NS_NODES_NUM = "MaxReplicationNSNodesNum";
	protected static final String PROP_KEY_MAX_REPLICATION_SPREAD_NODES_NUM = "MaxReplicationSpreadNodesNum";
	
	protected static final String PROP_KEY_ASSUME_NS_ORDERED = "AssumeNsOrdered";
	protected static final String PROP_KEY_DENSITY_CALCULATION_QUANTILE_FUNC_THRESHOLD = "DensityCalculationQuantileFuncThreshold";
	protected static final String PROP_KEY_METRIC = "Metric";
	protected static final String PROP_KEY_ESTIMATED_DISTANCE_COEFFICIENT = "EstimatedDistanceCoefficient";
	
	protected static final String PROP_KEY_PUT_RESPONSE_ANONYMOUS_ROUTE = "PutResponseAnonymousRoute";
	protected static final String PROP_KEY_REFRESH_PUT_RESPONSE_ANONYMOUS_ROUTE = "RefreshPutResponseAnonymousRoute";
	protected static final String PROP_KEY_GET_RESPONSE_ANONYMOUS_ROUTE = "GetResponseAnonymousRoute";
	protected static final String PROP_KEY_DELETE_RESPONSE_ANONYMOUS_ROUTE = "DeleteResponseAnonymousRoute";
	
	protected static final String PROP_KEY_REPLICATION_GET_REGISTER_ROUTE = "ReplicationGetRegisterRoute";
	protected static final String PROP_KEY_REPLICATION_GET_ANONYMOUS_ROUTE = "ReplicationGetAnonymousRoute";
	
	protected static final String PROP_KEY_RESOURCE_ACCESS_CONTROLLER = "ResourceAccessController";
	
	protected static final String PROP_KEY_RESOURCE_REPLICATION_SPREAD_MANAGER = "ResourceReplicationSpreadManager";
	
	protected static final String PROP_KEY_IGNORE_EXACT_PUT_REQUESTS = "IgnoreExactPutRequests";
	protected static final String PROP_KEY_IGNORE_EXACT_REFRESH_PUT_REQUESTS = "IgnoreExactRefreshPutRequests";
	protected static final String PROP_KEY_IGNORE_EXACT_GET_REQUESTS = "IgnoreExactGetRequests";
	protected static final String PROP_KEY_IGNORE_EXACT_DELETE_REQUESTS = "IgnoreExactDeleteRequests";
	
	protected static final String PROP_KEY_ESTIMATE_DENSITY_BASED_ON_LAST_NODE_ONLY = "EstimateDensityBasedOnLastNodeOnly";
	
	
	
	
	public static final boolean ANONYMOUS_REPLICATE_GENERATE_RANDOM_DIRECT_RECIPIENTS_ALL_NS = false;
	
	
	
	
	
	protected HyCubeDHTStorageManager dhtStorageManager;
	
	protected HashMap<Integer, HyCubePutRequestData> ongoingPutRequests;
	protected HashMap<Integer, HyCubeRefreshPutRequestData> ongoingRefreshPutRequests;
	protected HashMap<Integer, HyCubeGetRequestData> ongoingGetRequests;
	protected HashMap<Integer, HyCubeDeleteRequestData> ongoingDeleteRequests;
	
	protected int nextPutCommandId;
	protected int nextRefreshPutCommandId;
	protected int nextGetCommandId;
	protected int nextDeleteCommandId;
	
	
	protected Object dhtManagerLock;
	
	
	protected NodeAccessor nodeAccessor;
	protected NodeProperties properties;

	protected HyCubeMessageFactory messageFactory;
	
	
	
	protected EventType putCallbackEventType;
	protected EventType refreshPutCallbackEventType;
	protected EventType getCallbackEventType;
	protected EventType deleteCallbackEventType;
	
	
	protected EventType putRequestTimeoutEventType;
	protected EventType refreshPutRequestTimeoutEventType;
	protected EventType getRequestTimeoutEventType;
	protected EventType deleteRequestTimeoutEventType;
	
	
	protected int putRequestTimeout;
	protected int refreshPutRequestTimeout;
	protected int getRequestTimeout;
	protected int deleteRequestTimeout;
	
	
	
	protected int resourceStoreTime;
	
	
	protected boolean checkIfResourceReplicaBeforeStoring;
	protected int resourceStoreNodesNum;
	protected int replicationNodesNum;
	
	protected boolean replicate;
	protected boolean anonymousReplicate;
	protected int maxReplicationNSNodesNum;
	
	
	protected int maxReplicationSpreadNodesNum;
	
	
	protected HyCubeRoutingTable routingTable;
	protected boolean assumeNsOrdered;
	protected double densityCalculationQuantileFuncThreshold;
	protected Metric metric;
	protected double estDistCoefficient;
	
	
	protected boolean putResponseAnonymousRoute;
	protected boolean refreshPutResponseAnonymousRoute;
	protected boolean getResponseAnonymousRoute;
	protected boolean deleteResponseAnonymousRoute;
	
	
	boolean replicationGetRegisterRoute;
	boolean replicationGetAnonymousRoute;
	
	
	protected HyCubeResourceAccessController resourceAccessController;
	
	protected HyCubeResourceReplicationSpreadManager replicationSpreadManager;
	
	
	
	protected boolean ignoreExactPutRequests;
	protected boolean ignoreExactRefreshPutRequests;
	protected boolean ignoreExactGetRequests;
	protected boolean ignoreExactDeleteRequests;
	
	
	
	protected boolean estimateDensityBasedOnLastNodeOnly;
	
	
	
	protected Random rand;
	
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		this.nodeAccessor = nodeAccessor;
		this.properties = properties;
		
		if (! (nodeAccessor.getMessageFactory() instanceof HyCubeMessageFactory)) throw new UnrecoverableRuntimeException("The message factory is expected to be an instance of: " + HyCubeMessageFactory.class.getName() + ".");
		messageFactory = (HyCubeMessageFactory) nodeAccessor.getMessageFactory();
		
		
		String putCallbackEventTypeKey = properties.getProperty(PROP_KEY_PUT_CALLBACK_EVENT_TYPE_KEY);
		if (putCallbackEventTypeKey == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_PUT_CALLBACK_EVENT_TYPE_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_PUT_CALLBACK_EVENT_TYPE_KEY));
		
		String refreshPutCallbackEventTypeKey = properties.getProperty(PROP_KEY_REFRESH_PUT_CALLBACK_EVENT_TYPE_KEY);
		if (refreshPutCallbackEventTypeKey == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_REFRESH_PUT_CALLBACK_EVENT_TYPE_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_REFRESH_PUT_CALLBACK_EVENT_TYPE_KEY));
		
		String getCallbackEventTypeKey = properties.getProperty(PROP_KEY_GET_CALLBACK_EVENT_TYPE_KEY);
		if (getCallbackEventTypeKey == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_GET_CALLBACK_EVENT_TYPE_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_GET_CALLBACK_EVENT_TYPE_KEY));
		
		String deleteCallbackEventTypeKey = properties.getProperty(PROP_KEY_DELETE_CALLBACK_EVENT_TYPE_KEY);
		if (deleteCallbackEventTypeKey == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_DELETE_CALLBACK_EVENT_TYPE_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_DELETE_CALLBACK_EVENT_TYPE_KEY));
		
		
		
		putCallbackEventType = new EventType(EventCategory.extEvent, putCallbackEventTypeKey);
		refreshPutCallbackEventType = new EventType(EventCategory.extEvent, refreshPutCallbackEventTypeKey);
		getCallbackEventType = new EventType(EventCategory.extEvent, getCallbackEventTypeKey);
		deleteCallbackEventType = new EventType(EventCategory.extEvent, deleteCallbackEventTypeKey);		

		
		
		String putRequestTimeoutEventTypeKey = properties.getProperty(PROP_KEY_PUT_REQUEST_TIMEOUT_EVENT_TYPE_KEY);
		if (putRequestTimeoutEventTypeKey == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_PUT_REQUEST_TIMEOUT_EVENT_TYPE_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_PUT_REQUEST_TIMEOUT_EVENT_TYPE_KEY));

		String refreshPutRequestTimeoutEventTypeKey = properties.getProperty(PROP_KEY_REFRESH_PUT_REQUEST_TIMEOUT_EVENT_TYPE_KEY);
		if (refreshPutRequestTimeoutEventTypeKey == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_REFRESH_PUT_REQUEST_TIMEOUT_EVENT_TYPE_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_REFRESH_PUT_REQUEST_TIMEOUT_EVENT_TYPE_KEY));

		String getRequestTimeoutEventTypeKey = properties.getProperty(PROP_KEY_GET_REQUEST_TIMEOUT_EVENT_TYPE_KEY);
		if (getRequestTimeoutEventTypeKey == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_GET_REQUEST_TIMEOUT_EVENT_TYPE_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_GET_REQUEST_TIMEOUT_EVENT_TYPE_KEY));
		
		String deleteRequestTimeoutEventTypeKey = properties.getProperty(PROP_KEY_DELETE_REQUEST_TIMEOUT_EVENT_TYPE_KEY);
		if (deleteRequestTimeoutEventTypeKey == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_DELETE_REQUEST_TIMEOUT_EVENT_TYPE_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_DELETE_REQUEST_TIMEOUT_EVENT_TYPE_KEY));
		
		
		
		putRequestTimeoutEventType = new EventType(EventCategory.extEvent, putRequestTimeoutEventTypeKey);
		refreshPutRequestTimeoutEventType = new EventType(EventCategory.extEvent, refreshPutRequestTimeoutEventTypeKey);
		getRequestTimeoutEventType = new EventType(EventCategory.extEvent, getRequestTimeoutEventTypeKey);
		deleteRequestTimeoutEventType = new EventType(EventCategory.extEvent, deleteRequestTimeoutEventTypeKey);

		
		
		try {
			
			putRequestTimeout = (Integer) properties.getProperty(PROP_KEY_PUT_REQUEST_TIMEOUT, MappedType.INT);
			refreshPutRequestTimeout = (Integer) properties.getProperty(PROP_KEY_REFRESH_PUT_REQUEST_TIMEOUT, MappedType.INT);
			getRequestTimeout = (Integer) properties.getProperty(PROP_KEY_GET_REQUEST_TIMEOUT, MappedType.INT);
			deleteRequestTimeout = (Integer) properties.getProperty(PROP_KEY_DELETE_REQUEST_TIMEOUT, MappedType.INT);
			
			
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize the DHT manager instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		
		
		
		
		ongoingPutRequests = new HashMap<Integer, HyCubePutRequestData>(HashMapUtils.getHashMapCapacityForElementsNum(GlobalConstants.INITIAL_DHT_REQUESTS_DATA_COLLECTION_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		ongoingRefreshPutRequests = new HashMap<Integer, HyCubeRefreshPutRequestData>(HashMapUtils.getHashMapCapacityForElementsNum(GlobalConstants.INITIAL_DHT_REQUESTS_DATA_COLLECTION_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		ongoingGetRequests = new HashMap<Integer, HyCubeGetRequestData>(HashMapUtils.getHashMapCapacityForElementsNum(GlobalConstants.INITIAL_DHT_REQUESTS_DATA_COLLECTION_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		ongoingDeleteRequests = new HashMap<Integer, HyCubeDeleteRequestData>(HashMapUtils.getHashMapCapacityForElementsNum(GlobalConstants.INITIAL_DHT_REQUESTS_DATA_COLLECTION_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		
		
		nextPutCommandId = Integer.MIN_VALUE;
		nextRefreshPutCommandId = Integer.MIN_VALUE;
		nextGetCommandId = Integer.MIN_VALUE;
		nextDeleteCommandId = Integer.MIN_VALUE;
		
		
		dhtManagerLock = new Object();
		
		
		try {
			resourceStoreTime = (Integer) properties.getProperty(PROP_KEY_RESOURCE_STORE_TIME, MappedType.INT);
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize the DHT manager instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		
		
		try {	
			String dhtStorageManagerKey = properties.getProperty(PROP_KEY_DHT_STORAGE_MANAGER);
			if (dhtStorageManagerKey == null || dhtStorageManagerKey.isEmpty()) {
				throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_DHT_STORAGE_MANAGER), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_DHT_STORAGE_MANAGER));
			}
			
			NodeProperties dhtStorageManagerProperties = properties.getNestedProperty(PROP_KEY_DHT_STORAGE_MANAGER, dhtStorageManagerKey);
			String dhtStorageManagerClass = dhtStorageManagerProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
			
			dhtStorageManager = (HyCubeDHTStorageManager) ClassInstanceLoader.newInstance(dhtStorageManagerClass, HyCubeDHTStorageManager.class);
			dhtStorageManager.initialize(nodeAccessor, dhtStorageManagerProperties);
			
		} catch (ClassInstanceLoadException e) {
			throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create DHT storage manager instance. The DHT storage manager is expected to be an instance of: " + HyCubeDHTStorageManager.class.getName(), e);
		}
	
				
		
		if (!(nodeAccessor.getRoutingTable() instanceof HyCubeRoutingTable)) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize the DHT manager instance. The routing table is expected to be an instance of: " + HyCubeRoutingTable.class.getName());
		}
		routingTable = (HyCubeRoutingTable) nodeAccessor.getRoutingTable();
		
		
		
		try {
			
			checkIfResourceReplicaBeforeStoring = (Boolean) properties.getProperty(PROP_KEY_CHECK_IF_REPLICA_BEFORE_STORING, MappedType.BOOLEAN);
			replicationNodesNum = (Integer) properties.getProperty(PROP_KEY_REPLICATION_NODES_NUM, MappedType.INT);
			resourceStoreNodesNum = (Integer) properties.getProperty(PROP_KEY_RESOURCE_STORE_NODES_NUM, MappedType.INT);
			

			replicate = (Boolean) properties.getProperty(PROP_KEY_REPLICATE, MappedType.BOOLEAN);
			
			anonymousReplicate = (Boolean) properties.getProperty(PROP_KEY_ANONYMOUS_REPLICATE, MappedType.BOOLEAN);
			
			maxReplicationNSNodesNum = (Integer) properties.getProperty(PROP_KEY_MAX_REPLICATION_NS_NODES_NUM, MappedType.INT);
			
			
			maxReplicationSpreadNodesNum = (Integer) properties.getProperty(PROP_KEY_MAX_REPLICATION_SPREAD_NODES_NUM, MappedType.INT);
			
			
			assumeNsOrdered = (Boolean) properties.getProperty(PROP_KEY_ASSUME_NS_ORDERED, MappedType.BOOLEAN);
			
			densityCalculationQuantileFuncThreshold = (Double) properties.getProperty(PROP_KEY_DENSITY_CALCULATION_QUANTILE_FUNC_THRESHOLD, MappedType.DOUBLE);
			if (densityCalculationQuantileFuncThreshold < 0 || densityCalculationQuantileFuncThreshold > 1) {
				throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_DENSITY_CALCULATION_QUANTILE_FUNC_THRESHOLD), "Unable to initialize the DHT manager instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_DENSITY_CALCULATION_QUANTILE_FUNC_THRESHOLD));
			}
			
			metric = (Metric) properties.getEnumProperty(PROP_KEY_METRIC, Metric.class);
			
			estDistCoefficient = (Double) properties.getProperty(PROP_KEY_ESTIMATED_DISTANCE_COEFFICIENT, MappedType.DOUBLE);
			
			
			
			putResponseAnonymousRoute = (Boolean) properties.getProperty(PROP_KEY_PUT_RESPONSE_ANONYMOUS_ROUTE, MappedType.BOOLEAN);
			refreshPutResponseAnonymousRoute = (Boolean) properties.getProperty(PROP_KEY_REFRESH_PUT_RESPONSE_ANONYMOUS_ROUTE, MappedType.BOOLEAN);
			getResponseAnonymousRoute = (Boolean) properties.getProperty(PROP_KEY_GET_RESPONSE_ANONYMOUS_ROUTE, MappedType.BOOLEAN);
			deleteResponseAnonymousRoute = (Boolean) properties.getProperty(PROP_KEY_DELETE_RESPONSE_ANONYMOUS_ROUTE, MappedType.BOOLEAN);
			
			
			
			replicationGetRegisterRoute = (Boolean) properties.getProperty(PROP_KEY_REPLICATION_GET_REGISTER_ROUTE, MappedType.BOOLEAN);
			
			replicationGetAnonymousRoute = (Boolean) properties.getProperty(PROP_KEY_REPLICATION_GET_ANONYMOUS_ROUTE, MappedType.BOOLEAN);
			
			
			
			
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize the DHT manager instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		
		
		
		
		try {	
			String resourceAccessControllerKey = properties.getProperty(PROP_KEY_RESOURCE_ACCESS_CONTROLLER);
			if (resourceAccessControllerKey == null || resourceAccessControllerKey.isEmpty()) {
				throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_RESOURCE_ACCESS_CONTROLLER), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_RESOURCE_ACCESS_CONTROLLER));
			}
			
			NodeProperties resourceAccessControllerProperties = properties.getNestedProperty(PROP_KEY_RESOURCE_ACCESS_CONTROLLER, resourceAccessControllerKey);
			String resourceAccessControllerClass = resourceAccessControllerProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
			
			resourceAccessController = (HyCubeResourceAccessController) ClassInstanceLoader.newInstance(resourceAccessControllerClass, HyCubeResourceAccessController.class);
			resourceAccessController.initialize(nodeAccessor, resourceAccessControllerProperties);
			
		} catch (ClassInstanceLoadException e) {
			throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create DHT storage manager instance. The DHT storage manager is expected to be an instance of: " + HyCubeDHTStorageManager.class.getName(), e);
		}
		
		
		
		try {	
			String replicationSpreadManagerKey = properties.getProperty(PROP_KEY_RESOURCE_REPLICATION_SPREAD_MANAGER);
			if (replicationSpreadManagerKey == null || replicationSpreadManagerKey.isEmpty()) {
				throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_RESOURCE_REPLICATION_SPREAD_MANAGER), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_RESOURCE_REPLICATION_SPREAD_MANAGER));
			}
			
			NodeProperties replicationSpreadManagerProperties = properties.getNestedProperty(PROP_KEY_RESOURCE_REPLICATION_SPREAD_MANAGER, replicationSpreadManagerKey);
			String replicationSpreadManagerClass = replicationSpreadManagerProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
			
			replicationSpreadManager = (HyCubeResourceReplicationSpreadManager) ClassInstanceLoader.newInstance(replicationSpreadManagerClass, HyCubeResourceReplicationSpreadManager.class);
			replicationSpreadManager.initialize(nodeAccessor, replicationSpreadManagerProperties);
			
		} catch (ClassInstanceLoadException e) {
			throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create DHT storage manager instance. The DHT storage manager is expected to be an instance of: " + HyCubeDHTStorageManager.class.getName(), e);
		}
		
		
		
		try {
		
			ignoreExactPutRequests = (Boolean) properties.getProperty(PROP_KEY_IGNORE_EXACT_PUT_REQUESTS, MappedType.BOOLEAN);
			ignoreExactRefreshPutRequests = (Boolean) properties.getProperty(PROP_KEY_IGNORE_EXACT_PUT_REQUESTS, MappedType.BOOLEAN);
			ignoreExactGetRequests = (Boolean) properties.getProperty(PROP_KEY_IGNORE_EXACT_PUT_REQUESTS, MappedType.BOOLEAN);
			ignoreExactDeleteRequests = (Boolean) properties.getProperty(PROP_KEY_IGNORE_EXACT_PUT_REQUESTS, MappedType.BOOLEAN);
		
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize the DHT manager instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		
		
		
		try {
			estimateDensityBasedOnLastNodeOnly = (Boolean) properties.getProperty(PROP_KEY_ESTIMATE_DENSITY_BASED_ON_LAST_NODE_ONLY, MappedType.BOOLEAN);
		
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize the DHT manager instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		
		
		this.rand = new Random();
		
		
		
	}

	
	
	
	
	protected int getNextPutCommandId() {
		synchronized (dhtManagerLock) {
			int commandId = nextPutCommandId;
			nextPutCommandId++;
			return commandId;
		}
	}
	
	protected int getNextRefreshPutCommandId() {
		synchronized (dhtManagerLock) {
			int commandId = nextRefreshPutCommandId;
			nextRefreshPutCommandId++;
			return commandId;
		}
	}
	
	protected int getNextGetCommandId() {
		synchronized (dhtManagerLock) {
			int commandId = nextGetCommandId;
			nextGetCommandId++;
			return commandId;
		}
	}
	
	protected int getNextDeleteCommandId() {
		synchronized (dhtManagerLock) {
			int commandId = nextDeleteCommandId;
			nextDeleteCommandId++;
			return commandId;
		}
	}
	
	


	
	@Override
	public HyCubeResourceAccessController getResourceAccessController() {
		return resourceAccessController;
	}
	
	
	
	
	
	@Override
	public Object putToStorage(BigInteger key, NodeId senderNodeId, Object value) {
		return putToStorage(key, senderNodeId, value, null);
	}
	
	@Override
	public Object putToStorage(BigInteger key, NodeId senderNodeId, Object value, Object[] parameters) {
		
		if ( ! (value instanceof HyCubeResource)) throw new IllegalArgumentException("The value is expected to be an instance of: " + HyCubeResource.class.getName());
		long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
		return putToStorage(key, senderNodeId, (HyCubeResource)value, currTime, parameters);

	}
	
	@Override
	public boolean putToStorage(BigInteger key, NodeId senderNodeId, HyCubeResource r, long refreshTime) {
		return putToStorage(key, senderNodeId, r, refreshTime, false, null);
	}
	
	@Override
	public boolean putToStorage(BigInteger key, NodeId senderNodeId, HyCubeResource r, long refreshTime, boolean replication) {
		return putToStorage(key, senderNodeId, r, refreshTime, null);
	}
	
	@Override
	public boolean putToStorage(BigInteger key, NodeId senderNodeId, HyCubeResource r, long refreshTime, Object[] parameters) {
		return putToStorage(key, senderNodeId, r, refreshTime, false, parameters);
	}
	
	@Override
	public boolean putToStorage(BigInteger key, NodeId senderNodeId, HyCubeResource r, long refreshTime, boolean replication, Object[] parameters) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Putting to storage...");
		}
		
		boolean result = dhtStorageManager.putToStorage(key, senderNodeId, r, refreshTime, replication, parameters); 
		
		replicationSpreadManager.putToStorageProcessed(key, senderNodeId, r, refreshTime, replication, parameters, result);
		
		return result;

		
	}

	
	
	@Override
	public Object refreshPutToStorage(BigInteger key, NodeId senderNodeId, Object value) {
		return refreshPutToStorage(key, senderNodeId, value, null);
	}
	
	@Override
	public Object refreshPutToStorage(BigInteger key, NodeId senderNodeId, Object value, Object[] parameters) {
		if ( ! (value instanceof HyCubeResourceDescriptor)) throw new IllegalArgumentException("The value is expected to be an instance of: " + HyCubeResourceDescriptor.class.getName());
		long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
		return refreshPutToStorage(key, senderNodeId, (HyCubeResourceDescriptor)value, currTime, parameters);

	}
	
	@Override
	public boolean refreshPutToStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor rd, long refreshTime) {
		return refreshPutToStorage(key, senderNodeId, rd, refreshTime, false, null);
	}
	
	@Override
	public boolean refreshPutToStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor rd, long refreshTime, boolean replication) {
		return refreshPutToStorage(key, senderNodeId, rd, refreshTime, replication, null);
	}
	
	@Override
	public boolean refreshPutToStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor rd, long refreshTime, Object[] parameters) {
		return refreshPutToStorage(key, senderNodeId, rd, refreshTime, false, parameters);
	}
	
	@Override
	public boolean refreshPutToStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor rd, long refreshTime, boolean replication, Object[] parameters) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Refreshing put to storage...");
		}
		
		boolean result = dhtStorageManager.refreshPutToStorage(key, senderNodeId, rd, refreshTime, replication, parameters);
		
		replicationSpreadManager.refreshPutToStorageProcessed(key, senderNodeId, rd, refreshTime, replication, parameters, result);
		
		return result;
		

	}
	
	
	
	@Override
	public Object[] getFromStorage(BigInteger key, NodeId senderNodeId, Object detail) {
		return getFromStorage(key, senderNodeId, detail, null);
	}
	
	@Override
	public Object[] getFromStorage(BigInteger key, NodeId senderNodeId, Object detail, Object[] parameters) {
		if ( ! (detail instanceof HyCubeResourceDescriptor)) throw new IllegalArgumentException("The detail is expected to be an instance of: " + HyCubeResourceDescriptor.class.getName());
		return getFromStorage(key, senderNodeId, (HyCubeResourceDescriptor)detail, parameters);
		
	}
	
	@Override
	public HyCubeResourceEntry[] getFromStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor criteria) {
		return getFromStorage(key, senderNodeId, criteria, null);
	}
	
	@Override
	public HyCubeResourceEntry[] getFromStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor criteria, Object[] parameters) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Getting from storage...");
		}
		
		HyCubeResourceEntry[] result = dhtStorageManager.getFromStorage(key, senderNodeId, criteria, parameters);
		
		replicationSpreadManager.getFromStorageProcessed(key, senderNodeId, criteria, parameters, result);
		
		return result;
		
	}

	
	
	@Override
	public Object deleteFromStorage(BigInteger key, NodeId sender, Object detail) {
		return deleteFromStorage(key, sender, detail, null);
	}
	
	@Override
	public Object deleteFromStorage(BigInteger key, NodeId sender, Object detail, Object[] parameters) {
		if ( ! (detail instanceof HyCubeResourceDescriptor)) throw new IllegalArgumentException("The detail is expected to be an instance of: " + HyCubeResourceDescriptor.class.getName());
		return deleteFromStorage(key, sender, (HyCubeResourceDescriptor)detail, parameters);
	}
	
	@Override
	public boolean deleteFromStorage(BigInteger key, NodeId sender, HyCubeResourceDescriptor criteria) {
		return deleteFromStorage(key, sender, criteria, null);
	}
	
	@Override
	public boolean deleteFromStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor criteria, Object[] parameters) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Deleting from storage...");
		}
		
		boolean result = dhtStorageManager.deleteFromStorage(key, senderNodeId, criteria, parameters);
		
		replicationSpreadManager.deleteFromProcessed(key, senderNodeId, criteria, parameters, result);
		
		return result;
		
		
	}
	
	

	
	
	
	@Override
	public EventType getPutCallbackEventType() {
		return getCallbackEventType;

	}
	
	@Override
	public EventType getRefreshPutCallbackEventType() {
		return refreshPutCallbackEventType;
		
	}

	@Override
	public EventType getGetCallbackEventType() {
		return putCallbackEventType;

	}

	@Override
	public EventType getDeleteCallbackEventType() {
		return deleteCallbackEventType;

	}
	


	
	
	
	
	public EventType getPutRequestTimeoutEventType() {
		return getRequestTimeoutEventType;

	}
	
	public EventType getRefreshPutRequestTimeoutEventType() {
		return refreshPutRequestTimeoutEventType;
		
	}

	public EventType getGetRequestTimeoutEventType() {
		return putRequestTimeoutEventType;

	}

	public EventType getDeleteRequestTimeoutEventType() {
		return deleteRequestTimeoutEventType;

	}
	
	
	
	
	
	
	
	@Override
	public HyCubeDHTManagerEntryPoint getEntryPoint() {
		return (HyCubeDHTManagerEntryPoint)this;
		
	}
	
	
	@Override
	public Object call() {
		throw new UnsupportedOperationException("The entry point method is not implemented.");
	}
	
	
	@Override
	public Object call(Object arg) {
		throw new UnsupportedOperationException("The entry point method is not implemented.");
		
	}
	

	@Override
	public Object call(Object[] args) {
		throw new UnsupportedOperationException("The entry point method is not implemented.");
		
	}


	@Override
	public Object call(Object entryPoint, Object[] args) {
		throw new UnsupportedOperationException("The entry point method is not implemented.");
		
	}

	
	public Object callExtension(HyCubeDHTManagerEntryPointType entryPoint, Object[] args) {
		throw new UnsupportedOperationException("The entry point method is not implemented.");		
		
	}
	
	
	
	

	
	
	
	
	
	
	
	public void processPutRequest(NodePointer sender, HyCubeMessage msg, int commandId, BigInteger key, String resourceDescriptorString, byte[] resourceData, long refreshTime) throws ProcessMessageException {

		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Processing put request... " + commandId);
		}
		
		
		boolean sendResponse = true;
		if (msg.isAnonymousRoute() && (!msg.isRegisterRoute())) sendResponse = false;
		boolean anonymousResponse = putResponseAnonymousRoute || msg.isAnonymousRoute();	//if anonymous, this should be a registered route, otherwise, the response would not be sent at all
		
		
		if (ignoreExactPutRequests && msg.getRecipientId().equals(nodeAccessor.getNodeId())) {
			if (sendResponse) {
				sendPutResponse(commandId, sender, msg.isRegisterRoute(), (msg.isRegisterRoute() ? msg.getRouteId() : 0), anonymousResponse, false);
			}
			return;
		}
		
		
		boolean routed = false;
		if (! msg.getRecipientId().equals(nodeAccessor.getNodeId())) {
			MessageSendProcessInfo mspi = new MessageSendProcessInfo(msg, new Object[0]);
			try {
				routed = nodeAccessor.sendMessage(mspi, GlobalConstants.WAIT_ON_BKG_MSG_SEND);
			} catch (NetworkAdapterException e) {
				throw new ProcessMessageException("An exception has been thrown while routing the put message.", e);
			}
		}

		if (! routed) {
			if (isReplica(key, nodeAccessor.getNodeId(), resourceStoreNodesNum)) {
				
				if (devLog.isDebugEnabled()) {
					devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Adding the resource to the storage. " + commandId);			
				}
				
				HyCubeResource r = new HyCubeResource(resourceDescriptorString, resourceData);
				
				boolean status = putToStorage(key, sender.getNodeId(), r, refreshTime);
				
				if (sendResponse) {
					sendPutResponse(commandId, sender, msg.isRegisterRoute(), (msg.isRegisterRoute() ? msg.getRouteId() : 0), anonymousResponse, status);
				}
				
			}
			else {
				
				if (devLog.isDebugEnabled()) {
					devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Cannot route the put request and the closest node is not a replica. Sending a negative put response. " + commandId);
				}
				
				if (sendResponse) {
					sendPutResponse(commandId, sender, msg.isRegisterRoute(), (msg.isRegisterRoute() ? msg.getRouteId() : 0), anonymousResponse, false);
				}
				
			}
			
		}
		
	}
	
	


	public void processPutResponse(NodePointer sender, HyCubeMessage msg, int commandId, boolean putStatus) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Processing put response...");
		}
		
		HyCubePutRequestData rd = null;
		synchronized (ongoingPutRequests) {
			rd = ongoingPutRequests.remove(commandId);
		}
		
		if (rd == null) {
			//the request timed out or was already processed
			return;
		}
		
		if (rd.getPutCallback() != null) {
			//create the event
			Event event = new PutCallbackEvent(this, rd.getPutCallback(), rd.getPutCallbackArg(), commandId, putStatus);
			
			//insert to the appropriate event queue
			try {
				nodeAccessor.getEventQueue(putCallbackEventType).put(event);
			} catch (InterruptedException e) {
				//this should never happen
				throw new UnrecoverableRuntimeException("An exception was thrown while inserting an event to an event queue.");
			}
		}
		
	}
	
	
	protected void putRequestTimedOut(int commandId) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Put request timed out.");
		}
		
		HyCubePutRequestData rd = null;
		synchronized (ongoingPutRequests) {
			rd = ongoingPutRequests.remove(commandId);
		}
		
		if (rd == null) {
			//the request timed out or was already processed
			return;
		}
		
		discardPutRequest(rd);
		
	}
	
	
	protected void discardPutRequest(HyCubePutRequestData rd) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Discarding put request...");
		}
		
		if (rd.getPutCallback() != null) {
			//create the event
			Event event = new PutCallbackEvent(this, rd.getPutCallback(), rd.getPutCallbackArg(), rd.getCommandId(), false);
			
			//insert to the appropriate event queue
			try {
				nodeAccessor.getEventQueue(putCallbackEventType).put(event);
			} catch (InterruptedException e) {
				//this should never happen
				throw new UnrecoverableRuntimeException("An exception was thrown while inserting an event to an event queue.");
			}
		}
		
	}
	
	
	public void processRefreshPutRequest(NodePointer sender, HyCubeMessage msg, int commandId, BigInteger key, String resourceDescriptorString, long refreshTime) throws ProcessMessageException {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Processing refresh put request...");
		}
		
		
		boolean sendResponse = true;
		if (msg.isAnonymousRoute() && (!msg.isRegisterRoute())) sendResponse = false;
		boolean anonymousResponse = refreshPutResponseAnonymousRoute || msg.isAnonymousRoute();	//if anonymous, this should be a registered route, otherwise, the response would not be sent at all

		
		if (ignoreExactRefreshPutRequests && msg.getRecipientId().equals(nodeAccessor.getNodeId())) {
			if (sendResponse) {
				sendRefreshPutResponse(commandId, sender, msg.isRegisterRoute(), (msg.isRegisterRoute() ? msg.getRouteId() : 0), anonymousResponse, false);
			}
			return;
		}
		
		
		boolean routed = false;
		if (! msg.getRecipientId().equals(nodeAccessor.getNodeId())) {
			MessageSendProcessInfo mspi = new MessageSendProcessInfo(msg, new Object[0]);
			try {
				routed = nodeAccessor.sendMessage(mspi, GlobalConstants.WAIT_ON_BKG_MSG_SEND);
			} catch (NetworkAdapterException e) {
				throw new ProcessMessageException("An exception has been thrown while routing the refresh put message.", e);
			}
		}
		
		if (! routed) {
			
			if (isReplica(key, nodeAccessor.getNodeId(), resourceStoreNodesNum)) {
				
				if (devLog.isDebugEnabled()) {
					devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Refreshing the resource in the storage. " + commandId);			
				}
				
				HyCubeResourceDescriptor r = new HyCubeResourceDescriptor(resourceDescriptorString);
				
				boolean status = refreshPutToStorage(key, sender.getNodeId(), r, refreshTime);
				
				if (sendResponse) {
					sendRefreshPutResponse(commandId, sender, msg.isRegisterRoute(), (msg.isRegisterRoute() ? msg.getRouteId() : 0), anonymousResponse, status);
				}
				
			}
			else {
				
				if (devLog.isDebugEnabled()) {
					devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Cannot route the refresh put request and the closest node is not a replica. Sending a negative put response. " + commandId);
				}
				
				if (sendResponse) {
					sendRefreshPutResponse(commandId, sender, msg.isRegisterRoute(), (msg.isRegisterRoute() ? msg.getRouteId() : 0), anonymousResponse, false);
				}
				
			}
			
		}
		
		
		
	}

	
	public void processRefreshPutResponse(NodePointer sender, HyCubeMessage msg, int commandId, boolean refreshStatus) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Processing refresh put response...");
		}
		
		HyCubeRefreshPutRequestData rd = null;
		synchronized (ongoingRefreshPutRequests) {
			rd = ongoingRefreshPutRequests.remove(commandId);
		}
		
		if (rd == null) {
			//the request timed out or was already processed
			return;
		}
		
		if (rd.getRefreshPutCallback() != null) {
			//create the event
			Event event = new RefreshPutCallbackEvent(this, rd.getRefreshPutCallback(), rd.getRefreshPutCallbackArg(), commandId, refreshStatus);
			
			//insert to the appropriate event queue
			try {
				nodeAccessor.getEventQueue(refreshPutCallbackEventType).put(event);
			} catch (InterruptedException e) {
				//this should never happen
				throw new UnrecoverableRuntimeException("An exception was thrown while inserting an event to an event queue.");
			}
		}
		
	}
	
	
	protected void refreshPutRequestTimedOut(int commandId) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Refresh put request timed out.");
		}
		
		HyCubeRefreshPutRequestData rd = null;
		synchronized (ongoingRefreshPutRequests) {
			rd = ongoingRefreshPutRequests.remove(commandId);
		}
		
		if (rd == null) {
			//the request timed out or was already processed
			return;
		}
		
		discardRefreshPutRequest(rd);
		
	}
	
	
	protected void discardRefreshPutRequest(HyCubeRefreshPutRequestData rd) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Discarding refresh put request...");
		}
		
		if (rd.getRefreshPutCallback() != null) {
			//create the event
			Event event = new RefreshPutCallbackEvent(this, rd.getRefreshPutCallback(), rd.getRefreshPutCallbackArg(), rd.getCommandId(), false);
			
			//insert to the appropriate event queue
			try {
				nodeAccessor.getEventQueue(refreshPutCallbackEventType).put(event);
			} catch (InterruptedException e) {
				//this should never happen
				throw new UnrecoverableRuntimeException("An exception was thrown while inserting an event to an event queue.");
			}
		}
		
	}
	
	
	public void processGetRequest(NodePointer sender, HyCubeMessage msg, int commandId, BigInteger key, String criteriaString, boolean getFromClosestNode) throws ProcessMessageException {

		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Processing get request... " + commandId);
		}
		
		
		boolean dropMessage = false;
		if (msg.isAnonymousRoute() && (!msg.isRegisterRoute())) dropMessage = true;

		
		if (dropMessage) {
			
			//the message should be dropped. GET requests expect GET responses, and for anonymous and not registered messages, the original request sender is not known
			
			if (devLog.isDebugEnabled()) {
				devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "The GET response will not be sent - an anonymous and not registered request was received. The original sender is unknown. " + commandId);
			}
			
			return;
			
		}
		
		
		boolean anonymousResponse = getResponseAnonymousRoute || msg.isAnonymousRoute();	//if anonymous, this should be a registered route, otherwise, the response would not be sent at all
		
		
		if (ignoreExactGetRequests && msg.getRecipientId().equals(nodeAccessor.getNodeId())) {
			sendGetResponse(commandId, sender, msg.isRegisterRoute(), (msg.isRegisterRoute() ? msg.getRouteId() : 0), anonymousResponse, new String[0], new byte[0][]);
			return;
		}
		
		
		HyCubeResourceDescriptor criteria = new HyCubeResourceDescriptor(criteriaString);
		
		HyCubeResourceEntry[] res = getFromStorage(key, sender.getNodeId(), criteria);
		

		//if isReplica and contains the resource (only if it is requested to find the first node containing the replica (not the closest one)) or the key=nodeId - message will not be routed)
		if (!(getFromClosestNode) && isReplica(key, nodeAccessor.getNodeId(), resourceStoreNodesNum) && res != null && res.length > 0) {
			
			if (devLog.isDebugEnabled()) {
				devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Resource found. Sending get reponse. " + commandId);
			}
			
			String[] resourceDescriptorStrings = new String[res.length];
			byte[][] resourcesData = new byte[res.length][];
			
			for (int i = 0; i < res.length; i++) {
				resourceDescriptorStrings[i] = res[i].getResource().getResourceDescriptor().getDescriptorString();
				resourcesData[i] = res[i].getResource().getData();
			}
			
			sendGetResponse(commandId, sender, msg.isRegisterRoute(), (msg.isRegisterRoute() ? msg.getRouteId() : 0), anonymousResponse, resourceDescriptorStrings, resourcesData);
			
		}
		else {
			
			if (devLog.isDebugEnabled()) {
				devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Resource not found locally. Routing get request. " + commandId);
			}
			
			
			boolean routed = false;
			if (! msg.getRecipientId().equals(nodeAccessor.getNodeId())) {
				MessageSendProcessInfo mspi = new MessageSendProcessInfo(msg, new Object[0]);
				try {
					routed = nodeAccessor.sendMessage(mspi, GlobalConstants.WAIT_ON_BKG_MSG_SEND);
				} catch (NetworkAdapterException e) {
					throw new ProcessMessageException("An exception has been thrown while routing the get message.", e);
				}
			}
			
			if (! routed) {
				//the message was not routed, returning the result found locally if any (even if the node is not a replica)
				if (devLog.isDebugEnabled()) {
					devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Cannot route the get request. Sending the result that was found. " + commandId);
				}
				
				String[] resourceDescriptorStrings = new String[res.length];
				byte[][] resourcesData = new byte[res.length][];
				
				for (int i = 0; i < res.length; i++) {
					resourceDescriptorStrings[i] = res[i].getResource().getResourceDescriptor().getDescriptorString();
					resourcesData[i] = res[i].getResource().getData();
				}
				
				sendGetResponse(commandId, sender, msg.isRegisterRoute(), (msg.isRegisterRoute() ? msg.getRouteId() : 0), anonymousResponse, resourceDescriptorStrings, resourcesData);
				
			}
			//else do nothing - the message was routed further
			
		}
		
		
	}



	public void processGetResponse(NodePointer sender, HyCubeMessage msg, int commandId, String[] resourceDescriptorStrings, byte[][] resourcesData) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Processing get response...");
		}
		
		HyCubeGetRequestData rd = null;
		synchronized (ongoingGetRequests) {
			rd = ongoingGetRequests.remove(commandId);
		}
		
		if (rd == null) {
			//the request timed out or was already processed
			return;
		}
		
		if (resourceDescriptorStrings == null || resourcesData == null || resourceDescriptorStrings.length != resourcesData.length) {
			throw new UnrecoverableRuntimeException("Invalid resource descriptor/data passed.");
		}
		
		
		if (rd.getGetCallback() != null) {
			HyCubeResource[] getResult = new HyCubeResource[resourceDescriptorStrings.length];
			for (int i = 0; i < getResult.length; i++) {
				getResult[i] = new HyCubeResource(resourceDescriptorStrings[i], resourcesData[i]);
			}
			
			//create the event
			Event event = new GetCallbackEvent(this, rd.getGetCallback(), rd.getGetCallbackArg(), commandId, getResult);
			
			//insert to the appropriate event queue
			try {
				nodeAccessor.getEventQueue(getCallbackEventType).put(event);
			} catch (InterruptedException e) {
				//this should never happen
				throw new UnrecoverableRuntimeException("An exception was thrown while inserting an event to an event queue.");
			}
		}
				
	}
	
	
	protected void getRequestTimedOut(int commandId) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Get request timed out.");
		}
		
		HyCubeGetRequestData rd = null;
		synchronized (ongoingGetRequests) {
			rd = ongoingGetRequests.remove(commandId);
		}
		
		if (rd == null) {
			//the request timed out or was already processed
			return;
		}
		
		discardGetRequest(rd);
		
	}
	
	
	protected void discardGetRequest(HyCubeGetRequestData rd) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Discarding get request...");
		}
		
		if (rd.getGetCallback() != null) {
			HyCubeResource[] getResult = new HyCubeResource[0];
			
			//create the event
			Event event = new GetCallbackEvent(this, rd.getGetCallback(), rd.getGetCallbackArg(), rd.getCommandId(), getResult);
			
			//insert to the appropriate event queue
			try {
				nodeAccessor.getEventQueue(getCallbackEventType).put(event);
			} catch (InterruptedException e) {
				//this should never happen
				throw new UnrecoverableRuntimeException("An exception was thrown while inserting an event to an event queue.");
			}
		}
	}
	
	
	
	public void processDeleteRequest(NodePointer sender, HyCubeMessage msg, int commandId, BigInteger key, String resourceDescriptorString) throws ProcessMessageException {

		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Processing delete request...");
		}
		
		
		boolean sendResponse = true;
		if (msg.isAnonymousRoute() && (!msg.isRegisterRoute())) sendResponse = false;
		boolean anonymousResponse = deleteResponseAnonymousRoute || msg.isAnonymousRoute();	//if anonymous, this should be a registered route, otherwise, the response would not be sent at all
		
		
		if (ignoreExactDeleteRequests && msg.getRecipientId().equals(nodeAccessor.getNodeId())) {
			if (sendResponse) {
				sendDeleteResponse(commandId, sender, msg.isRegisterRoute(), (msg.isRegisterRoute() ? msg.getRouteId() : 0), anonymousResponse, false);
			}
			return;
		}
		
		
		boolean routed = false;
		if (! msg.getRecipientId().equals(nodeAccessor.getNodeId())) {
			MessageSendProcessInfo mspi = new MessageSendProcessInfo(msg);
			try {
				routed = nodeAccessor.sendMessage(mspi, GlobalConstants.WAIT_ON_BKG_MSG_SEND);
			} catch (NetworkAdapterException e) {
				throw new ProcessMessageException("An exception has been thrown while routing the delete message.", e);
			}
		}

		if (! routed) {
			
			HyCubeResourceDescriptor rd = new HyCubeResourceDescriptor(resourceDescriptorString);
			
			boolean status = deleteFromStorage(key, sender.getNodeId(), rd);
			
			if (sendResponse) {
				sendDeleteResponse(commandId, sender, msg.isRegisterRoute(), (msg.isRegisterRoute() ? msg.getRouteId() : 0), anonymousResponse, status);
			}
			
		}
		
		
		
		
	}

	public void processDeleteResponse(NodePointer sender, HyCubeMessage msg, int commandId, boolean deleteStatus) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Processing delete response...");
		}
		
		HyCubeDeleteRequestData rd = null;
		synchronized (ongoingDeleteRequests) {
			rd = ongoingDeleteRequests.remove(commandId);
		}
		
		if (rd == null) {
			//the request timed out or was already processed
			return;
		}
		
		if (rd.getDeleteCallback() != null) {
			//create the event
			Event event = new DeleteCallbackEvent(this, rd.getDeleteCallback(), rd.getDeleteCallbackArg(), commandId, deleteStatus);
			
			//insert to the appropriate event queue
			try {
				nodeAccessor.getEventQueue(deleteCallbackEventType).put(event);
			} catch (InterruptedException e) {
				//this should never happen
				throw new UnrecoverableRuntimeException("An exception was thrown while inserting an event to an event queue.");
			}
		}

	}
	
	
	protected void deleteRequestTimedOut(int commandId) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Delete request timed out.");
		}
		
		HyCubeDeleteRequestData rd = null;
		synchronized (ongoingDeleteRequests) {
			rd = ongoingDeleteRequests.remove(commandId);
		}
		
		if (rd == null) {
			//the request timed out or was already processed
			return;
		}
		
		discardDeleteRequest(rd);
		
	}
	
	
	protected void discardDeleteRequest(HyCubeDeleteRequestData rd) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Discarding delete request...");
		}
		
		if (rd.getDeleteCallback() != null) {
			//create the event
			Event event = new DeleteCallbackEvent(this, rd.getDeleteCallback(), rd.getDeleteCallbackArg(), rd.getCommandId(), false);
			
			//insert to the appropriate event queue
			try {
				nodeAccessor.getEventQueue(deleteCallbackEventType).put(event);
			} catch (InterruptedException e) {
				//this should never happen
				throw new UnrecoverableRuntimeException("An exception was thrown while inserting an event to an event queue.");
			}
		}
		
	}
	

	


	
	
	
	

	protected void sendPutResponse(int commandId, NodePointer recipient, boolean registeredRoute, int routeId, boolean anonymousRoute, boolean status) {

		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Sending put response...");
		}
		
		//prepare the message:
		
		int messageSerialNo = nodeAccessor.getNextMessageSerialNo();
		byte[] putReplyMessageData = (new HyCubePutReplyMessageData(commandId, status)).getBytes(); 
		Message putReplyMessage = messageFactory.newMessage(messageSerialNo, nodeAccessor.getNodeId(), recipient.getNodeId(), nodeAccessor.getNetworkAdapter().getPublicAddressBytes(), false, registeredRoute, routeId, anonymousRoute, HyCubeMessageType.PUT_REPLY, nodeAccessor.getNodeParameterSet().getMessageTTL(), (short)0, false, false, (short)0, (short)0, putReplyMessageData); 

		
		//send the message directly to the recipient:
		try {
			nodeAccessor.sendMessage(new MessageSendProcessInfo(putReplyMessage, recipient.getNetworkNodePointer(), false), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
		} catch (NetworkAdapterException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a put response to a node.", e);
		} catch (ProcessMessageException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a put response to a node.", e);
		}

		
		
	}
	
	
	protected void sendRefreshPutResponse(int commandId, NodePointer recipient, boolean registeredRoute, int routeId, boolean anonymousRoute, boolean status) {

		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Sending refresh put response...");
		}
		
		//prepare the message:
		
		int messageSerialNo = nodeAccessor.getNextMessageSerialNo();
		byte[] refreshPutReplyMessageData = (new HyCubeRefreshPutReplyMessageData(commandId, status)).getBytes(); 
		Message refreshPutReplyMessage = messageFactory.newMessage(messageSerialNo, nodeAccessor.getNodeId(), recipient.getNodeId(), nodeAccessor.getNetworkAdapter().getPublicAddressBytes(), false, registeredRoute, routeId, anonymousRoute, HyCubeMessageType.REFRESH_PUT_REPLY, nodeAccessor.getNodeParameterSet().getMessageTTL(), (short)0, false, false, (short)0, (short)0, refreshPutReplyMessageData); 

		
		//send the message directly to the recipient:
		try {
			nodeAccessor.sendMessage(new MessageSendProcessInfo(refreshPutReplyMessage, recipient.getNetworkNodePointer(), false), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
		} catch (NetworkAdapterException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a refresh put response to a node.", e);
		} catch (ProcessMessageException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a refresh put response to a node.", e);
		}

		
		
	}
	
	
	private void sendGetResponse(int commandId, NodePointer recipient, boolean registeredRoute, int routeId, boolean anonymousRoute, String[] resourceDescriptorStrings, byte[][] resourcesData) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Sending get response...");
		}
		
		//prepare the message:
		
		int messageSerialNo = nodeAccessor.getNextMessageSerialNo();
		byte[] getReplyMessageData = (new HyCubeGetReplyMessageData(commandId, resourceDescriptorStrings, resourcesData)).getBytes(); 
		Message getReplyMessage = messageFactory.newMessage(messageSerialNo, nodeAccessor.getNodeId(), recipient.getNodeId(), nodeAccessor.getNetworkAdapter().getPublicAddressBytes(), false, registeredRoute, routeId, anonymousRoute, HyCubeMessageType.GET_REPLY, nodeAccessor.getNodeParameterSet().getMessageTTL(), (short)0, false, false, (short)0, (short)0, getReplyMessageData); 

		
		//send the message directly to the recipient:
		try {
			nodeAccessor.sendMessage(new MessageSendProcessInfo(getReplyMessage, recipient.getNetworkNodePointer(), false), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
		} catch (NetworkAdapterException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a get response to a node.", e);
		} catch (ProcessMessageException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a get response to a node.", e);
		}
		
		
	}
	
	
	protected void sendDeleteResponse(int commandId, NodePointer recipient, boolean registeredRoute, int routeId, boolean anonymousRoute, boolean status) {

		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Sending delete response...");
		}
		
		//prepare the message:
		
		int messageSerialNo = nodeAccessor.getNextMessageSerialNo();
		byte[] deleteReplyMessageData = (new HyCubeDeleteReplyMessageData(commandId, status)).getBytes(); 
		Message deleteReplyMessage = messageFactory.newMessage(messageSerialNo, nodeAccessor.getNodeId(), recipient.getNodeId(), nodeAccessor.getNetworkAdapter().getPublicAddressBytes(), false, registeredRoute, routeId, anonymousRoute, HyCubeMessageType.DELETE_REPLY, nodeAccessor.getNodeParameterSet().getMessageTTL(), (short)0, false, false, (short)0, (short)0, deleteReplyMessageData); 

		
		//send the message directly to the recipient:
		try {
			nodeAccessor.sendMessage(new MessageSendProcessInfo(deleteReplyMessage, recipient.getNetworkNodePointer(), false), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
		} catch (NetworkAdapterException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a delete response to a node.", e);
		} catch (ProcessMessageException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a delete response to a node.", e);
		}

		
		
	}


	

	
	
	@Override
	public PutCallback put(BigInteger key, Object value, PutCallback putCallback, Object putCallbackArg) {
		return put(null, key, value, putCallback, putCallbackArg);
	}
	
	@Override
	public PutCallback put(BigInteger key, Object value, PutCallback putCallback, Object putCallbackArg, Object[] parameters) {
		return put(null, key, value, putCallback, putCallbackArg, parameters);
	}
	
	
	@Override
	public PutCallback put(BigInteger key, HyCubeResource resource, PutCallback putCallback, Object putCallbackArg) {
		return put(null, key, resource, putCallback, putCallbackArg);
	}
	
	@Override
	public PutCallback put(BigInteger key, HyCubeResource resource, PutCallback putCallback, Object putCallbackArg, Object[] parameters) {
		return put(null, key, resource, putCallback, putCallbackArg, parameters);
	}
	
	
	
	
	@Override
	public PutCallback put(NodePointer recipient, BigInteger key, Object value, PutCallback putCallback, Object putCallbackArg) {
		return put(recipient, key, value, putCallback, putCallbackArg, null);
	}
	
	@Override
	public PutCallback put(NodePointer recipient, BigInteger key, Object value, PutCallback putCallback, Object putCallbackArg, Object[] parameters) {
		if ( ! (value instanceof HyCubeResource)) {
			throw new IllegalArgumentException("The value is expected to be an instance of: " + HyCubeResource.class.getName());
		}
		return put(recipient, key, (HyCubeResource) value, putCallback, putCallbackArg, parameters);
	}
	
	
	@Override
	public PutCallback put(NodePointer recipient, BigInteger key, HyCubeResource resource, PutCallback putCallback, Object putCallbackArg) {
		return put(recipient, key, resource, putCallback, putCallbackArg, null);
	}
	
	@Override
	public PutCallback put(NodePointer recipient, BigInteger key, HyCubeResource resource, PutCallback putCallback, Object putCallbackArg, Object[] parameters) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "PUT called...");
		}
		
		if (key == null) {
			throw new IllegalArgumentException("Key must be not null.");
		}
		
		if (resource == null) {
			throw new IllegalArgumentException("Resource must be not null.");
		}
		
		int commandId = getNextPutCommandId();
		
		HyCubePutRequestData rd = new HyCubePutRequestData();
		
		rd.setCommandId(commandId);
		rd.setPutCallback(putCallback);
		rd.setPutCallbackArg(putCallbackArg);
		
		HyCubePutRequestData prev = null;
		synchronized (ongoingPutRequests) {
			prev = ongoingPutRequests.remove(commandId);
			ongoingPutRequests.put(commandId, rd);
		}
		
		if (prev != null) {
			//discard the previous request (practically impossible to happen that the previous request with the same id is still being processed)
			discardPutRequest(prev);
		}
		
		boolean exactPut = false;
		boolean secure = false;
		boolean skipRandomNextHops = false;
		boolean registerRoute = false;
		boolean anonymousRoute = false;
		
		exactPut = getPutParameterExactPut(parameters);				//while sending with this flag, the message recipient will be set to the exact node (not the lookup key), thus the message will not be routed further
		secure = getPutParameterSecureRouting(parameters);
		skipRandomNextHops = getPutParameterSkipRandomNextHops(parameters);
		registerRoute = getPutParameterRegisterRoute(parameters);
		anonymousRoute = getPutParameterAnonymousRoute(parameters);
		
		if (exactPut && recipient == null) {
			throw new IllegalArgumentException("Recipient must be not null.");
		}
		
		
		long refreshTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
		
		boolean status = false;
		if (recipient == null || (! recipient.getNodeId().equals(nodeAccessor.getNodeId())) || !exactPut) {
			//don't send if EXACT recipient is specified and is self
			status = sendPutRequest(commandId, recipient, registerRoute, anonymousRoute, key, resource, exactPut, secure, skipRandomNextHops, refreshTime);
		}
		
		if (status) {
			//set the timeout event for the request:
			ProcessEventProxy processEventProxy = new ProcessEventProxy() {
				@Override
				public void processEvent(Event event) throws EventProcessException {
					if ( ! (event.getEventArg() instanceof Integer)) {
						throw new EventProcessException("The event argument is expecte to be an instance of: " + Integer.class.getName());
					}
					putRequestTimedOut((Integer)event.getEventArg());
				}
			};
	
			Event event = new Event(0, putRequestTimeoutEventType, processEventProxy, commandId);
			Queue<Event> queue = nodeAccessor.getEventQueue(putRequestTimeoutEventType);
			EventScheduler scheduler = nodeAccessor.getEventScheduler(); 
			scheduler.scheduleEventWithDelay(event, queue, putRequestTimeout);
		}
		else {
			//if the message was not sent, check self
			boolean putLocalStatus = false;
			if (isReplica(key, nodeAccessor.getNodeId(), resourceStoreNodesNum)) {
				if (devLog.isDebugEnabled()) {
					devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Adding the resource to the local storage. ");			
				}
				putLocalStatus = putToStorage(key, nodeAccessor.getNodeId(), resource, refreshTime);
			}
			if (putCallback != null) {
				//create the event
				Event event = new PutCallbackEvent(this, putCallback, putCallbackArg, commandId, putLocalStatus);					
				//insert to the appropriate event queue
				try {
					nodeAccessor.getEventQueue(putCallbackEventType).put(event);
				} catch (InterruptedException e) {
					//this should never happen
					throw new UnrecoverableRuntimeException("An exception was thrown while inserting an event to an event queue.");
				}
			}
			
			//remove the request
			synchronized (ongoingPutRequests) {
				ongoingPutRequests.remove(commandId);
			}		
			
		}
		
		return putCallback;
		
	}

	protected boolean sendPutRequest(int commandId, NodePointer recipient, boolean registerRoute, boolean anonymousRoute, BigInteger key, HyCubeResource resource, boolean exactPut, boolean secure, boolean skipRandomNextHops, long refreshTime) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Sending put request...");
		}
		
		NodeId keyNodeId = nodeAccessor.getNodeIdFactory().fromBigInteger(key);
		
		//prepare the message:
		int messageSerialNo = nodeAccessor.getNextMessageSerialNo();
		byte[] putMessageData = (new HyCubePutMessageData(commandId, key, resource.getResourceDescriptor().getDescriptorString(), resource.getData(), refreshTime)).getBytes(); 
		int routeId = 0;
		if (registerRoute) routeId = ((HyCubeRoutingManager)(nodeAccessor.getRoutingManager())).getAndReserveNextRandomUnusedRouteId();
		Message putMessage = messageFactory.newMessage(messageSerialNo, nodeAccessor.getNodeId(), (exactPut ? recipient.getNodeId() : keyNodeId), nodeAccessor.getNetworkAdapter().getPublicAddressBytes(), registerRoute, false, routeId, anonymousRoute, HyCubeMessageType.PUT, nodeAccessor.getNodeParameterSet().getMessageTTL(), (short)0, secure, skipRandomNextHops, (short)0, (short)0, putMessageData); 
		
		
		MessageSendProcessInfo mspi = new MessageSendProcessInfo(putMessage, (recipient != null ? recipient.getNetworkNodePointer() : null), false);
		
		if (recipient != null) {
			//send the message to the recipient:
			try {
				nodeAccessor.sendMessage(mspi, GlobalConstants.WAIT_ON_BKG_MSG_SEND);
			} catch (NetworkAdapterException e) {
				throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a put request to a node.", e);
			} catch (ProcessMessageException e) {
				throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a put request to a node.", e);
			}
			return true;
		}
		else {
			boolean routed;
			try {
				routed = nodeAccessor.sendMessage(mspi, GlobalConstants.WAIT_ON_MSG_SEND_DEFAULT);
			} catch (NetworkAdapterException e) {
				throw new UnrecoverableRuntimeException("An exception has been thrown while trying to route a put request to a node.", e);
			} catch (ProcessMessageException e) {
				throw new UnrecoverableRuntimeException("An exception has been thrown while trying to route a put request to a node.", e);
			}
			return routed;
		}
		
		
		
	}
	
	




		
	@Override
	public RefreshPutCallback refreshPut(BigInteger key, Object value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg) {
		return refreshPut(null, key, value, refreshPutCallback, refreshPutCallbackArg);
	}
	
	@Override
	public RefreshPutCallback refreshPut(BigInteger key, Object value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg, Object[] parameters) {
		return refreshPut(null, key, value, refreshPutCallback, refreshPutCallbackArg, parameters);
	}
	
	public RefreshPutCallback refreshPut(BigInteger key, HyCubeResourceDescriptor resourceDescriptor, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg) {
		return refreshPut(null, key, resourceDescriptor, refreshPutCallback, refreshPutCallbackArg);
	}
	
	@Override
	public RefreshPutCallback refreshPut(BigInteger key, HyCubeResourceDescriptor resourceDescriptor, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg, Object[] parameters) {
		return refreshPut(null, key, resourceDescriptor, refreshPutCallback, refreshPutCallbackArg, parameters);
	}
	
	
	
	@Override
	public RefreshPutCallback refreshPut(NodePointer recipient, BigInteger key, Object value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg) {
		return refreshPut(recipient, key, value, refreshPutCallback, refreshPutCallbackArg, null);
	}
	
	@Override
	public RefreshPutCallback refreshPut(NodePointer recipient, BigInteger key, Object value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg, Object[] parameters) {
		if ( ! (value instanceof HyCubeResourceDescriptor)) {
			throw new IllegalArgumentException("The value is expected to be an instance of: " + HyCubeResourceDescriptor.class.getName());
		}
		return refreshPut(recipient, key, (HyCubeResourceDescriptor) value, refreshPutCallback, refreshPutCallbackArg, parameters);
	}
	
	public RefreshPutCallback refreshPut(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor resourceDescriptor, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg) {
		return refreshPut(recipient, key, resourceDescriptor, refreshPutCallback, refreshPutCallbackArg);
	}
	
	@Override
	public RefreshPutCallback refreshPut(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor resourceDescriptor, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg, Object[] parameters) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "REFRESH PUT called...");
		}
		
		if (key == null) {
			throw new IllegalArgumentException("Key must be not null.");
		}
		
		if (resourceDescriptor == null) {
			throw new IllegalArgumentException("Resource descriptor must be not null.");
		}
		
		int commandId = getNextRefreshPutCommandId();
		
		HyCubeRefreshPutRequestData rd = new HyCubeRefreshPutRequestData();
		
		rd.setCommandId(commandId);
		rd.setRefreshPutCallback(refreshPutCallback);
		rd.setRefreshPutCallbackArg(refreshPutCallbackArg);
		
		HyCubeRefreshPutRequestData prev = null;
		synchronized (ongoingRefreshPutRequests) {
			prev = ongoingRefreshPutRequests.remove(commandId);
			ongoingRefreshPutRequests.put(commandId, rd);
		}
		
		if (prev != null) {
			//discard the previous request (practically impossible to happen that the previous request with the same id is still being processed)
			discardRefreshPutRequest(prev);
		}
		
		boolean exactRefreshPut = false;
		boolean secure = false;
		boolean skipRandomNextHops = false;
		boolean registerRoute = false;
		boolean anonymousRoute = false;
		
		exactRefreshPut = getRefreshPutParameterExactRefreshPut(parameters);				//while sending with this flag, the message recipient will be set to the exact node (not the lookup key), thus the message will not be routed further
		secure = getRefreshPutParameterSecureRouting(parameters);
		skipRandomNextHops = getRefreshPutParameterSkipRandomNextHops(parameters);
		registerRoute = getRefreshPutParameterRegisterRoute(parameters);
		anonymousRoute = getRefreshPutParameterAnonymousRoute(parameters);

		
		if (exactRefreshPut && recipient == null) {
			throw new IllegalArgumentException("Recipient must be not null.");
		}
		
		
		long refreshTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
		
		
		boolean status = false; 
		if (recipient == null || (! recipient.getNodeId().equals(nodeAccessor.getNodeId())) || !exactRefreshPut) {
			//don't send if EXACT recipient is specified and is self
			status = sendRefreshPutRequest(commandId, recipient, registerRoute, anonymousRoute, key, resourceDescriptor, exactRefreshPut, secure, skipRandomNextHops, refreshTime);
		}
		
		
		if (status) {
		
			//set the timeout event for the request:
			ProcessEventProxy processEventProxy = new ProcessEventProxy() {
				@Override
				public void processEvent(Event event) throws EventProcessException {
					if ( ! (event.getEventArg() instanceof Integer)) {
						throw new EventProcessException("The event argument is expecte to be an instance of: " + Integer.class.getName());
					}
					refreshPutRequestTimedOut((Integer)event.getEventArg());
				}
			};
	
			
			Event event = new Event(0, refreshPutRequestTimeoutEventType, processEventProxy, commandId);
			Queue<Event> queue = nodeAccessor.getEventQueue(refreshPutRequestTimeoutEventType);
			EventScheduler scheduler = nodeAccessor.getEventScheduler(); 
			scheduler.scheduleEventWithDelay(event, queue, refreshPutRequestTimeout);
		}
		else {
			//if the message was not sent, check self
			boolean refreshPutLocalStatus = false;
			if (isReplica(key, nodeAccessor.getNodeId(), resourceStoreNodesNum)) {
				if (devLog.isDebugEnabled()) {
					devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Refreshing the resource in the local storage. ");			
				}
				refreshPutLocalStatus = refreshPutToStorage(key, nodeAccessor.getNodeId(), resourceDescriptor, refreshTime);
			}
			if (refreshPutCallback != null) {
				//create the event
				Event event = new RefreshPutCallbackEvent(this, refreshPutCallback, refreshPutCallbackArg, commandId, refreshPutLocalStatus);					
				//insert to the appropriate event queue
				try {
					nodeAccessor.getEventQueue(refreshPutCallbackEventType).put(event);
				} catch (InterruptedException e) {
					//this should never happen
					throw new UnrecoverableRuntimeException("An exception was thrown while inserting an event to an event queue.");
				}
			}			

			//remove the request
			synchronized (ongoingRefreshPutRequests) {
				ongoingRefreshPutRequests.remove(commandId);
			}
			
		}
		
		return refreshPutCallback;
		
	}



	protected boolean sendRefreshPutRequest(int commandId, NodePointer recipient, boolean registerRoute, boolean anonymousRoute, BigInteger key, HyCubeResourceDescriptor resourceDescriptor, boolean exactRefreshPut, boolean secure, boolean skipRandomNextHops, long refreshTime) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Sending refresh put message...");
		}
		
		NodeId keyNodeId = nodeAccessor.getNodeIdFactory().fromBigInteger(key);
		
		//prepare the message:
		int messageSerialNo = nodeAccessor.getNextMessageSerialNo();
		byte[] refreshPutMessageData = (new HyCubeRefreshPutMessageData(commandId, key, resourceDescriptor.getDescriptorString(), refreshTime)).getBytes();
		int routeId = 0;
		if (registerRoute) routeId = ((HyCubeRoutingManager)(nodeAccessor.getRoutingManager())).getAndReserveNextRandomUnusedRouteId();
		Message refreshPutMessage = messageFactory.newMessage(messageSerialNo, nodeAccessor.getNodeId(), (exactRefreshPut ? recipient.getNodeId() : keyNodeId), nodeAccessor.getNetworkAdapter().getPublicAddressBytes(), registerRoute, false, routeId, anonymousRoute, HyCubeMessageType.REFRESH_PUT, nodeAccessor.getNodeParameterSet().getMessageTTL(), (short)0, secure, skipRandomNextHops, (short)0, (short)0, refreshPutMessageData); 
		
		
		MessageSendProcessInfo mspi = new MessageSendProcessInfo(refreshPutMessage, (recipient != null ? recipient.getNetworkNodePointer() : null), false);
		
		if (recipient != null) {
			//send the message to the recipient:
			try {
				nodeAccessor.sendMessage(mspi, GlobalConstants.WAIT_ON_BKG_MSG_SEND);
			} catch (NetworkAdapterException e) {
				throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a refresh put request to a node.", e);
			} catch (ProcessMessageException e) {
				throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a refresh put request to a node.", e);
			}
			return true;
		}
		else {
			boolean routed;
			try {
				routed = nodeAccessor.sendMessage(mspi, GlobalConstants.WAIT_ON_MSG_SEND_DEFAULT);
			} catch (NetworkAdapterException e) {
				throw new UnrecoverableRuntimeException("An exception has been thrown while trying to route a refresh put request to a node.", e);
			} catch (ProcessMessageException e) {
				throw new UnrecoverableRuntimeException("An exception has been thrown while trying to route a refresh put request to a node.", e);
			}
			return routed;
		}
		
		
	}



	
	
	
	
	@Override
	public GetCallback get(BigInteger key, Object detail, GetCallback getCallback, Object getCallbackArg) {
		return get(null, key, detail, getCallback, getCallbackArg);
	}
	
	@Override
	public GetCallback get(BigInteger key, Object detail, GetCallback getCallback, Object getCallbackArg, Object[] parameters) {
		return get(null, key, detail, getCallback, getCallbackArg, parameters);
	}
	
	@Override
	public GetCallback get(BigInteger key, HyCubeResourceDescriptor criteria, GetCallback getCallback, Object getCallbackArg) {
		return get(null, key, criteria, getCallback, getCallbackArg);
	}
	
	@Override
	public GetCallback get(BigInteger key, HyCubeResourceDescriptor criteria, GetCallback getCallback, Object getCallbackArg, Object[] parameters) {
		return get(null, key, criteria, getCallback, getCallbackArg, parameters);
	}
	
	
	
	@Override
	public GetCallback get(NodePointer recipient, BigInteger key, Object detail, GetCallback getCallback, Object getCallbackArg) {
		return get(recipient, key, detail, getCallback, getCallbackArg, null);
	}
	
	@Override
	public GetCallback get(NodePointer recipient, BigInteger key, Object detail, GetCallback getCallback, Object getCallbackArg, Object[] parameters) {
		if ( ! (detail instanceof HyCubeResourceDescriptor)) {
			throw new IllegalArgumentException("The detail is expected to be an instance of: " + HyCubeResourceDescriptor.class.getName());
		}
		return get(recipient, key, (HyCubeResourceDescriptor) detail, getCallback, getCallbackArg, parameters);
	}
	
	@Override
	public GetCallback get(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor criteria, GetCallback getCallback, Object getCallbackArg) {
		return get(recipient, key, criteria, getCallback, getCallbackArg, null);
	}
	
	@Override
	public GetCallback get(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor criteria, GetCallback getCallback, Object getCallbackArg, Object[] parameters) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "GET called...");
		}
		
		if (key == null) {
			throw new IllegalArgumentException("Key must be not null.");
		}
		
		if (criteria == null) {
			throw new IllegalArgumentException("Criteria must be not null.");
		}
		
		int commandId = getNextGetCommandId();
		
		HyCubeGetRequestData rd = new HyCubeGetRequestData();
		
		rd.setCommandId(commandId);
		rd.setGetCallback(getCallback);
		rd.setGetCallbackArg(getCallbackArg);
		
		HyCubeGetRequestData prev = null;
		synchronized (ongoingGetRequests) {
			prev = ongoingGetRequests.remove(commandId);
			ongoingGetRequests.put(commandId, rd);
		}
		
		if (prev != null) {
			//discard the previous request (practically impossible to happen that the previous request with the same id is still being processed)
			discardGetRequest(prev);
		}
		
		boolean exactGet = false;
		boolean findClosestNode = false;
		boolean secure = false;
		boolean skipRandomNextHops = false;
		boolean registerRoute = false;
		boolean anonymousRoute = false;
		
		
		exactGet = getGetParameterExactGet(parameters);		//while sending with this flag, the message recipient will be set to the exact node (not the lookup key), thus the message will not be routed further
		findClosestNode = getGetParameterFindClosestNode(parameters);
		secure = getGetParameterSecureRouting(parameters);
		skipRandomNextHops = getGetParameterSkipRandomNextHops(parameters);
		registerRoute = getGetParameterRegisterRoute(parameters);
		anonymousRoute = getGetParameterAnonymousRoute(parameters);

		
		if (exactGet && recipient == null) {
			throw new IllegalArgumentException("Recipient must be not null.");
		}
		
		if (findClosestNode) {
			//route the message to the closest node possible and get the resource
			boolean status = false;
			if (recipient == null || (! recipient.getNodeId().equals(nodeAccessor.getNodeId())) || !exactGet) {
				//don't send if EXACT recipient is specified and is self
				status = sendGetRequest(commandId, recipient, registerRoute, anonymousRoute, key, criteria, exactGet, findClosestNode, secure, skipRandomNextHops);
			}
			if (status) {
				//the get message was sent -> set the timeout event for the request:
				ProcessEventProxy processEventProxy = new ProcessEventProxy() {
					@Override
					public void processEvent(Event event) throws EventProcessException {
						if ( ! (event.getEventArg() instanceof Integer)) {
							throw new EventProcessException("The event argument is expecte to be an instance of: " + Integer.class.getName());
						}
						getRequestTimedOut((Integer)event.getEventArg());
					}
				};
				Event event = new Event(0, getRequestTimeoutEventType, processEventProxy, commandId);
				Queue<Event> queue = nodeAccessor.getEventQueue(getRequestTimeoutEventType);
				EventScheduler scheduler = nodeAccessor.getEventScheduler(); 
				scheduler.scheduleEventWithDelay(event, queue, getRequestTimeout);
			}
			else {
				//if the message was not sent, check self
				HyCubeResourceEntry[] res = getFromStorage(key, this.nodeAccessor.getNodeId(), criteria);
				if (res != null && res.length > 0) {
					//return the resource found locally
					if (devLog.isDebugEnabled()) {
						devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Resource found locally. Returning. " + commandId);
					}
					if (rd.getGetCallback() != null) {
						HyCubeResource[] getResult = new HyCubeResource[res.length];
						for (int i = 0; i < getResult.length; i++) {
							getResult[i] = res[i].getResource();
						}
						//create the event
						Event event = new GetCallbackEvent(this, rd.getGetCallback(), rd.getGetCallbackArg(), commandId, getResult);
						//insert to the appropriate event queue
						try {
							nodeAccessor.getEventQueue(getCallbackEventType).put(event);
						} catch (InterruptedException e) {
							//this should never happen
							throw new UnrecoverableRuntimeException("An exception was thrown while inserting an event to an event queue.");
						}
					}
				}
				else {
					//the message was not sent and the resource was not found
					if (rd.getGetCallback() != null) {
						//create the event
						Event event = new GetCallbackEvent(this, rd.getGetCallback(), rd.getGetCallbackArg(), commandId, new HyCubeResource[0]);
						//insert to the appropriate event queue
						try {
							nodeAccessor.getEventQueue(getCallbackEventType).put(event);
						} catch (InterruptedException e) {
							//this should never happen
							throw new UnrecoverableRuntimeException("An exception was thrown while inserting an event to an event queue.");
						}
					}
					
				}

				//remove the request
				synchronized (ongoingGetRequests) {
					ongoingGetRequests.remove(commandId);
				}
			}
			
		}
		else {
			//return the resource(s) from the first node that is a replica and contains the resource (only if recipient is not explicitely specified)
			boolean resourceFoundLocally = false;
			if (recipient == null) {
				HyCubeResourceEntry[] res = getFromStorage(key, this.nodeAccessor.getNodeId(), criteria);
				if (isReplica(key, nodeAccessor.getNodeId(), resourceStoreNodesNum) && res != null && res.length > 0) {
					resourceFoundLocally = true;
					//return the resource found locally
					if (devLog.isDebugEnabled()) {
						devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Resource found locally. Returning. " + commandId);
					}
					if (rd.getGetCallback() != null) {
						HyCubeResource[] getResult = new HyCubeResource[res.length];
						for (int i = 0; i < getResult.length; i++) {
							getResult[i] = res[i].getResource();
						}
						//create the event
						Event event = new GetCallbackEvent(this, rd.getGetCallback(), rd.getGetCallbackArg(), commandId, getResult);
						//insert to the appropriate event queue
						try {
							nodeAccessor.getEventQueue(getCallbackEventType).put(event);
						} catch (InterruptedException e) {
							//this should never happen
							throw new UnrecoverableRuntimeException("An exception was thrown while inserting an event to an event queue.");
						}
					}
				}
			}
			if (!resourceFoundLocally) {
				//try to route the message
				if (devLog.isDebugEnabled()) {
					devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Routing get request. " + commandId);
				}
				boolean status = false;
				if (recipient == null || (! recipient.getNodeId().equals(nodeAccessor.getNodeId())) || !exactGet) {
					//don't send if EXACT recipient is specified and is self
					status = sendGetRequest(commandId, recipient, registerRoute, anonymousRoute, key, criteria, exactGet, findClosestNode, secure, skipRandomNextHops);
				}
				if (status) {
					//the get message was sent -> set the timeout event for the request:
					ProcessEventProxy processEventProxy = new ProcessEventProxy() {
						@Override
						public void processEvent(Event event) throws EventProcessException {
							if ( ! (event.getEventArg() instanceof Integer)) {
								throw new EventProcessException("The event argument is expecte to be an instance of: " + Integer.class.getName());
							}
							getRequestTimedOut((Integer)event.getEventArg());
						}
					};
					Event event = new Event(0, getRequestTimeoutEventType, processEventProxy, commandId);
					Queue<Event> queue = nodeAccessor.getEventQueue(getRequestTimeoutEventType);
					EventScheduler scheduler = nodeAccessor.getEventScheduler(); 
					scheduler.scheduleEventWithDelay(event, queue, getRequestTimeout);
				}
				else {
					//the message was not routed, returning the result found locally if any (even if the node is not a replica)
					if (devLog.isDebugEnabled()) {
						devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Cannot route the get request. Returning the result that was found. " + commandId);
					}
					HyCubeResourceEntry[] res = getFromStorage(key, this.nodeAccessor.getNodeId(), criteria);
					if (rd.getGetCallback() != null) {
						HyCubeResource[] getResult = new HyCubeResource[res.length];
						for (int i = 0; i < getResult.length; i++) {
							getResult[i] = res[i].getResource();
						}
						//create the event
						Event event = new GetCallbackEvent(this, rd.getGetCallback(), rd.getGetCallbackArg(), commandId, getResult);
						//insert to the appropriate event queue
						try {
							nodeAccessor.getEventQueue(getCallbackEventType).put(event);
						} catch (InterruptedException e) {
							//this should never happen
							throw new UnrecoverableRuntimeException("An exception was thrown while inserting an event to an event queue.");
						}
					}
				}
				
			}
		}
		
		
		return getCallback;
		
	}




	protected boolean sendGetRequest(int commandId, NodePointer recipient, boolean registerRoute, boolean anonymousRoute, BigInteger key, HyCubeResourceDescriptor criteria, boolean exactGet, boolean getFromClosestNode, boolean secure, boolean skipRandomNextHops) {

		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Sending get request");
		}

		NodeId keyNodeId = nodeAccessor.getNodeIdFactory().fromBigInteger(key);
		
		//prepare the message:
		int messageSerialNo = nodeAccessor.getNextMessageSerialNo();
		byte[] getMessageData = (new HyCubeGetMessageData(commandId, key, criteria.getDescriptorString(), getFromClosestNode)).getBytes();
		int routeId = 0;
		if (registerRoute) routeId = ((HyCubeRoutingManager)(nodeAccessor.getRoutingManager())).getAndReserveNextRandomUnusedRouteId();
		Message getMessage = messageFactory.newMessage(messageSerialNo, nodeAccessor.getNodeId(), (exactGet ? recipient.getNodeId() : keyNodeId), nodeAccessor.getNetworkAdapter().getPublicAddressBytes(), registerRoute, false, routeId, anonymousRoute, HyCubeMessageType.GET, nodeAccessor.getNodeParameterSet().getMessageTTL(), (short)0, secure, skipRandomNextHops, (short)0, (short)0, getMessageData); 
		
		
		MessageSendProcessInfo mspi = new MessageSendProcessInfo(getMessage, (recipient != null ? recipient.getNetworkNodePointer() : null), false);
		
		if (recipient != null) {
			//send the message to the recipient:
			try {
				nodeAccessor.sendMessage(mspi, GlobalConstants.WAIT_ON_BKG_MSG_SEND);
			} catch (NetworkAdapterException e) {
				throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a get request to a node.", e);
			} catch (ProcessMessageException e) {
				throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a get request to a node.", e);
			}
			return true;
		}
		else {
			boolean routed;
			try {
				routed = nodeAccessor.sendMessage(mspi, GlobalConstants.WAIT_ON_MSG_SEND_DEFAULT);
			} catch (NetworkAdapterException e) {
				throw new UnrecoverableRuntimeException("An exception has been thrown while trying to route a get request to a node.", e);
			} catch (ProcessMessageException e) {
				throw new UnrecoverableRuntimeException("An exception has been thrown while trying to route a get request to a node.", e);
			}
			return routed;
		}
		
	}





	@Override
	public DeleteCallback delete(NodePointer recipient, BigInteger key, Object detail, DeleteCallback deleteCallback, Object deleteCallbackArg) {
		return delete(recipient, key, detail, deleteCallback, deleteCallbackArg, null);
	}
		
	@Override
	public DeleteCallback delete(NodePointer recipient, BigInteger key, Object detail, DeleteCallback deleteCallback, Object deleteCallbackArg, Object[] parameters) {
		if ( ! (detail instanceof HyCubeResourceDescriptor)) {
			throw new IllegalArgumentException("The detail is expected to be an instance of: " + HyCubeResourceDescriptor.class.getName());
		}
		return delete(recipient, key, (HyCubeResourceDescriptor) detail, deleteCallback, deleteCallbackArg, parameters);
	}
	
	@Override
	public DeleteCallback delete(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor criteria, DeleteCallback deleteCallback, Object deleteCallbackArg) {
		return delete(recipient, key, criteria, deleteCallback, deleteCallbackArg, null);
	}
	
	@Override
	public DeleteCallback delete(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor criteria, DeleteCallback deleteCallback, Object deleteCallbackArg, Object[] parameters) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "DELETE called...");
		}
		
		if (key == null) {
			throw new IllegalArgumentException("Key must be not null.");
		}
		
		if (criteria == null) {
			throw new IllegalArgumentException("Criteria must be not null.");
		}
		
		int commandId = getNextDeleteCommandId();
		
		HyCubeDeleteRequestData rd = new HyCubeDeleteRequestData();
		
		rd.setCommandId(commandId);
		rd.setDeleteCallback(deleteCallback);
		rd.setDeleteCallbackArg(deleteCallbackArg);
		
		HyCubeDeleteRequestData prev = null;
		synchronized (ongoingDeleteRequests) {
			prev = ongoingDeleteRequests.remove(commandId);
			ongoingDeleteRequests.put(commandId, rd);
		}
		
		if (prev != null) {
			//discard the previous request (practically impossible to happen that the previous request with the same id is still being processed)
			discardDeleteRequest(prev);
		}
		
		boolean exactDelete = false;
		boolean secure = false;
		boolean skipRandomNextHops = false;
		boolean registerRoute = false;
		boolean anonymousRoute = false;
		
		exactDelete = getDeleteParameterExactDelete(parameters);		//while sending with this flag, the message recipient will be set to the exact node (not the lookup key), thus the message will not be routed further
		secure = getDeleteParameterSecureRouting(parameters);
		skipRandomNextHops = getDeleteParameterSkipRandomNextHops(parameters);
		registerRoute = getDeleteParameterRegisterRoute(parameters);
		anonymousRoute = getDeleteParameterAnonymousRoute(parameters);
		
		
		if (exactDelete && recipient == null) {
			throw new IllegalArgumentException("Recipient must be not null.");
		}
		

		sendDeleteRequest(commandId, recipient, registerRoute, anonymousRoute, key, criteria, exactDelete, secure, skipRandomNextHops);
		
		
		//set the timeout event for the request:
		ProcessEventProxy processEventProxy = new ProcessEventProxy() {
			@Override
			public void processEvent(Event event) throws EventProcessException {
				if ( ! (event.getEventArg() instanceof Integer)) {
					throw new EventProcessException("The event argument is expecte to be an instance of: " + Integer.class.getName());
				}
				deleteRequestTimedOut((Integer)event.getEventArg());
			}
		};

		
		Event event = new Event(0, deleteRequestTimeoutEventType, processEventProxy, commandId);
		Queue<Event> queue = nodeAccessor.getEventQueue(deleteRequestTimeoutEventType);
		EventScheduler scheduler = nodeAccessor.getEventScheduler(); 
		scheduler.scheduleEventWithDelay(event, queue, deleteRequestTimeout);
		
		return deleteCallback;
		
	}





	protected void sendDeleteRequest(int commandId, NodePointer recipient, boolean registerRoute, boolean anonymousRoute, BigInteger key, HyCubeResourceDescriptor criteria, boolean exactDelete, boolean secure, boolean skipRandomNextHops) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Sending delete request...");
		}
		
		NodeId keyNodeId = nodeAccessor.getNodeIdFactory().fromBigInteger(key);
		
		//prepare the message:
		int messageSerialNo = nodeAccessor.getNextMessageSerialNo();
		byte[] deleteMessageData = (new HyCubeDeleteMessageData(commandId, key, criteria.getDescriptorString())).getBytes();
		int routeId = 0;
		if (registerRoute) routeId = ((HyCubeRoutingManager)(nodeAccessor.getRoutingManager())).getAndReserveNextRandomUnusedRouteId();
		Message deleteMessage = messageFactory.newMessage(messageSerialNo, nodeAccessor.getNodeId(), (exactDelete ? recipient.getNodeId() : keyNodeId), nodeAccessor.getNetworkAdapter().getPublicAddressBytes(), registerRoute, false, routeId, anonymousRoute, HyCubeMessageType.DELETE, nodeAccessor.getNodeParameterSet().getMessageTTL(), (short)0, secure, skipRandomNextHops, (short)0, (short)0, deleteMessageData); 
		
		//send the message to the recipient:
		try {
			nodeAccessor.sendMessage(new MessageSendProcessInfo(deleteMessage, (recipient != null ? recipient.getNetworkNodePointer() : null), false), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
		} catch (NetworkAdapterException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a delete request to a node.", e);
		} catch (ProcessMessageException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a delete request to a node.", e);
		}
		
	}


	
	public void processDHT() {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Process DHT called...");
		}
		
		long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
		long discardTime = currTime - resourceStoreTime;
		
		discardOutdatedEntries(discardTime);
		
	}
	
	
	
	
	
	
	public void discardOutdatedEntries(long discardTime) {
		dhtStorageManager.discardOutdatedEntries(discardTime);
	}
	
	
	
	
	
	
	public void replicate() {
		
		Map<BigInteger, HyCubeResourceReplicationEntry[]> replicationInfos = dhtStorageManager.getResourcesInfoForReplication();
		
		//send resource descriptors for all stored resources to the nodes in the neighborgood set, respecting the maximum number of the closest nodes defined
		//smaller maximum number of nodes would descrease overall overhead, but may have negative influence on the replication efficiency - some nodes may be skipped with higher probability
		int replicationNSNodesNum = routingTable.getNSSize();
		if (replicationNSNodesNum > maxReplicationNSNodesNum) replicationNSNodesNum = maxReplicationNSNodesNum;  
		
		routingTable.getNsLock().readLock().lock();
		
		ArrayList<NodePointer> replicationNodes = new ArrayList<NodePointer>(maxReplicationNSNodesNum);
		if (assumeNsOrdered) {
			for (int i = 0; i < routingTable.getNeighborhoodSet().size() && i < maxReplicationNSNodesNum; i++) {
				replicationNodes.add(routingTable.getNeighborhoodSet().get(i).getNode());
			}
			
			//release the lock
			routingTable.getNsLock().readLock().unlock();
			
		}
		else {
			int nsSize = routingTable.getNeighborhoodSet().size();
			ArrayList<RoutingTableEntry> nsCopy = new ArrayList<RoutingTableEntry>(routingTable.getNeighborhoodSet());
			
			//release the lock
			routingTable.getNsLock().readLock().unlock();
			
			
			for (int i = 0; i < nsSize && i < maxReplicationNSNodesNum; i++) {
				//find the closest node
				RoutingTableEntry closest = null;
				int closestIndex = 0;
				for (int j = 0; j < nsCopy.size(); j++) {
					if (closest == null || nsCopy.get(j).getDistance() < closest.getDistance()) {
						closest = nsCopy.get(j);
						closestIndex = j;
					}
				}
				
				replicationNodes.add(nsCopy.get(closestIndex).getNode());
				
				//remove
				nsCopy.remove(closestIndex);
				
			}
			
		}
		

		//send the replication info to the replication nodes
		//data is not included in the message - the node would ask for the data if needed -> less overhead, as in most cases the data will already be replicated
		
		for (BigInteger key : replicationInfos.keySet()) {
			
			HyCubeResourceReplicationEntry[] replicationInfo = replicationInfos.get(key);
			
			BigInteger[] keys = new BigInteger[replicationInfo.length];
			String[] resourceDescriptorStrings = new String[replicationInfo.length];
			long[] refreshTimes = new long[replicationInfo.length];
			int[] replicationSpreadNodesNums = new int[replicationInfo.length];
			
			for (int i = 0; i < replicationInfo.length; i++) {
				keys[i] = replicationInfo[i].getKey();
				resourceDescriptorStrings[i] = replicationInfo[i].getResourceDescriptor().getDescriptorString();
				refreshTimes[i] = replicationInfo[i].getRefreshTime();
				replicationSpreadNodesNums[i] = replicationSpreadManager.getReplicationNodesNumForResource(replicationNodesNum, replicationInfo[i].getKey(), replicationInfo[i].getResourceDescriptor(), replicationInfo[i].getRefreshTime());
			}
			
			for (NodePointer recipient : replicationNodes) {
				sendReplicationInfo(recipient, keys, resourceDescriptorStrings, refreshTimes, replicationSpreadNodesNums, replicationNodes);
			}
		
		}
		
		
		
	}
	
	
	
	protected void sendReplicationInfo(NodePointer recipient, BigInteger[] keys, String[] resourceDescriptorStrings, long[] refreshTimes, int[] replicationSpreadNodesNums, ArrayList<NodePointer> replicationNodes) {

		if (!replicate) return;
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Sending replication info...");
		}
		
		//prepare the message:
		
		int messageSerialNo = nodeAccessor.getNextMessageSerialNo();
		byte[] replicateMessageData = (new HyCubeReplicateMessageData(resourceDescriptorStrings.length, keys, resourceDescriptorStrings, refreshTimes, replicationSpreadNodesNums)).getBytes();
		Object[] routingParameters = null;
		if (anonymousReplicate) {
			routingParameters = HyCubeRoutingManager.createRoutingParameters(null, null, null, null, null, true);
		}
		else {
			routingParameters = null;
		}
		Message replicateMessage = messageFactory.newMessage(messageSerialNo, nodeAccessor.getNodeId(), recipient.getNodeId(), nodeAccessor.getNetworkAdapter().getPublicAddressBytes(), HyCubeMessageType.REPLICATE, nodeAccessor.getNodeParameterSet().getMessageTTL(), (short)0, false, false, (short)0, (short)0, replicateMessageData); 

		NetworkNodePointer directRecipient = null;
		if (anonymousReplicate) {

			//generate random next hop for the replication message
			
			if (ANONYMOUS_REPLICATE_GENERATE_RANDOM_DIRECT_RECIPIENTS_ALL_NS) {
				//generate the random direct recipient among NS nodes
				routingTable.getNsLock().readLock().lock();
				int nsSize = routingTable.getNeighborhoodSet().size();
				ArrayList<RoutingTableEntry> nsCopy = new ArrayList<RoutingTableEntry>(routingTable.getNeighborhoodSet());
				routingTable.getNsLock().readLock().unlock();
				int drnsIndex = rand.nextInt(nsSize);
				directRecipient = nsCopy.get(drnsIndex).getNode().getNetworkNodePointer();
			}
			else {
				//generate the random direct recipient from the replication nodes list
				int drIndex = rand.nextInt(replicationNodes.size());
				directRecipient = replicationNodes.get(drIndex).getNetworkNodePointer();
			}

			
			
			
		}
		else {
			//send the message directly to the recipient:
			directRecipient = recipient.getNetworkNodePointer();
		}
		
		try {
			nodeAccessor.sendMessage(new MessageSendProcessInfo(replicateMessage, directRecipient, false, routingParameters), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
		} catch (NetworkAdapterException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a replicate message to a node.", e);
		} catch (ProcessMessageException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a replicate message to a node.", e);
		}
		
	}





	public void processReplicateMessage(NodePointer sender, HyCubeMessage msg, int resourcesNum, BigInteger[] keys, String[] resourceDescriptorStrings, long[] refreshTimes, int[] replicationSpreadNodesNums) throws ProcessMessageException {
		
		if (msg.getRecipientId().equals(nodeAccessor.getNodeId())) {
			if (!replicate) return;
			
			for (int i = 0; i < resourcesNum; i++) {
				
				//check if replica, if not -> do nothing
				if (isReplica(keys[i], this.nodeAccessor.getNodeId(), Math.min(Math.max(replicationNodesNum, replicationSpreadNodesNums[i]), maxReplicationSpreadNodesNum))) {
					
					HyCubeResourceDescriptor rd = new HyCubeResourceDescriptor(resourceDescriptorStrings[i]);
					
					HyCubeResourceEntry[] localResources = getFromStorage(keys[i], this.nodeAccessor.getNodeId(), rd);
			
					//check if the resource is stored locally (getFromStorage should return just one entry - only one resource may be stored for the same resource id and resource url)
					
					if (localResources != null && localResources.length > 0) {
						
						//refresh put to storage with the refresh time specified in the replicate message
						//set null as sender when storing replicated data 
						refreshPutToStorage(keys[i], sender.getNodeId(), rd, refreshTimes[i], true);
						
					}
					else {

						BigInteger key = keys[i];
						NodeId senderNodeId = sender.getNodeId();
						long refreshTime = refreshTimes[i];
						
						Object[] getCallbackArg = new Object[] {key, senderNodeId, refreshTime};
						
						//get the data - exact get from the replicate sender (with callback), on callback -> save the data -> call put
						GetCallback getCallback = new GetCallback() {
							@Override
							public void getReturned(Object callbackArg, Object getResultO) {
								HyCubeResource[] getResult = (HyCubeResource[]) getResultO;
								//only one record should be returned, as the exact resource id and resource url were specified
								if (getResult != null && getResult.length > 0) {
									if (devLog.isDebugEnabled()) {
										devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Replicating the resource to the local storage.");			
									}
									HyCubeResource r = new HyCubeResource(getResult[0].getResourceDescriptor(), getResult[0].getData());
									BigInteger key = (BigInteger) ((Object[])callbackArg)[0];
									NodeId senderNodeId = (NodeId) ((Object[])callbackArg)[1];
									long refreshTime = (Long) ((Object[])callbackArg)[2];
									
									putToStorage(key, senderNodeId, r, refreshTime, true);
									
								}
							}
						};
						
						//get the data with the callback saving the replica
						
						boolean registerRoute = replicationGetRegisterRoute;
						boolean anonymousRoute = replicationGetAnonymousRoute;
						Object[] parameters = createGetParameters(false, false, false, false, registerRoute, anonymousRoute);
						get(sender, key, rd, getCallback, getCallbackArg, parameters);
						
					}
					
				}
				else {
					//do nothing
				}
			}
		}
		else {
			MessageSendProcessInfo mspi = new MessageSendProcessInfo(msg);
			try {
				nodeAccessor.sendMessage(mspi, GlobalConstants.WAIT_ON_BKG_MSG_SEND);
			} catch (NetworkAdapterException e) {
				throw new ProcessMessageException("An exception has been thrown while routing the delete message.", e);
			}
		}



		
	
	}	
	
	
	
	
	
	@Override
	public boolean isReplica(BigInteger key, NodeId nodeId, int k) {
		
		if (checkIfResourceReplicaBeforeStoring == false) return true;
		
		double density = 0;
		
		
		routingTable.getNsLock().readLock().lock();
		
		
		int elemsTotal = (int) Math.round(densityCalculationQuantileFuncThreshold * (routingTable.getNeighborhoodSet().size()));
		int indexMax = elemsTotal - 1;
		if (indexMax < 0) indexMax = 0;
		double[] densities;
		
		if (estimateDensityBasedOnLastNodeOnly) {
			densities = new double[1];
		}
		else {
			densities = new double[elemsTotal];
		}
		
		int startEi = 0;
		if (estimateDensityBasedOnLastNodeOnly) {
			startEi = indexMax;
		}
		
		for (int ei = startEi; ei < elemsTotal; ei++) {
			
			density = 0;
			
			int index = ei;
			int elems = ei + 1;
		
			if (routingTable.getNeighborhoodSet().size() > 0) {
				if (assumeNsOrdered) {
	//				int elems = index + 1;
					double quantileDist = routingTable.getNeighborhoodSet().get(index).getDistance();
					density = ((double)elems) / Math.pow(quantileDist, routingTable.getDimensions());
				}
				else {
	//				int elems = index + 1;
					//get the elems first elements (ordered by distance ascending):
					ArrayList<RoutingTableEntry> nsCopy = new ArrayList<RoutingTableEntry>(routingTable.getNeighborhoodSet());
					for (int i = 0; i < elems; i++) {
						//find the closest node
						RoutingTableEntry closest = null;
						int closestIndex = 0;
						for (int j = 0; j < nsCopy.size(); j++) {
							if (closest == null || nsCopy.get(j).getDistance() < closest.getDistance()) {
								closest = nsCopy.get(j);
								closestIndex = j;
							}
						}
						if (i == elems - 1) {
							//this is the element we're looking for:
							double quantileDist = nsCopy.get(closestIndex).getDistance();
							density = ((double)elems) / Math.pow(quantileDist, routingTable.getDimensions());
						}
						else {
							//remove
							nsCopy.remove(closestIndex);
						}
					}
					
				}
			}
			else {
				density = 0;
			}
		
			if (estimateDensityBasedOnLastNodeOnly) {
				densities[0] = density;
			}
			else {
				densities[index] = density;
			}
		}
		
		density = 0;
		if (estimateDensityBasedOnLastNodeOnly) {
			density = densities[0];
		}
		else {
			for (int i = 0; i < densities.length; i++) {
				density += densities[i];
			}
			density = density / ((double)densities.length);
		}
		
		
		routingTable.getNsLock().readLock().unlock();
		
		
		double estDistK = Math.pow(((double)k)/((double)density), ((double)1) / ((double)routingTable.getDimensions()));

		HyCubeNodeId keyNodeId = (HyCubeNodeId) nodeAccessor.getNodeIdFactory().fromBigInteger(key);
		
		double distKey = HyCubeNodeId.calculateDistance((HyCubeNodeId) nodeId, keyNodeId, metric);
		
		if (distKey <= estDistK * estDistCoefficient) {
			return true;
		}
		else {
			return false;
		}
		
		
	}
	
	
	
	
	
	
	@Override
	public void discard() {	
	}



	
	
	public static Object[] createPutParameters(Boolean exactPut, Boolean secureRouting, Boolean skipRandomNextHops, Boolean registerRoute, Boolean anonymousRoute) {
		Object[] putParameters = new Object[] {exactPut, secureRouting, skipRandomNextHops, registerRoute, anonymousRoute};
		return putParameters;
	}
	
	public static boolean getPutParameterExactPut(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 0) || (!(parameters[0] instanceof Boolean))) return false;
		return (Boolean)parameters[0];
	}
	
	public static boolean getPutParameterSecureRouting(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 1) || (!(parameters[1] instanceof Boolean))) return false;
		return (Boolean)parameters[1];
	}
	
	public static boolean getPutParameterSkipRandomNextHops(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 2) || (!(parameters[2] instanceof Boolean))) return false;
		return (Boolean)parameters[2];
	}
	
	public static boolean getPutParameterRegisterRoute(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 3) || (!(parameters[3] instanceof Boolean))) return false;
		return (Boolean)parameters[3];
	}
	
	public static boolean getPutParameterAnonymousRoute(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 4) || (!(parameters[4] instanceof Boolean))) return false;
		return (Boolean)parameters[4];
	}
	
	
	
	
	public static Object[] createRefreshParameters(Boolean exactRefreshPut, Boolean secureRouting, Boolean skipRandomNextHops, Boolean registerRoute, Boolean anonymousRoute) {
		Object[] refreshPutParameters = new Object[] {exactRefreshPut, secureRouting, skipRandomNextHops, registerRoute, anonymousRoute};
		return refreshPutParameters;
	}
	
	public static boolean getRefreshPutParameterExactRefreshPut(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 0) || (!(parameters[0] instanceof Boolean))) return false;
		return (Boolean)parameters[0];
	}
	
	public static boolean getRefreshPutParameterSecureRouting(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 1) || (!(parameters[1] instanceof Boolean))) return false;
		return (Boolean)parameters[1];
	}
	
	public static boolean getRefreshPutParameterSkipRandomNextHops(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 2) || (!(parameters[2] instanceof Boolean))) return false;
		return (Boolean)parameters[2];
	}
	
	public static boolean getRefreshPutParameterRegisterRoute(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 3) || (!(parameters[3] instanceof Boolean))) return false;
		return (Boolean)parameters[3];
	}
	
	public static boolean getRefreshPutParameterAnonymousRoute(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 4) || (!(parameters[4] instanceof Boolean))) return false;
		return (Boolean)parameters[4];
	}
	
	
	
	
	public static Object[] createGetParameters(Boolean exactGet, Boolean findClosestNode, Boolean secureRouting, Boolean skipRandomNextHops, Boolean registerRoute, Boolean anonymousRoute) {
		Object[] getParameters = new Object[] {exactGet, findClosestNode, secureRouting, skipRandomNextHops, registerRoute, anonymousRoute};
		return getParameters;
	}
	
	public static boolean getGetParameterExactGet(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 0) || (!(parameters[0] instanceof Boolean))) return false;
		return (Boolean)parameters[0];
	}
	
	public static boolean getGetParameterFindClosestNode(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 1) || (!(parameters[1] instanceof Boolean))) return false;
		return (Boolean)parameters[1];
	}
	
	public static boolean getGetParameterSecureRouting(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 2) || (!(parameters[2] instanceof Boolean))) return false;
		return (Boolean)parameters[2];
	}
	
	public static boolean getGetParameterSkipRandomNextHops(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 3) || (!(parameters[3] instanceof Boolean))) return false;
		return (Boolean)parameters[3];
	}
	
	public static boolean getGetParameterRegisterRoute(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 4) || (!(parameters[4] instanceof Boolean))) return false;
		return (Boolean)parameters[4];
	}
	
	public static boolean getGetParameterAnonymousRoute(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 5) || (!(parameters[5] instanceof Boolean))) return false;
		return (Boolean)parameters[5];
	}
	
	


	public static Object[] createDeleteParameters(Boolean exactDelete, Boolean secureRouting, Boolean skipRandomNextHops, Boolean registerRoute, Boolean anonymousRoute) {
		Object[] deleteParameters = new Object[] {exactDelete, secureRouting, skipRandomNextHops, registerRoute, anonymousRoute};
		return deleteParameters;
	}

	public static boolean getDeleteParameterExactDelete(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 0) || (!(parameters[0] instanceof Boolean))) return false;
		return (Boolean)parameters[0];
	}
	
	public static boolean getDeleteParameterSecureRouting(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 1) || (!(parameters[1] instanceof Boolean))) return false;
		return (Boolean)parameters[1];
	}
	
	public static boolean getDeleteParameterSkipRandomNextHops(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 2) || (!(parameters[2] instanceof Boolean))) return false;
		return (Boolean)parameters[2];
	}
	
	public static boolean getDeleteParameterRegisterRoute(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 3) || (!(parameters[3] instanceof Boolean))) return false;
		return (Boolean)parameters[3];
	}
	
	public static boolean getDeleteParameterAnonymousRoute(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 4) || (!(parameters[4] instanceof Boolean))) return false;
		return (Boolean)parameters[4];
	}
	
	
	


	
}
