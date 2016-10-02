package net.hycube.maintenance;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Queue;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.HyCubeNodeId;
import net.hycube.core.HyCubeNodeIdFactory;
import net.hycube.core.HyCubeRoutingTable;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodeId;
import net.hycube.core.NodePointer;
import net.hycube.core.RoutingTableEntry;
import net.hycube.core.UnrecoverableRuntimeException;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.environment.NodeProperties;
import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventCategory;
import net.hycube.eventprocessing.EventScheduler;
import net.hycube.eventprocessing.EventType;
import net.hycube.messaging.messages.HyCubeMessageFactory;
import net.hycube.messaging.messages.HyCubeMessageType;
import net.hycube.messaging.messages.Message;
import net.hycube.messaging.processing.MessageSendProcessInfo;
import net.hycube.messaging.processing.ProcessMessageException;
import net.hycube.random.RandomMultiple;
import net.hycube.search.SearchCallback;
import net.hycube.search.SearchManager;
import net.hycube.transport.NetworkAdapterException;
import net.hycube.utils.HashMapUtils;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public class HyCubeRecoveryManager {

	
	public class HyCubeRecoveryEvent extends Event {
		
		protected HyCubeRecoveryType recoveryType;
		
		protected NodeId nodeId;
		protected short k;

		public HyCubeRecoveryEvent(HyCubeRecoveryType recoveryType) {
			super(0, getRecoveryEventType(), null, null);
			
			this.recoveryType = recoveryType;
			
		}
		
		@Override
		public void process() {
			switch (recoveryType) {
				case FULL_RECOVERY:
					HyCubeRecoveryManager.this.doRecover();
					break;
				case RECOVERY_NS:
					HyCubeRecoveryManager.this.doRecoverNS();
					break;
				case RECOVERY_ID:
					HyCubeRecoveryManager.this.doRecoverId(nodeId, k);
					break;
				default:
					break;
			}
			return;			
		}
		
	}
	
	public static class NotifiedNode {
		protected long time;
		protected long nodeIdHash;
		
		public NotifiedNode(long time, long nodeIdHash) {
			this.time = time;
			this.nodeIdHash = nodeIdHash;
		}
		
	}
	
	
	
	protected static final String PROP_KEY_SEND_RECOVERY_TO_NS = "SendRecoveryToNS";
	protected static final String PROP_KEY_SEND_RECOVERY_TO_RT1 = "SendRecoveryToRT1";
	protected static final String PROP_KEY_SEND_RECOVERY_TO_RT2 = "SendRecoveryToRT2";
	
	protected static final String PROP_KEY_SEND_NOTIFY_TO_NS = "SendNotifyToNS";
	protected static final String PROP_KEY_SEND_NOTIFY_TO_RT1 = "SendNotifyToRT1";
	protected static final String PROP_KEY_SEND_NOTIFY_TO_RT2 = "SendNotifyToRT2";
	
	protected static final String PROP_KEY_PROCESS_RECOVERY_AS_NOTIFY = "ProcessRecoveryAsNotify";
	
	protected static final String PROP_KEY_RETURN_NS = "ReturnNS";
	protected static final String PROP_KEY_RETURN_RT1 = "ReturnRT1";
	protected static final String PROP_KEY_RETURN_RT2 = "ReturnRT2";

	protected static final String PROP_KEY_RECOVERY_NS_RETURN_NS = "RecoveryNSReturnNS";
	protected static final String PROP_KEY_RECOVERY_NS_RETURN_RT1 = "RecoveryNSReturnRT1";
	protected static final String PROP_KEY_RECOVERY_NS_RETURN_RT2 = "RecoveryNSReturnRT2";
	
	protected static final String PROP_KEY_RECOVERY_ID_RETURN_NS = "RecoveryIdReturnNS";
	protected static final String PROP_KEY_RECOVERY_ID_RETURN_RT1 = "RecoveryIdReturnRT1";
	protected static final String PROP_KEY_RECOVERY_ID_RETURN_RT2 = "RecoveryIdReturnRT2";
	
	protected static final String PROP_KEY_SEND_NOTIFY_TO_RECOVERY_REPLY_NODES = "SendNotifyToRecoveryReplyNodes";
	
	protected static final String PROP_KEY_RECOVERY_EVENT_KEY = "RecoveryEventKey";
		
	protected static final String PROP_KEY_RECOVERY_NODES_MAX = "RecoveryNodesMax";
	protected static final String PROP_KEY_MIN_RECOVERY_INTERVAL = "MinRecoveryInterval";
	protected static final String PROP_KEY_MIN_NOTIFY_NODE_INTERVAL = "MinNotifyNodeInterval";
	protected static final String PROP_KEY_NOTIFY_NODES_CACHE_MAX_SIZE = "NotifyNodesCacheMaxSize";
	protected static final String PROP_KEY_NOTIFY_NODES_MAX = "NotifyNodesMax";
	protected static final String PROP_KEY_NOTIFY_RECOVERY_REPLY_NODES_MAX = "NotifyRecoveryReplyNodesMax";
	protected static final String PROP_KEY_RECOVERY_REPLY_NODES_TO_PROCESS_MAX = "RecoveryReplyNodesToProcessMax";
	protected static final String PROP_KEY_RECOVERY_REPLY_NODES_MAX = "RecoveryReplyNodesMax";
	
	
	
	protected static final int RECOVERY_NODES_INITIAL_SIZE = GlobalConstants.DEFAULT_INITIAL_COLLECTION_SIZE * 4;
	protected static final int NOTIFY_NODES_INITIAL_SIZE = GlobalConstants.DEFAULT_INITIAL_COLLECTION_SIZE * 4;
	
	
	
	protected NodeAccessor nodeAccessor;
	protected NodeProperties properties;
	
	
	
	protected boolean sendRecoveryToNS;
	protected boolean sendRecoveryToRT1;
	protected boolean sendRecoveryToRT2;
	
	protected boolean sendNotifyToNS;
	protected boolean sendNotifyToRT1;
	protected boolean sendNotifyToRT2;
	
	protected boolean processRecoveryAsNotify;
	
	protected boolean returnNS;
	protected boolean returnRT1;
	protected boolean returnRT2;
	
	protected boolean recoveryNSReturnNS;
	protected boolean recoveryNSReturnRT1;
	protected boolean recoveryNSReturnRT2;
	
	protected boolean recoveryIdReturnNS;
	protected boolean recoveryIdReturnRT1;
	protected boolean recoveryIdReturnRT2;
	
	protected boolean sendNotifyToRecoveryReplyNodes;
	
	
	protected boolean initialized;
	protected boolean recoveryInProgress;
	
	protected HyCubeNodeId nodeId;
	protected HyCubeRoutingTable routingTable;
	
	protected HyCubeMessageFactory messageFactory;
	protected HyCubeNodeIdFactory nodeIdFactory;
	
	protected NotifyProcessor notifyProcessor;
	
	protected EventType recoveryEventType;
	
	protected int recoveryNodesMax;
	protected int minRecoveryInterval; 
	protected int minNotifyNodeInterval;
	protected int notifyNodesCacheMaxSize;
	protected int notifyNodesMax;
	protected int notifyRecoveryReplyNodesMax;
	protected int recoveryReplyNodesToProcessMax;
	protected int recoveryReplyNodesMax;
	
	
	protected HashSet<Long> notifyNodes;
	protected LinkedList<NotifiedNode> notifyNodesByTime;
	protected HashMap<Long, Integer> notifyNodesElemCounts;
	
	
	protected boolean isScheduled;
	protected boolean wasRecoveryScheduled;
	protected long lastRecoveryTimestamp;
	
	
	
	
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		this.nodeAccessor = nodeAccessor;
		this.properties = properties;
		
		
		this.recoveryInProgress = false;
		
		
		if (! (nodeAccessor.getNodeId() instanceof HyCubeNodeId)) throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, "The node id is expected to be an instance of:" + HyCubeNodeId.class.getName());
		this.nodeId = (HyCubeNodeId) nodeAccessor.getNodeId();
		
		if (! (nodeAccessor.getRoutingTable() instanceof HyCubeRoutingTable)) throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, "The routing table is expected to be an instance of:" + HyCubeRoutingTable.class.getName());
		routingTable = (HyCubeRoutingTable) nodeAccessor.getRoutingTable();
	
		if (! (nodeAccessor.getMessageFactory() instanceof HyCubeMessageFactory)) throw new UnrecoverableRuntimeException("The message factory is expected to be an instance of: " + HyCubeMessageFactory.class.getName() + ".");
		messageFactory = (HyCubeMessageFactory) nodeAccessor.getMessageFactory();
		
		if (!(nodeAccessor.getNodeIdFactory() instanceof HyCubeNodeIdFactory)) throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize recovery manager instance. node id factory should be an instance of: " + HyCubeNodeIdFactory.class.getName() + ".");
		nodeIdFactory = (HyCubeNodeIdFactory) nodeAccessor.getNodeIdFactory();
		
		notifyProcessor = nodeAccessor.getNotifyProcessor();
		if (notifyProcessor == null) throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, "The notify processor is not set.");
		
		
		
		try {
			
			this.sendRecoveryToNS = (Boolean) properties.getProperty(PROP_KEY_SEND_RECOVERY_TO_NS, MappedType.BOOLEAN);
			this.sendRecoveryToRT1 = (Boolean) properties.getProperty(PROP_KEY_SEND_RECOVERY_TO_RT1, MappedType.BOOLEAN);
			this.sendRecoveryToRT2 = (Boolean) properties.getProperty(PROP_KEY_SEND_RECOVERY_TO_RT2, MappedType.BOOLEAN);
			
			this.sendNotifyToNS = (Boolean) properties.getProperty(PROP_KEY_SEND_NOTIFY_TO_NS, MappedType.BOOLEAN);
			this.sendNotifyToRT1 = (Boolean) properties.getProperty(PROP_KEY_SEND_NOTIFY_TO_RT1, MappedType.BOOLEAN);
			this.sendNotifyToRT2 = (Boolean) properties.getProperty(PROP_KEY_SEND_NOTIFY_TO_RT2, MappedType.BOOLEAN);
			
			this.processRecoveryAsNotify = (Boolean) properties.getProperty(PROP_KEY_PROCESS_RECOVERY_AS_NOTIFY, MappedType.BOOLEAN);
			
			this.returnNS = (Boolean) properties.getProperty(PROP_KEY_RETURN_NS, MappedType.BOOLEAN);
			this.returnRT1 = (Boolean) properties.getProperty(PROP_KEY_RETURN_RT1, MappedType.BOOLEAN);
			this.returnRT2 = (Boolean) properties.getProperty(PROP_KEY_RETURN_RT2, MappedType.BOOLEAN);
			
			this.recoveryNSReturnNS = (Boolean) properties.getProperty(PROP_KEY_RECOVERY_NS_RETURN_NS, MappedType.BOOLEAN);
			this.recoveryNSReturnRT1 = (Boolean) properties.getProperty(PROP_KEY_RECOVERY_NS_RETURN_RT1, MappedType.BOOLEAN);
			this.recoveryNSReturnRT2 = (Boolean) properties.getProperty(PROP_KEY_RECOVERY_NS_RETURN_RT2, MappedType.BOOLEAN);
			
			this.recoveryIdReturnNS = (Boolean) properties.getProperty(PROP_KEY_RECOVERY_ID_RETURN_NS, MappedType.BOOLEAN);
			this.recoveryIdReturnRT1 = (Boolean) properties.getProperty(PROP_KEY_RECOVERY_ID_RETURN_RT1, MappedType.BOOLEAN);
			this.recoveryIdReturnRT2 = (Boolean) properties.getProperty(PROP_KEY_RECOVERY_ID_RETURN_RT2, MappedType.BOOLEAN);
			
			
			this.sendNotifyToRecoveryReplyNodes = (Boolean) properties.getProperty(PROP_KEY_SEND_NOTIFY_TO_RECOVERY_REPLY_NODES, MappedType.BOOLEAN);
			
		
			
			this.recoveryNodesMax = (Integer) properties.getProperty(PROP_KEY_RECOVERY_NODES_MAX, MappedType.INT);
			
			this.minRecoveryInterval = (Integer) properties.getProperty(PROP_KEY_MIN_RECOVERY_INTERVAL, MappedType.INT);
			
			this.minNotifyNodeInterval = (Integer) properties.getProperty(PROP_KEY_MIN_NOTIFY_NODE_INTERVAL, MappedType.INT);
			
			this.notifyNodesCacheMaxSize = (Integer) properties.getProperty(PROP_KEY_NOTIFY_NODES_CACHE_MAX_SIZE, MappedType.INT);
			
			this.notifyNodesMax = (Integer) properties.getProperty(PROP_KEY_NOTIFY_NODES_MAX, MappedType.INT);
			
			this.notifyRecoveryReplyNodesMax = (Integer) properties.getProperty(PROP_KEY_NOTIFY_RECOVERY_REPLY_NODES_MAX, MappedType.INT);
			
			this.recoveryReplyNodesToProcessMax = (Integer) properties.getProperty(PROP_KEY_RECOVERY_REPLY_NODES_TO_PROCESS_MAX, MappedType.INT);
			
			this.recoveryReplyNodesMax = (Integer) properties.getProperty(PROP_KEY_RECOVERY_REPLY_NODES_MAX, MappedType.INT);
			
			
			
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize the lookup manager instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		
		String recoveryEventTypeKey = properties.getProperty(PROP_KEY_RECOVERY_EVENT_KEY);
		recoveryEventType = new EventType(EventCategory.extEvent, recoveryEventTypeKey);
				
		
		
		notifyNodes = new HashSet<Long>(HashMapUtils.getHashMapCapacityForElementsNum(NOTIFY_NODES_INITIAL_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		notifyNodesByTime = new LinkedList<NotifiedNode>();
		notifyNodesElemCounts = new HashMap<Long, Integer>(HashMapUtils.getHashMapCapacityForElementsNum(NOTIFY_NODES_INITIAL_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		
		isScheduled = false;
		wasRecoveryScheduled = false;
		lastRecoveryTimestamp = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
		
		
		initialized = true;
		
		
		
	}
	
	
	
	public void recover() {
		
		//enqueue the recovery event
		
		//create the event:
		Event event = new HyCubeRecoveryEvent(HyCubeRecoveryType.FULL_RECOVERY);
				
		//schedule the event:
		Queue<Event> queue = nodeAccessor.getEventQueue(recoveryEventType);
		EventScheduler scheduler = nodeAccessor.getEventScheduler();
		
		long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
		
		
		synchronized(this) {
			if (! isScheduled) {
				if (wasRecoveryScheduled) {
					if (currTime > lastRecoveryTimestamp) {
						if (currTime - lastRecoveryTimestamp >= minRecoveryInterval) {
							queue.add(event);
							isScheduled = true;
						}
						else {
							scheduler.scheduleEvent(event, queue, lastRecoveryTimestamp + minRecoveryInterval);
							isScheduled = true;
						}
					}
				}
				else {
					//schedule without checking last recovery time
					queue.add(event);
					isScheduled = true;
					wasRecoveryScheduled = true;
				}
			}
			else return;
		}

		
	}
	
	
	public void recoverNS() {
		
		//enqueue the recovery event
		
		//create the event:
		Event event = new HyCubeRecoveryEvent(HyCubeRecoveryType.RECOVERY_NS);
				
		//schedule the event:
		Queue<Event> queue = nodeAccessor.getEventQueue(recoveryEventType);
		EventScheduler scheduler = nodeAccessor.getEventScheduler();

		long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
		
		
		synchronized(this) {
			if (! isScheduled) {
				if (wasRecoveryScheduled) {
					if (currTime > lastRecoveryTimestamp) {
						if (currTime - lastRecoveryTimestamp >= minRecoveryInterval) {
							queue.add(event);
							isScheduled = true;
						}
						else {
							scheduler.scheduleEvent(event, queue, lastRecoveryTimestamp + minRecoveryInterval);
							isScheduled = true;
						}
					}
				}
				else {
					//schedule without checking last recovery time
					queue.add(event);
					isScheduled = true;
					wasRecoveryScheduled = true;
				}
			}
			else return;
		}
		
	}

	
	
	public void recoverId(NodeId nodeId, short k) {
		//enqueue the recovery event
		
		//create the event:
		HyCubeRecoveryEvent event = new HyCubeRecoveryEvent(HyCubeRecoveryType.RECOVERY_ID);
		event.nodeId = nodeId;
		event.k = k;
						
		//schedule the event:
		Queue<Event> queue = nodeAccessor.getEventQueue(recoveryEventType);
		
		queue.add(event);		
		
		
	}
	
	
	//!!! should be called only by a enqueued HyCubeRecoveryEvent (recover call)
	protected void doRecover() {
		
		synchronized (this) {
			
			if (this.initialized == false) {
				return;
			}
			
			isScheduled = false;
			lastRecoveryTimestamp = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
			
		}
		

		int recoveryNsNodesNum = 0;
		int recoveryRtNodesNum = 0;
		
		routingTable.getNsLock().readLock().lock();
		routingTable.getRt1Lock().readLock().lock();
		routingTable.getRt2Lock().readLock().lock();
		
		if (sendRecoveryToNS) recoveryNsNodesNum += routingTable.getNeighborhoodSet().size();
		if (sendRecoveryToRT1) recoveryRtNodesNum += routingTable.getRt1Map().size();
		if (sendRecoveryToRT2) recoveryRtNodesNum += routingTable.getRt2Map().size();

		HashMap<Long, NodePointer> recoveryNsNodes = new HashMap<Long, NodePointer>(HashMapUtils.getHashMapCapacityForElementsNum(recoveryNsNodesNum, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		HashMap<Long, NodePointer> recoveryRtNodes = new HashMap<Long, NodePointer>(HashMapUtils.getHashMapCapacityForElementsNum(recoveryRtNodesNum, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		
		
		//send recovery messages to neighbors
		
		//add the nodes to the map (will prevent sending messages to the same nodes many times):
		
		if (sendRecoveryToNS) {
			for (RoutingTableEntry rte : routingTable.getNeighborhoodSet()) {
				if (! recoveryNsNodes.containsKey(rte.getNodeIdHash())) {
					recoveryNsNodes.put(rte.getNodeIdHash(), rte.getNode());
				}
			}
		}

		
		if (sendRecoveryToRT1) {
			for (RoutingTableEntry rte : routingTable.getRt1Map().values()) {
				if ((! recoveryNsNodes.containsKey(rte.getNodeIdHash())) && (! recoveryRtNodes.containsKey(rte.getNodeIdHash()))) {
					recoveryRtNodes.put(rte.getNodeIdHash(), rte.getNode());
				}
			}
		}
		if (sendRecoveryToRT2) {
			for (RoutingTableEntry rte : routingTable.getRt2Map().values()) {
				if ((! recoveryNsNodes.containsKey(rte.getNodeIdHash())) && (! recoveryRtNodes.containsKey(rte.getNodeIdHash()))) {
					recoveryRtNodes.put(rte.getNodeIdHash(), rte.getNode());
				}
			}
		}

		
		routingTable.getRt2Lock().readLock().unlock();
		routingTable.getRt1Lock().readLock().unlock();
		routingTable.getNsLock().readLock().unlock();
		
		
		HashSet<Integer> indexesToSkip;
		if (this.recoveryNodesMax > 0) {
			int recoveryRtNodesMax = recoveryNodesMax - recoveryNsNodes.size();
			if (recoveryRtNodesMax < 0) recoveryRtNodesMax = 0;
			int numToSkip = recoveryRtNodes.size() - recoveryRtNodesMax;
			if (numToSkip < 0) numToSkip = 0;
			int[] randomIndToSkip = RandomMultiple.randomSelection(recoveryRtNodes.size(), numToSkip);
			indexesToSkip = new HashSet<Integer>(HashMapUtils.getHashMapCapacityForElementsNum(randomIndToSkip.length, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
			for (int indexToSkip : randomIndToSkip) {
				indexesToSkip.add(indexToSkip);
			}

		}
		else indexesToSkip = new HashSet<Integer>(HashMapUtils.getHashMapCapacityForElementsNum(0, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		

				
		
		
		for (NodePointer np : recoveryNsNodes.values()) {
			
			sendRecoveryRequest(np, this.returnNS, this.returnRT1, this.returnRT2);

			if (minNotifyNodeInterval > 0) {
				if (processRecoveryAsNotify) {
					//mark nodes as notified
					checkIfNotifyAndCacheNodeAsNotified(np.getNodeIdHash());
				}
			}

		}
		
		int index = 0;
		for (NodePointer np : recoveryRtNodes.values()) {
			
			if (indexesToSkip.contains(index)) {
				index++;
				continue;
			}

			sendRecoveryRequest(np, this.returnNS, this.returnRT1, this.returnRT2);
		
			if (minNotifyNodeInterval > 0) {
				if (processRecoveryAsNotify) {
					//mark nodes as notified
					checkIfNotifyAndCacheNodeAsNotified(np.getNodeIdHash());
				}
			}
			
			index++;
		}
		
		
		
		
		
		//send notify messages to neighbors
		
		routingTable.getNsLock().readLock().lock();
		routingTable.getRt1Lock().readLock().lock();
		routingTable.getRt2Lock().readLock().lock();
		
		
		//add the nodes to the map (will prevent sending messages to the same nodes many times):
		
		LinkedList<NodePointer> nodesNsToSendNotify = new LinkedList<NodePointer>();
		LinkedList<NodePointer> nodesRtToSendNotify = new LinkedList<NodePointer>();
		
		//hash set to preventing sending multiple notifies to the same node
		int numNodesToNotifyMax = 0;
		if (sendNotifyToNS) numNodesToNotifyMax += routingTable.getNeighborhoodSet().size();
		if (sendNotifyToRT1) numNodesToNotifyMax += routingTable.getRt1Map().size();
		if (sendNotifyToRT2) numNodesToNotifyMax += routingTable.getRt2Map().size();
		
		HashSet<Long> nodesNotified = new HashSet<Long>(HashMapUtils.getHashMapCapacityForElementsNum(numNodesToNotifyMax, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		
		if (sendNotifyToNS) {
			for (RoutingTableEntry rte : routingTable.getNeighborhoodSet()) {
				
				if (nodesNotified.contains(rte.getNodeIdHash())) continue;
				
				nodesNotified.add(rte.getNodeIdHash());
				
				boolean sendNotify = checkIfNotifyAndCacheNodeAsNotified(rte.getNodeIdHash());
				if (sendNotify) nodesNsToSendNotify.add(rte.getNode());
				
			}
			
		}
		
		
		
		
		if (sendNotifyToRT1) {
			
			for (RoutingTableEntry rte : routingTable.getRt1Map().values()) {
				
				if (nodesNotified.contains(rte.getNodeIdHash())) continue;
				
				nodesNotified.add(rte.getNodeIdHash());
				
				boolean sendNotify = checkIfNotifyAndCacheNodeAsNotified(rte.getNodeIdHash());
				if (sendNotify) nodesRtToSendNotify.add(rte.getNode());
				
			}
			
		}
		if (sendNotifyToRT2) {
			
			for (RoutingTableEntry rte : routingTable.getRt2Map().values()) {

				if (nodesNotified.contains(rte.getNodeIdHash())) continue;
				
				nodesNotified.add(rte.getNodeIdHash());
				
				boolean sendNotify = checkIfNotifyAndCacheNodeAsNotified(rte.getNodeIdHash());
				if (sendNotify) nodesRtToSendNotify.add(rte.getNode());
				
			}
			
		}
		
		
		
		//release locks
		routingTable.getRt2Lock().readLock().unlock();
		routingTable.getRt1Lock().readLock().unlock();
		routingTable.getNsLock().readLock().unlock();
		
		
		
		
		HashSet<Integer> indexesToSkipNotify;
		if (this.notifyNodesMax > 0) {
			int notifyNodesFromRTNum = notifyNodesMax - nodesNsToSendNotify.size();
			if (notifyNodesFromRTNum < 0) notifyNodesFromRTNum = 0;
			int numToSkip = nodesRtToSendNotify.size() - notifyNodesFromRTNum;
			if (numToSkip < 0) numToSkip = 0;
			int[] randomIndToSkip = RandomMultiple.randomSelection(nodesRtToSendNotify.size(), numToSkip);
			indexesToSkipNotify = new HashSet<Integer>(HashMapUtils.getHashMapCapacityForElementsNum(randomIndToSkip.length, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
			for (int indexToSkip : randomIndToSkip) {
				indexesToSkipNotify.add(indexToSkip);
			}

		}
		else indexesToSkipNotify = new HashSet<Integer>(HashMapUtils.getHashMapCapacityForElementsNum(0, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		
		
		
		
		 
		
		//send notify to nodes (beyond the synchronized block):
		for (NodePointer n : nodesNsToSendNotify) {

			sendNotify(n);
			
		}
		
		
		index = 0;

		for (NodePointer n : nodesRtToSendNotify) {
			
			if (indexesToSkipNotify.contains(index)) continue;
			
			sendNotify(n);
			
			index++;
			
		}
		
	}
	
	
	

	/**
	 * Should be called only by a enqueued HyCubeRecoveryEvent (recoverNS call) !!! 
	 */
	protected void doRecoverNS() {
		
		synchronized (this) {
			
			if (this.initialized == false) {
				return;
			}
			
			isScheduled = false;
			lastRecoveryTimestamp = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
			
		}
		

		int recoveryNodesNum = 0;
		
		
		routingTable.getNsLock().readLock().lock();
		
		if (sendRecoveryToNS) recoveryNodesNum += routingTable.getNeighborhoodSet().size();
		
		HashMap<Long, NodePointer> recoveryNodes = new HashMap<Long, NodePointer>(HashMapUtils.getHashMapCapacityForElementsNum(recoveryNodesNum, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);

				
		//send recovery messages to neighbors
		
		//add the nodes to the map (will prevent sending messages to the same nodes many times):
		
		if (sendRecoveryToNS) {
			for (RoutingTableEntry rte : routingTable.getNeighborhoodSet()) {
				if (! recoveryNodes.containsKey(rte.getNodeIdHash())) {
					recoveryNodes.put(rte.getNodeIdHash(), rte.getNode());
				}
			}
		}
			
		routingTable.getNsLock().readLock().unlock();
		
		
				
		if (minNotifyNodeInterval > 0) {
			if (processRecoveryAsNotify) {
				for (NodePointer np : recoveryNodes.values()) {
					checkIfNotifyAndCacheNodeAsNotified(np.getNodeIdHash());
				}
			}
		}
		
		
		for (NodePointer np : recoveryNodes.values()) {
			sendRecoveryRequest(np, this.recoveryNSReturnNS, this.recoveryNSReturnRT1, this.recoveryNSReturnRT2);
		}
		
		
		

		//send notify messages to neighbors
		
		//add the nodes to the map (will prevent sending messages to the same nodes many times):
		
		LinkedList<NodePointer> nodesToSendNotify = new LinkedList<NodePointer>();
		
		if (sendNotifyToNS) {
			routingTable.getNsLock().readLock().lock();
			for (RoutingTableEntry rte : routingTable.getNeighborhoodSet()) {
				boolean sendNotify = checkIfNotifyAndCacheNodeAsNotified(rte.getNodeIdHash());
				if (sendNotify) nodesToSendNotify.add(rte.getNode());
				
			}
			routingTable.getNsLock().readLock().unlock();
			
			//send notify to nodes (beyond the synchronized block):
			for (NodePointer n : nodesToSendNotify) {
				sendNotify(n);
			}
			
		}
		
		
		
				
		
		
	}
	
	
	
	//!!! should be called only by a enqueued HyCubeRecoveryEvent (recoverId call)
	protected void doRecoverId(NodeId nodeId, short k) {
		
		if (nodeId == null) {
			throw new IllegalArgumentException("Node ID is null.");
		}
		if (k <= 0) {
			throw new IllegalArgumentException("Illegal value of k.");
		}
		
		
		synchronized (this) {
			
			if (this.initialized == false) {
				return;
			}
			
		}
		

		
		SearchManager searchManager = nodeAccessor.getSearchManager();
		
		SearchCallback searchCallback = new SearchCallback() {
			@Override
			public void searchReturned(int searchId, Object callbackArg, NodePointer[] result) {
				doRecoverIdFromNodes(result);
			}
		};
		
		searchManager.search(nodeId, (short) k, searchCallback, null);
		
		
		
		
				
		
		
	}
	
	
	protected void doRecoverIdFromNodes(NodePointer[] nodePointers) {
		doRecoverFromNodes(nodePointers, this.recoveryIdReturnNS, this.recoveryIdReturnRT1, this.recoveryIdReturnRT2);
	}
	
	
	protected void doRecoverFromNodes(NodePointer[] nodePointers, boolean returnNS, boolean returnRT1, boolean returnRT2) {
		
		synchronized (this) {
			
			if (this.initialized == false) {
				return;
			}
			
		}
		

		HashMap<Long, NodePointer> recoveryNodes = new HashMap<Long, NodePointer>(HashMapUtils.getHashMapCapacityForElementsNum(nodePointers.length, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);

		
		
		
		//send recovery messages to neighbors
		
		//add the nodes to the map (will prevent sending messages to the same nodes many times):
		
		for (NodePointer np : nodePointers) {
			if (! recoveryNodes.containsKey(np.getNodeIdHash())) {
				recoveryNodes.put(np.getNodeIdHash(), np);
			}
		}
		
				
		if (minNotifyNodeInterval > 0) {
			if (processRecoveryAsNotify) {
				for (NodePointer np : recoveryNodes.values()) {
					checkIfNotifyAndCacheNodeAsNotified(np.getNodeIdHash());
				}
			}
		}
		
		
		for (NodePointer np : recoveryNodes.values()) {
			sendRecoveryRequest(np, returnNS, returnRT1, returnRT2);
		}
		
		
		

		//send notify messages to neighbors
		
		//add the nodes to the map (will prevent sending messages to the same nodes many times):
		
		LinkedList<NodePointer> nodesToSendNotify = new LinkedList<NodePointer>();
		
		if (sendNotifyToNS) {
			routingTable.getNsLock().readLock().lock();
			for (RoutingTableEntry rte : routingTable.getNeighborhoodSet()) {
				boolean sendNotify = checkIfNotifyAndCacheNodeAsNotified(rte.getNodeIdHash());
				if (sendNotify) nodesToSendNotify.add(rte.getNode());
				
			}
			routingTable.getNsLock().readLock().unlock();
			
			//send notify to nodes (beyond the synchronized block):
			for (NodePointer n : nodesToSendNotify) {
				sendNotify(n);
			}
			
		}

	}
	
	
	

	

	
	
	
	protected void sendRecoveryRequest(NodePointer recipient, boolean returnNS, boolean returnRT1, boolean returnRT2) {
		
		//prepare the message:
		
		int messageSerialNo = nodeAccessor.getNextMessageSerialNo();
		HyCubeRecoveryMessageData recMessageData = new HyCubeRecoveryMessageData();
		recMessageData.setReturnNS(returnNS);
		recMessageData.setReturnRT1(returnRT1);
		recMessageData.setReturnRT2(returnRT2);
		
		byte[] recMessageDataBytes = recMessageData.getBytes(); 

		Message recMessage = messageFactory.newMessage(messageSerialNo, nodeAccessor.getNodeId(), recipient.getNodeId(), nodeAccessor.getNetworkAdapter().getPublicAddressBytes(), HyCubeMessageType.RECOVERY, nodeAccessor.getNodeParameterSet().getMessageTTL(), (short)0, false, false, (short)0, (short)0, recMessageDataBytes); 
			
		//send the message directly to the recipient:
		try {
			nodeAccessor.sendMessage(new MessageSendProcessInfo(recMessage, recipient.getNetworkNodePointer(), false), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
		} catch (NetworkAdapterException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a recovery to a node.", e);
		} catch (ProcessMessageException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a recovery to a node.", e);
		}
		
		
	}
	
	
	protected void sendNotify(NodePointer recipient) {
		
		//prepare the message:
		
		int messageSerialNo = nodeAccessor.getNextMessageSerialNo(); 
		Message notifyMessage = messageFactory.newMessage(messageSerialNo, nodeAccessor.getNodeId(), recipient.getNodeId(), nodeAccessor.getNetworkAdapter().getPublicAddressBytes(), HyCubeMessageType.NOTIFY, nodeAccessor.getNodeParameterSet().getMessageTTL(), (short)0, false, false, (short)0, (short)0, null); 
			
		//send the message directly to the recipient:
		try {
			nodeAccessor.sendMessage(new MessageSendProcessInfo(notifyMessage, recipient.getNetworkNodePointer(), false), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
		} catch (NetworkAdapterException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a notify message to a node.", e);
		} catch (ProcessMessageException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a notify message to a node.", e);
		}
		
		
	}



	public void processRecoveryRequest(NodePointer sender, boolean returnNS, boolean returnRT1, boolean returnRT2) {
		
		synchronized (this) {
			if (this.initialized == false) {
				return;
			}
		}
		
		NodePointer[] nodesReturned = null;
		
		
		
		
		
		routingTable.getNsLock().readLock().lock();
		routingTable.getRt1Lock().readLock().lock();
		routingTable.getRt2Lock().readLock().lock();
		
		
		
		
		Collection<RoutingTableEntry> nsRtes = null;
		if (returnNS) {
			nsRtes = routingTable.getNsMap().values();
		}

		Collection<RoutingTableEntry> rt1Rtes = null;
		if (returnRT1) {
			rt1Rtes = routingTable.getRt1Map().values();
		}

		Collection<RoutingTableEntry> rt2Rtes = null;
		if (returnRT2) {
			rt2Rtes = routingTable.getRt2Map().values();
		}


		int numNsNodesFound = (nsRtes != null ? nsRtes.size() : 0);
		int numRtNodesFound = (rt1Rtes != null ? rt1Rtes.size() : 0) + (rt2Rtes != null ? rt2Rtes.size() : 0);


		//maps - to remove doubles:
		HashMap<Long, NodePointer> nodesNsFoundMap = new HashMap<Long, NodePointer>(HashMapUtils.getHashMapCapacityForElementsNum(numNsNodesFound, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		HashMap<Long, NodePointer> nodesRtFoundMap = new HashMap<Long, NodePointer>(HashMapUtils.getHashMapCapacityForElementsNum(numRtNodesFound, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);

		if (nsRtes != null) {
			for (RoutingTableEntry rte : nsRtes) {
				if (!nodesNsFoundMap.containsKey(rte.getNodeIdHash())) {
					nodesNsFoundMap.put(rte.getNodeIdHash(), rte.getNode());
				}
			}
		}
		if (rt1Rtes != null) {
			for (RoutingTableEntry rte : rt1Rtes) {
				if ((!nodesNsFoundMap.containsKey(rte.getNodeIdHash())) && (!nodesRtFoundMap.containsKey(rte.getNodeIdHash()))) {
					nodesRtFoundMap.put(rte.getNodeIdHash(), rte.getNode());
				}
			}
		}
		if (rt2Rtes != null) {
			for (RoutingTableEntry rte : rt2Rtes) {
				if ((!nodesNsFoundMap.containsKey(rte.getNodeIdHash())) && (!nodesRtFoundMap.containsKey(rte.getNodeIdHash()))) {
					nodesRtFoundMap.put(rte.getNodeIdHash(), rte.getNode());
				}
			}
		}

		
		
		routingTable.getRt2Lock().readLock().unlock();
		routingTable.getRt1Lock().readLock().unlock();
		routingTable.getNsLock().readLock().unlock();
		
		
		
		
		HashSet<Integer> indexesToSkip;
		if (this.recoveryReplyNodesMax > 0) {
			int recoveryReplyRtNodesMax = recoveryReplyNodesMax - nodesNsFoundMap.size();
			if (recoveryReplyRtNodesMax < 0) recoveryReplyRtNodesMax = 0;
			int numToSkip = nodesRtFoundMap.size() - recoveryReplyRtNodesMax;
			if (numToSkip < 0) numToSkip = 0;
			int[] randomIndToSkip = RandomMultiple.randomSelection(nodesRtFoundMap.size(), numToSkip);
			indexesToSkip = new HashSet<Integer>(HashMapUtils.getHashMapCapacityForElementsNum(randomIndToSkip.length, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
			for (int indexToSkip : randomIndToSkip) {
				indexesToSkip.add(indexToSkip);
			}

		}
		else indexesToSkip = new HashSet<Integer>(HashMapUtils.getHashMapCapacityForElementsNum(0, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		
		
		
		
		nodesReturned = new NodePointer[nodesNsFoundMap.size() + nodesRtFoundMap.size() - indexesToSkip.size()];
		int index = 0;
		for (NodePointer np : nodesNsFoundMap.values()) {
			nodesReturned[index] = np;
			index++;
		}
		int rtNodeIndex = 0;
		for (NodePointer np : nodesRtFoundMap.values()) {
			if (indexesToSkip.contains(rtNodeIndex)) {
				rtNodeIndex++;
				continue;
			}
			nodesReturned[index] = np;
			index++;
			rtNodeIndex++;
		}
				

		
		
		//send the reply to the requesting node:
		sendRecoveryReply(sender, nodesReturned);
		
		
		
		if (processRecoveryAsNotify) {
			long currTimestamp = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
			notifyProcessor.processNotify(sender, currTimestamp);
			
		}
		
		
		
	}


	protected void sendRecoveryReply(NodePointer recipient, NodePointer[] nodesReturned) {

		//prepare the message:
		
		int messageSerialNo = nodeAccessor.getNextMessageSerialNo();
		byte[] recReplyMessageData = (new HyCubeRecoveryReplyMessageData(nodesReturned)).getBytes(nodeIdFactory.getDimensions(), nodeIdFactory.getDigitsCount()); 
		Message recReplyMessage = messageFactory.newMessage(messageSerialNo, nodeAccessor.getNodeId(), recipient.getNodeId(), nodeAccessor.getNetworkAdapter().getPublicAddressBytes(), HyCubeMessageType.RECOVERY_REPLY, nodeAccessor.getNodeParameterSet().getMessageTTL(), (short)0, false, false, (short)0, (short)0, recReplyMessageData); 

		
		//send the message directly to the recipient:
		try {
			nodeAccessor.sendMessage(new MessageSendProcessInfo(recReplyMessage, recipient.getNetworkNodePointer(), false), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
		} catch (NetworkAdapterException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a recovery response to a node.", e);
		} catch (ProcessMessageException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a recovery response to a node.", e);
		}

	}







	public void processRecoveryResponse(NodePointer sender, NodePointer[] nodesReturned) {
		
		synchronized (this) {
			if (this.initialized == false) {
				return;
			}
		}
		
		//process the recovery reply
		long currTimestamp = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
		int numNodesProcessed = 0;
		for (NodePointer np : nodesReturned) {
			
			if (recoveryReplyNodesToProcessMax > 0 && numNodesProcessed >= recoveryReplyNodesToProcessMax) break;	//process only FIRST recoveryReplyNodesToProcessMax nodes 
			
			notifyProcessor.processNotify(np, currTimestamp);
			numNodesProcessed++;

			
		}
		
		
		if (sendNotifyToRecoveryReplyNodes) {

			int numNodesNotified = 0;
			for (NodePointer np : nodesReturned) {
				
				if (notifyRecoveryReplyNodesMax > 0 && numNodesNotified >= notifyRecoveryReplyNodesMax) break;
				
				boolean sendNotify = checkIfNotifyAndCacheNodeAsNotified(np.getNodeIdHash());
				if (sendNotify) {
					sendNotify(np);
					numNodesNotified++;
				}
				
			}
			
		}
		
		
	}

	
	
	
	
	
	protected boolean checkIfNotifyAndCacheNodeAsNotified(long nodeIdHash) {
		
		synchronized (notifyNodes) {
			long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
			if (! notifyNodes.contains(nodeIdHash)) {
				if (minNotifyNodeInterval > 0) {
					
					notifyNodes.add(nodeIdHash);
					notifyNodesByTime.add(new NotifiedNode(currTime, nodeIdHash));
					
					int elemCount;
					if (notifyNodesElemCounts.containsKey(nodeIdHash)) {
						elemCount = notifyNodesElemCounts.get(nodeIdHash) + 1;
					}
					else elemCount = 1;
					notifyNodesElemCounts.put(nodeIdHash, elemCount);
										
					while (notifyNodes.size() > notifyNodesCacheMaxSize){
						NotifiedNode removed = notifyNodesByTime.removeFirst();
						int elemCountRemoved;
						if (notifyNodesElemCounts.containsKey(removed.nodeIdHash)) {
							elemCountRemoved = notifyNodesElemCounts.get(removed.nodeIdHash);
						}
						else elemCountRemoved = 1;
						
						if (elemCountRemoved > 1) {
							notifyNodesElemCounts.put(removed.nodeIdHash, elemCountRemoved - 1);
						}
						else {
							notifyNodes.remove(removed.nodeIdHash);
							notifyNodesElemCounts.remove(removed.nodeIdHash);
						}
					}
					
				}
				return true;
			}
			else return false;
		}
		
		
	}
	
	
	
	
	
	public void clearNotifyNodes() {

		long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
		
		synchronized (notifyNodes) {
			ListIterator<NotifiedNode> iter = notifyNodesByTime.listIterator();
			while (iter.hasNext()) {
				NotifiedNode nn = iter.next();
				if (currTime >= nn.time + minNotifyNodeInterval) {
					
					iter.remove();
					
					int elemCount;
					if (notifyNodesElemCounts.containsKey(nn.nodeIdHash)) {
						elemCount = notifyNodesElemCounts.get(nn.nodeIdHash);
					}
					else elemCount = 1;
					
					if (elemCount > 1) {
						notifyNodesElemCounts.put(nn.nodeIdHash, elemCount - 1);
					}
					else {
						notifyNodes.remove(nn.nodeIdHash);
						notifyNodesElemCounts.remove(nn.nodeIdHash);
					}
					
				}
				else {
					//the list is sorted by time, no more nodes would be found
					break;
				}
			}
		
		}
		
	}
	
	
	
	
	public EventType getRecoveryEventType() {
		return recoveryEventType;
	}
	
		
	
	
	
	
	
	

	public void discard() {
		synchronized (this) {
			recoveryInProgress = false;
			initialized = false;
		}
		
	}









	

}
