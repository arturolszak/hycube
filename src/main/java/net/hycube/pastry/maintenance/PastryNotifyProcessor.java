package net.hycube.pastry.maintenance;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.HyCubeNodeId;
import net.hycube.core.HyCubeRoutingTableType;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodeId;
import net.hycube.core.NodePointer;
import net.hycube.core.RoutingTable;
import net.hycube.core.RoutingTableEntry;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.environment.NodeProperties;
import net.hycube.logging.LogHelper;
import net.hycube.maintenance.NotifyProcessor;
import net.hycube.pastry.core.PastryRoutingTable;
import net.hycube.rtnodeselection.HyCubeNSNodeSelector;
import net.hycube.rtnodeselection.HyCubeRTNodeSelector;
import net.hycube.utils.ClassInstanceLoadException;
import net.hycube.utils.ClassInstanceLoader;
import net.hycube.utils.HashMapUtils;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public class PastryNotifyProcessor extends NotifyProcessor {

//	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(PastryNotifyProcessor.class); 
	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
	
	
	protected static final String PROP_KEY_DIMENSIONS = "Dimensions";
	protected static final String PROP_KEY_LEVELS = "Levels";
	protected static final String PROP_KEY_LS_SIZE = "LSSize";
	protected static final String PROP_KEY_ROUTING_TABLE_SLOT_SIZE = "RoutingTableSlotSize";
	protected static final String PROP_KEY_USE_RT = "UseRT";
	protected static final String PROP_KEY_USE_LS = "UseLS";
	
	protected static final String PROP_KEY_USE_SECURE_ROUTING = "UseSecureRouting";
	
	protected static final String PROP_KEY_UPDATE_NETWORK_ADDRESS_WHEN_DIFFERENT = "UpdateNetworkAddressWhenDifferent";
	
	protected static final String PROP_KEY_RT_NODE_SELECTOR = "RTNodeSelector";
	protected static final String PROP_KEY_LS_NODE_SELECTOR = "LSNodeSelector";
	protected static final String PROP_KEY_SECURE_RT_NODE_SELECTOR = "SecureRTNodeSelector";
	
	
	protected static final int INITIAL_BEING_PROCESSED_SET_SIZE = 16;
	
	
	protected NodeId nodeId;
	protected long nodeIdHash;
	protected PastryRoutingTable routingTable;

	
	protected HyCubeNSNodeSelector lsNodeSelector;
	protected HyCubeRTNodeSelector rtNodeSelector;
	protected HyCubeRTNodeSelector secRTNodeSelector;
	
	protected boolean useSecureRouting;
	protected boolean useLS;
	protected boolean useRT;
	protected int lsSize;
	protected int dimensions;
	protected int digitsCount;
	protected int routingTableSlotSize;
	protected boolean updateNetworkAddressWhenDifferent;
	
	protected HashSet<Long> beingProcessed;
	
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		super.initialize(nodeAccessor, properties);
		
		initialize(nodeAccessor.getNodeId(), nodeAccessor.getRoutingTable(), properties);
		
	}
	
	
	public void initialize(NodeId nodeId, RoutingTable routingTable, NodeProperties properties) throws InitializationException {

		this.nodeId = nodeId;
		this.nodeIdHash = nodeId.calculateHash();
		this.routingTable = (PastryRoutingTable) routingTable;
		
		//parameters
		try {
			
			dimensions = (Integer) properties.getProperty(PROP_KEY_DIMENSIONS, MappedType.INT);
			if (dimensions <= 0) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize PastryNotifyProcessor instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_DIMENSIONS) + ".");
			}

			digitsCount = (Integer) properties.getProperty(PROP_KEY_LEVELS, MappedType.INT);
			if (digitsCount <= 0) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize PastryNotifyProcessor instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_LEVELS) + ".");
			}

			useLS = (Boolean) properties.getProperty(PROP_KEY_USE_LS, MappedType.BOOLEAN);

			useRT = (Boolean) properties.getProperty(PROP_KEY_USE_RT, MappedType.BOOLEAN);

			lsSize = (Integer) properties.getProperty(PROP_KEY_LS_SIZE, MappedType.INT);
			if (useLS && lsSize <= 0) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize PastryNotifyProcessor instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_LS_SIZE) + ".");
			}
			
			routingTableSlotSize = (Integer) properties.getProperty(PROP_KEY_ROUTING_TABLE_SLOT_SIZE, MappedType.INT);
			if (routingTableSlotSize <= 0) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize PastryNotifyProcessor instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_ROUTING_TABLE_SLOT_SIZE) + ".");
			}
			
			useSecureRouting = (Boolean) properties.getProperty(PROP_KEY_USE_SECURE_ROUTING, MappedType.BOOLEAN);
			
			updateNetworkAddressWhenDifferent = (Boolean) properties.getProperty(PROP_KEY_UPDATE_NETWORK_ADDRESS_WHEN_DIFFERENT, MappedType.BOOLEAN);


		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize PastryNotifyProcessor instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		try {
			String lsNodeSelectorKey = properties.getProperty(PROP_KEY_LS_NODE_SELECTOR);
			if (lsNodeSelectorKey == null || lsNodeSelectorKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_LS_NODE_SELECTOR), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_LS_NODE_SELECTOR));
			NodeProperties lsNodeSelectorProperties = properties.getNestedProperty(PROP_KEY_LS_NODE_SELECTOR, lsNodeSelectorKey);
			String lsNodeSelectorClass = lsNodeSelectorProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
			
			lsNodeSelector = (HyCubeNSNodeSelector) ClassInstanceLoader.newInstance(lsNodeSelectorClass, HyCubeNSNodeSelector.class);
			lsNodeSelector.initialize(nodeId, nodeAccessor, lsNodeSelectorProperties);
			
		} catch (ClassInstanceLoadException e) {
			throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create NS node selector class instance.", e);
		}

		try {
			String rtNodeSelectorKey = properties.getProperty(PROP_KEY_RT_NODE_SELECTOR);
			if (rtNodeSelectorKey == null || rtNodeSelectorKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_RT_NODE_SELECTOR), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_RT_NODE_SELECTOR));
			NodeProperties rtNodeSelectorProperties = properties.getNestedProperty(PROP_KEY_RT_NODE_SELECTOR, rtNodeSelectorKey);
			String rtNodeSelectorClass = rtNodeSelectorProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
			
			rtNodeSelector = (HyCubeRTNodeSelector) ClassInstanceLoader.newInstance(rtNodeSelectorClass, HyCubeRTNodeSelector.class);
			rtNodeSelector.initialize(nodeId, nodeAccessor, rtNodeSelectorProperties);
		} catch (ClassInstanceLoadException e) {
			throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create RT node selector class instance.", e);
		}

		if (useSecureRouting) {
			try {
				String secureRtNodeSelectorKey = properties.getProperty(PROP_KEY_SECURE_RT_NODE_SELECTOR);
				if (secureRtNodeSelectorKey == null || secureRtNodeSelectorKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_SECURE_RT_NODE_SELECTOR), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_SECURE_RT_NODE_SELECTOR));
				NodeProperties secureRtNodeSelectorProperties = properties.getNestedProperty(PROP_KEY_RT_NODE_SELECTOR, secureRtNodeSelectorKey);
				String secureRtNodeSelectorClass = secureRtNodeSelectorProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
				
				secRTNodeSelector = (HyCubeRTNodeSelector) ClassInstanceLoader.newInstance(secureRtNodeSelectorClass, HyCubeRTNodeSelector.class);
				secRTNodeSelector.initialize(nodeId, nodeAccessor, secureRtNodeSelectorProperties);
			} catch (ClassInstanceLoadException e) {
				throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create RT node selector class instance.", e);
			}
		}
		
		
		beingProcessed = new HashSet<Long>(HashMapUtils.getHashMapCapacityForElementsNum(INITIAL_BEING_PROCESSED_SET_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		
		
	}
	
	
	@Override
	public void processNotify(NodePointer newNode, long currTimestamp) {
		
		long newNodeIdHash = newNode.getNodeIdHash();
		
		synchronized (beingProcessed) {
			if (beingProcessed.contains(newNodeIdHash)) {
				if (devLog.isDebugEnabled()) {
					devLog.debug("Discarding notify info for :" + newNode.getNodeId().toHexString() + "/" + newNode.getNetworkNodePointer().getAddressString() +". The notify for the same node id is being processed.");
				}
				if (msgLog.isInfoEnabled()) {
					msgLog.info("Discarding notify info for :" + newNode.getNodeId().toHexString() + "/" + newNode.getNetworkNodePointer().getAddressString() +". The notify for the same node id is being processed.");
				}
				return;
			}
			else {
				beingProcessed.add(newNodeIdHash);
			}
		}
		
		
		try {
			processNotify(newNode, this.routingTable.getRoutingTable(), this.routingTable.getLeafSet(), this.routingTable.getRtMap(), this.routingTable.getLsMap(), this.routingTable.getRtLock(), this.routingTable.getLsLock(), rtNodeSelector, lsNodeSelector, currTimestamp);
			if (useSecureRouting) {
				processNotify(newNode, this.routingTable.getSecRoutingTable(), null, this.routingTable.getSecRtMap(), null, this.routingTable.getSecRtLock(), null, secRTNodeSelector, null, currTimestamp);
			}
		}
		finally {
			synchronized (beingProcessed) {
				beingProcessed.remove(newNodeIdHash);
			}
		}
				
	}
	
	
	
	
	protected void processNotify(NodePointer newNode, 
			List<RoutingTableEntry>[][] rt, List<RoutingTableEntry> ls,
			HashMap<Long, RoutingTableEntry> rtMap, HashMap<Long, RoutingTableEntry> lsMap,
			ReentrantReadWriteLock rtLock, ReentrantReadWriteLock lsLock,
			HyCubeRTNodeSelector rtNodeSelector, HyCubeNSNodeSelector lsNodeSelector, 
			long currTimestamp) {
		
		HyCubeNodeId nodeId = (HyCubeNodeId) this.nodeId;
		HyCubeNodeId newNodeId = (HyCubeNodeId) newNode.getNodeId();
		long newNodeIdHash = newNode.getNodeIdHash();
		String newNodeNetworkAddress = newNode.getNetworkNodePointer().getAddressString();
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Processing notify info for node " + newNodeId.toHexString() + "/" + newNode.getNetworkNodePointer().getAddressString() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing notify info for node " + newNodeId.toHexString() + "/" + newNode.getNetworkNodePointer().getAddressString() + ".");
		}

		if (nodeIdHash == newNodeIdHash) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("Discarding notify info for :" + newNodeId.toHexString() + "/" + newNode.getNetworkNodePointer().getAddressString() +". The new node id is the same as own node's id.");
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("Discarding notify info for :" + newNodeId.toHexString() + "/" + newNode.getNetworkNodePointer().getAddressString() +". The new node id is the same as own node's id.");
			}
			return;
		}
		
		
		try {
			
			routingTable.lockRoutingTableForRead();
						
			List<RoutingTableEntry> nodesWithSameHash = routingTable.getRoutingTableEntriesByNodeIdHash(newNodeIdHash);
			
			routingTable.unlockRoutingTableForRead();
			
			if (! nodesWithSameHash.isEmpty()) {
				if ((! nodesWithSameHash.get(0).getNode().getNodeId().equals(newNodeId))) {
					if (devLog.isDebugEnabled()) {
						devLog.debug("Discarding notify info for :" + newNodeId.toHexString() + "/" + newNode.getNetworkNodePointer().getAddressString() +". Another node with the same node id hash (and different node id) exists in routing tables.");
					}
					if (msgLog.isInfoEnabled()) {
						msgLog.info("Discarding notify info for :" + newNodeId.toHexString() + "/" + newNode.getNetworkNodePointer().getAddressString() +". Another node with the same node id hash (and different node id) exists in routing tables.");
					}
					return;
				}
				else if ((! nodesWithSameHash.get(0).getNode().getNetworkNodePointer().getAddressString().equals(newNodeNetworkAddress))) {
					//if there are node with the same node id in the routing table, but having different network address -> (update the network address in all references to that node or ignore the notify packet):
					if (updateNetworkAddressWhenDifferent) {
						for (RoutingTableEntry rte : nodesWithSameHash) {
							if (devLog.isDebugEnabled()) {
								devLog.debug("Updating the network address in all references to the node:" + newNodeId.toHexString() + "/" + newNode.getNetworkNodePointer().getAddressString() +".");
							}
							rte.getNode().setNetworkNodePointer(newNode.getNetworkNodePointer());
						}
					}
					else {
						if (devLog.isDebugEnabled()) {
							devLog.debug("Discarding notify info for :" + newNodeId.toHexString() + "/" + newNode.getNetworkNodePointer().getAddressString() +". Another node with the same node id and different network address exists in routing tables.");
						}
						if (msgLog.isInfoEnabled()) {
							msgLog.info("Discarding notify info for :" + newNodeId.toHexString() + "/" + newNode.getNetworkNodePointer().getAddressString() +". Another node with the same node id and different network address exists in routing tables.");
						}
						return;
					}
				}
			}
		}
		finally {
			
		}

		
		
		double dist = HyCubeNodeId.calculateRingDistance(nodeId, newNodeId);
		
		
		//LS:
		if (ls != null && useLS) {
			try {
				lsLock.writeLock().lock();
				
				if (!lsMap.containsKey(newNodeIdHash)) {				
					lsNodeSelector.processNode(newNode, ls, lsMap, lsSize, dist, currTimestamp);
				
				}
			}
			finally {
				lsLock.writeLock().unlock();
			}
		}
		
		
		//RT:
		if (rt != null && useRT) {

			boolean rtContainsNewNode;
			rtLock.readLock().lock();
			rtContainsNewNode = rtMap.containsKey(newNodeIdHash);
			rtLock.readLock().unlock(); 
			
			if (! rtContainsNewNode) {	//no other thread will change the rt2Map regarding this id hash -> exclusion ensured in processNotify(NodePointer, long)
			
				int commonPrefixLength = HyCubeNodeId.calculatePrefixLength(nodeId, newNodeId);
                int level = digitsCount - 1 - commonPrefixLength;
                int hypercube = newNodeId.getDigitAsInt(commonPrefixLength);
                
                try {
            		rtLock.writeLock().lock();
            		rtNodeSelector.processNode(newNode, rt[level][hypercube], rtMap, HyCubeRoutingTableType.RT1, level, hypercube, routingTableSlotSize, dist, currTimestamp);
            	}
            	finally {
            		rtLock.writeLock().unlock();
            	}
                

			}
		}
		
			
		
	}


	
	

}
