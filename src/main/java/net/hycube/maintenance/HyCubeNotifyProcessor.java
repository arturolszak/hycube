package net.hycube.maintenance;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.HyCubeNodeId;
import net.hycube.core.HyCubeRoutingTable;
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
import net.hycube.metric.Metric;
import net.hycube.rtnodeselection.HyCubeNSNodeSelector;
import net.hycube.rtnodeselection.HyCubeRTNodeSelector;
import net.hycube.utils.ClassInstanceLoadException;
import net.hycube.utils.ClassInstanceLoader;
import net.hycube.utils.HashMapUtils;
import net.hycube.utils.IntUtils;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public class HyCubeNotifyProcessor extends NotifyProcessor {

	
	public static class RecentlyProcessedNodeInfo {
		public long nodeIdHash;
		public long processTime;
		public RecentlyProcessedNodeInfo(long nodeIdHash, long processTime) {
			this.nodeIdHash = nodeIdHash;
			this.processTime = processTime;
		}
	}
	
	
//	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeNotifyProcessor.class);
	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
	
	
	protected static final String PROP_KEY_DIMENSIONS = "Dimensions";
	protected static final String PROP_KEY_LEVELS = "Levels";
	protected static final String PROP_KEY_NS_SIZE = "NSSize";
	protected static final String PROP_KEY_ROUTING_TABLE_SLOT_SIZE = "RoutingTableSlotSize";
	protected static final String PROP_KEY_USE_RT1 = "UseRT1";
	protected static final String PROP_KEY_USE_RT2 = "UseRT2";
	protected static final String PROP_KEY_USE_NS = "UseNS";
	
	protected static final String PROP_KEY_METRIC = "Metric";
	
	protected static final String PROP_KEY_EXCLUDE_RT2_SCOPE_FROM_RT1 = "ExcludeRT2ScopeFromRT1";

	protected static final String PROP_KEY_USE_SECURE_ROUTING = "UseSecureRouting";
	
	protected static final String PROP_KEY_UPDATE_NETWORK_ADDRESS_WHEN_DIFFERENT = "UpdateNetworkAddressWhenDifferent";
	
	protected static final String PROP_KEY_RT_NODE_SELECTOR = "RTNodeSelector";
	protected static final String PROP_KEY_NS_NODE_SELECTOR = "NSNodeSelector";
	protected static final String PROP_KEY_SECURE_RT_NODE_SELECTOR = "SecureRTNodeSelector";
	
	protected static final String PROP_KEY_RECENTLY_PROCESSED_NODES_RETENTION_TIME = "RecentlyProcessedNodesRetentionTime";
	protected static final String PROP_KEY_RECENTLY_PROCESSED_NODES_CACHE_MAX_SIZE = "RecentlyProcessedNodesCacheMaxSize";
	

	protected static final int INITIAL_BEING_PROCESSED_SET_SIZE = 16;

	
	protected NodeId nodeId;
	protected long nodeIdHash;
	protected HyCubeRoutingTable routingTable;

	
	protected HyCubeNSNodeSelector nsNodeSelector;
	protected HyCubeRTNodeSelector rtNodeSelector;
	protected HyCubeRTNodeSelector secRTNodeSelector;
	
	protected boolean useSecureRouting;
	protected Metric metric;
	protected boolean useNS;
	protected boolean useRT1;
	protected boolean useRT2;
	protected int nsSize;
	protected int dimensions;
	protected int digitsCount;
	protected boolean excludeRT2ScopeFromRT1;
	protected int routingTableSlotSize;
	protected boolean updateNetworkAddressWhenDifferent;
	
	protected HashSet<Long> beingProcessed;

	
	protected int recentlyProcessedNodesRetentionTime;
	protected int recentlyProcessedNodesCacheMaxSize;
	
	
	//hash map of recently processed nodes
    protected HashSet<Long> recentlyProcessed;
    
    //ordered list (by discard time) of recently processed nodes
    protected LinkedList<RecentlyProcessedNodeInfo> recentlyProcessedOrdered;
    
	

	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		super.initialize(nodeAccessor, properties);
		
		initialize(nodeAccessor.getNodeId(), nodeAccessor.getRoutingTable(), properties);
		
	}
	
	
	public void initialize(NodeId nodeId, RoutingTable routingTable, NodeProperties properties) throws InitializationException {
		

		this.nodeId = nodeId;
		this.nodeIdHash = nodeId.calculateHash();
		
		if (! (routingTable instanceof HyCubeRoutingTable)) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize HyCubeNotifyProcessor instance. The routing table is expected to be an instance of: " + HyCubeRoutingTable.class.getName());
		}
		this.routingTable = (HyCubeRoutingTable) routingTable;
		
		
		//parameters
		try {
						
			dimensions = (Integer) properties.getProperty(PROP_KEY_DIMENSIONS, MappedType.INT);
			if (dimensions <= 0) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize HyCubeNotifyProcessor instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_DIMENSIONS) + ".");
			}
			
			digitsCount = (Integer) properties.getProperty(PROP_KEY_LEVELS, MappedType.INT);
			if (digitsCount <= 0) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize HyCubeNotifyProcessor instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_LEVELS) + ".");
			}
			
			
			metric = (Metric) properties.getEnumProperty(PROP_KEY_METRIC, Metric.class);
			
			useNS = (Boolean) properties.getProperty(PROP_KEY_USE_NS, MappedType.BOOLEAN);
			
			useRT1 = (Boolean) properties.getProperty(PROP_KEY_USE_RT1, MappedType.BOOLEAN);
			
			useRT2 = (Boolean) properties.getProperty(PROP_KEY_USE_RT2, MappedType.BOOLEAN);
			
			nsSize = (Integer) properties.getProperty(PROP_KEY_NS_SIZE, MappedType.INT);
			if (useNS && nsSize <= 0) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize HyCubeNotifyProcessor instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_NS_SIZE) + ".");
			}
			
			routingTableSlotSize = (Integer) properties.getProperty(PROP_KEY_ROUTING_TABLE_SLOT_SIZE, MappedType.INT);
			if (routingTableSlotSize <= 0) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize HyCubeNotifyProcessor instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_ROUTING_TABLE_SLOT_SIZE) + ".");
			}
			
			excludeRT2ScopeFromRT1 = (Boolean) properties.getProperty(PROP_KEY_EXCLUDE_RT2_SCOPE_FROM_RT1, MappedType.BOOLEAN);
			
			useSecureRouting = (Boolean) properties.getProperty(PROP_KEY_USE_SECURE_ROUTING, MappedType.BOOLEAN);
			
			updateNetworkAddressWhenDifferent = (Boolean) properties.getProperty(PROP_KEY_UPDATE_NETWORK_ADDRESS_WHEN_DIFFERENT, MappedType.BOOLEAN);

			recentlyProcessedNodesRetentionTime = (Integer) properties.getProperty(PROP_KEY_RECENTLY_PROCESSED_NODES_RETENTION_TIME, MappedType.INT);
			recentlyProcessedNodesCacheMaxSize = (Integer) properties.getProperty(PROP_KEY_RECENTLY_PROCESSED_NODES_CACHE_MAX_SIZE, MappedType.INT);
			
			recentlyProcessed = new HashSet<Long>(HashMapUtils.getHashMapCapacityForElementsNum(GlobalConstants.DEFAULT_INITIAL_COLLECTION_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		    recentlyProcessedOrdered = new LinkedList<RecentlyProcessedNodeInfo>();
		    
			
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize HyCubeNotifyProcessor instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		
		try {
			String nsNodeSelectorKey = properties.getProperty(PROP_KEY_NS_NODE_SELECTOR);
			if (nsNodeSelectorKey == null || nsNodeSelectorKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_NS_NODE_SELECTOR), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_NS_NODE_SELECTOR));
			NodeProperties nsNodeSelectorProperties = properties.getNestedProperty(PROP_KEY_NS_NODE_SELECTOR, nsNodeSelectorKey);
			String nsNodeSelectorClass = nsNodeSelectorProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
			
			nsNodeSelector = (HyCubeNSNodeSelector) ClassInstanceLoader.newInstance(nsNodeSelectorClass, HyCubeNSNodeSelector.class);
			nsNodeSelector.initialize(nodeId, nodeAccessor, nsNodeSelectorProperties);
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
		
		
		
		
		if (recentlyProcessedNodesRetentionTime > 0) {
			//check if the node was processed recently. if so, skip the node
			if (wasNodeRecentlyProcessed(newNodeIdHash)) {
				return;
			}
	
			//else save the node as recently processed and process it
			saveRecentlyProcessedNode(newNodeIdHash);
		
		}
		
		
		
		try {
			processNotify(newNode, this.routingTable.getRoutingTable1(), this.routingTable.getRoutingTable2(), this.routingTable.getNeighborhoodSet(), this.routingTable.getRt1Map(), this.routingTable.getRt2Map(), this.routingTable.getNsMap(), this.routingTable.getRt1Lock(), this.routingTable.getRt2Lock(), this.routingTable.getNsLock(), this.rtNodeSelector, this.nsNodeSelector, currTimestamp);
			if (useSecureRouting) {
				processNotify(newNode, this.routingTable.getSecRoutingTable1(), this.routingTable.getSecRoutingTable2(), null, this.routingTable.getSecRt1Map(), this.routingTable.getSecRt2Map(), null, this.routingTable.getSecRt1Lock(), this.routingTable.getSecRt2Lock(), null, this.secRTNodeSelector, null, currTimestamp);
			}
		}
		finally {
			synchronized (beingProcessed) {
				beingProcessed.remove(newNodeIdHash);
			}
		}
		
	}
	

	
	protected void processNotify(NodePointer newNode, 
			List<RoutingTableEntry>[][] rt1, List<RoutingTableEntry>[][] rt2, List<RoutingTableEntry> ns,
			HashMap<Long, RoutingTableEntry> rt1Map, HashMap<Long, RoutingTableEntry> rt2Map, HashMap<Long, RoutingTableEntry> nsMap,
			ReentrantReadWriteLock rt1Lock, ReentrantReadWriteLock rt2Lock, ReentrantReadWriteLock nsLock,
			HyCubeRTNodeSelector rtNodeSelector, HyCubeNSNodeSelector nsNodeSelector, 
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

		
		
		double dist = HyCubeNodeId.calculateDistance(nodeId, newNodeId, metric);
		
		HyCubeNodeId xor = HyCubeNodeId.xorIDs(nodeId, newNodeId);
		
		
		//NS:
		if (ns != null && useNS) {
			
			try {
				nsLock.writeLock().lock();
				
				
				if (!nsMap.containsKey(newNodeIdHash)) {				
					nsNodeSelector.processNode(newNode, ns, nsMap, nsSize, dist, currTimestamp);	
				}
			}
			finally {
				nsLock.writeLock().unlock();
			}
				
		}

		
		
		//RT1:
		if (rt1 != null && (useRT1 || useRT2)) {	//RT1 should be updated if RT1 or RT2 is enabled, but if only RT2 is enabled, only sibling hypercubes are used in RT1 and next hop selection is influenced
			
			boolean rt1ContainsNewNode;
			rt1Lock.readLock().lock();
			rt1ContainsNewNode = rt1Map.containsKey(newNodeIdHash);
			rt1Lock.readLock().unlock(); 
			
			if (! rt1ContainsNewNode) {	//no other thread will change the rt2Map regarding this id hash -> exclusion ensured in processNotify(NodePointer, long)
				
				int commonPrefixLength = HyCubeNodeId.calculatePrefixLength(nodeId, newNodeId);
                int level = digitsCount - 1 - commonPrefixLength;
                int hypercube = newNodeId.getDigitAsInt(commonPrefixLength);
                
                boolean exclude = false;
                
                //if RT1 is disabled (and RT2 is enabled), only sibling hypercubes slots are used
                if (useRT1 == false /* && useRT2*/) {
                	int xorDigitAtLevel = xor.getDigitAsInt(commonPrefixLength);
                	if (IntUtils.getSetBitNumber(xorDigitAtLevel, dimensions) < 0) {	//this means that this is not the sibling hypercube (-1 cannot be returned - the node is in a different hypercube, -2 means that two or more bits are different, so not adjacent)
                		exclude = true;
                	}
                }
                
                if (exclude == false && useRT2 && excludeRT2ScopeFromRT1) {	
                	//if not yet excluded and RT2 is used (parameter) and excludeRT2ScoprFromRT1 parameter is set,
                	//check if the node is within the scope of a RT2 slot
                	
                	//perform xor on nodeId and newNodeId -> the result will be a NodeId with bits set to 0 where corresponding nodeId and newNodeId bits are equal, and 1 when not equal
    				//nodeId and newNodeId are in sibling hypercubes at levels maxLevels-j to maxLevels-i <=> all k<i first digits will be equal in the ids and digits i to j differ on one bit (the same bit in all digits) - the bit is the dimension in which the two hypercubes are adjacent
                	//if calculated maxLevels-j is less than "level", this means that newNodeId is located in an adjacent hypercube at lower level than "level", and is covered by RT2
    				//also, there might be a case that newNodeId is not in an adjacent hypercube in any dimension on any level
                	
                	
					
    				int digitChecked = -1;
    				int dim = -1;
    				for (int i = 0; i < digitsCount; i++) {
    					int digit = xor.getDigitAsInt(i);
    					int bitSetNo = IntUtils.getSetBitNumber(digit, dimensions);
    					if (bitSetNo == -2) break;	//more than one bit set to 1
    					else if (bitSetNo == -1 && dim == -1) digitChecked = i;
    					else if (bitSetNo == -1 && dim != -1) break;
    					else if (bitSetNo >= 0 && dim != -1 && dim != bitSetNo) break;
    					else {
    						digitChecked = i;
    						dim = bitSetNo;
    						int siblingOnLevel = digitsCount - 1 - digitChecked;
    						if (siblingOnLevel < level) {
    							//do not add the node to RT1 - the node is covered by RT2 at a lower level
    							//this node is in a sibling hypercube at lower level than "level", which means that it is covered by RT2 at lower level (RT1 lowest level for this node is "level")
    							exclude = true;
    							break;
    						}
    					}
    				}
                	
                }
                
                if (!exclude) {
                	try {
                		rt1Lock.writeLock().lock();
                		rtNodeSelector.processNode(newNode, rt1[level][hypercube], rt1Map, HyCubeRoutingTableType.RT1, level, hypercube, routingTableSlotSize, dist, currTimestamp);
                	}
                	finally {
                		rt1Lock.writeLock().unlock();
                	}
                }
            }
		}

		
		
		
		//RT2:
		if (rt2 != null && useRT2) {

			boolean rt2ContainsNewNode;
			rt2Lock.readLock().lock();
			rt2ContainsNewNode = rt2Map.containsKey(newNodeIdHash);
			rt2Lock.readLock().unlock(); 
			
			if (! rt2ContainsNewNode) {	//no other thread will change the rt2Map regarding this id hash -> exclusion ensured in processNotify(NodePointer, long)
				
				//check if newNodeId is in a sibling hypercube in some dimension dim (there will be two hypercubes, but one is always covered by RT1)
				
				//perform xor on nodeId and newNodeId -> the result will be a NodeId with bits set to 0 where corresponding nodeId and newNodeId bits are equal, and 1 when not equal
				//nodeId and newNodeId are in sibling hypercubes at levels maxLevels-j to maxLevels-i <=> all k<i first digits will be equal in the ids and digits i to j differ on one bit (the same bit in all digits) - the bit is the dimension in which the two hypercubes are adjacent
				//by calculating i, j and dimension there is no need to check all sibling hypercubes on all levels in different dimensions
				//having calculated i, j and dimension, there are only a few checks (j-i) whether the sibling hypercube in which the newNodeId is located is already covered by RT1 or not
				//also, there might be a case that newNodeId is not in an adjacent hypercube in any dimension on any level
				
				int digitChecked = -1;
				int firstDifferentDigit = -1;
				int dim = -1;
				for (int i = 0; i < digitsCount; i++) {
					int digit = xor.getDigitAsInt(i);
					int bitSetNo = IntUtils.getSetBitNumber(digit, dimensions);
					if (bitSetNo == -2) break;	//more than one bit set to 1
					else if (bitSetNo == -1 && dim == -1) digitChecked = i;
					else if (bitSetNo == -1 && dim != -1) break;
					else if (bitSetNo >= 0 && dim != -1 && dim != bitSetNo) break;
					else {
						digitChecked = i;
						dim = bitSetNo;
						if (firstDifferentDigit == -1) firstDifferentDigit = i;
					}
				}
				
				int level = -1;
				if (dim >= 0) {
					
					dim = dimensions - 1 - dim;
	
					int minLevel = digitsCount - 1 - digitChecked;
					int maxLevel = digitsCount - 1 - firstDifferentDigit;
					if ((maxLevel == digitsCount - 1) && useRT1) {
						maxLevel--;		//skip the highest level (the same hypercubes are covered by RT1 on the highest level)
					}
				
					for (int i = minLevel; i <= maxLevel; i++) {
						//check if the node is in the sibling hypercube at level i, and dimension dim; if so, this is the rt2 slot for the node
						HyCubeNodeId hypercube1 = nodeId.addBitInDimension(dim, digitsCount - i - 1);
		                HyCubeNodeId hypercube2 = nodeId.subBitInDimension(dim, digitsCount - i - 1);
		                HyCubeNodeId hypercubePrefix;
						
		                if (digitsCount == i + 1 || HyCubeNodeId.compareIds(hypercube1.getSubID(0, digitsCount - i - 1), nodeId.getSubID(0, digitsCount - i - 1))) {
		                    //the RT2 slot represents hypercube2; hycercube1 is represented by a RT1 slot
		                    hypercubePrefix = hypercube2.getSubID(0, digitsCount - i);
		                }
		                else {
		                    //the RT2 slot represents hypercube1; hypercube2 is represented by a RT1 slot
		                    hypercubePrefix = hypercube1.getSubID(0, digitsCount - i);
		                }
		                
		                if (HyCubeNodeId.startsWith(newNodeId, hypercubePrefix)) {
		                	//i - level, dim - dimension for the node
		                	level = i;
		                	break;
		                }
		                else {
		                	//do nothing, this node is in the other sibling hypercube (covered by rt1), continue to check the higher levels
		                }
					}
	                
				}
				
				if (level != -1) {
					try {
						rt2Lock.writeLock().lock();
						rtNodeSelector.processNode(newNode, rt2[level][dim], rt2Map, HyCubeRoutingTableType.RT2, level, dim, routingTableSlotSize, dist, currTimestamp);
					}
					finally {
						rt2Lock.writeLock().unlock();
					}
				}

				
			}	
		}
		
		
	}


	
	
	
	public void saveRecentlyProcessedNode(long nodeIdHash) {
		
		long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();

		synchronized (recentlyProcessed) {
			if (! recentlyProcessed.contains(nodeIdHash)) {
				RecentlyProcessedNodeInfo rpni = new RecentlyProcessedNodeInfo(nodeIdHash, currTime);
				recentlyProcessed.add(nodeIdHash);
				recentlyProcessedOrdered.add(rpni);
				while (recentlyProcessed.size() > recentlyProcessedNodesCacheMaxSize) {
					RecentlyProcessedNodeInfo removed = recentlyProcessedOrdered.removeFirst();
					recentlyProcessed.remove(removed.nodeIdHash);
				}
			}
		}
		
	}
	
	
	
	public boolean wasNodeRecentlyProcessed(long nodeIdHash) {
		
		clearRecentlyProcessedNodes();
		synchronized (recentlyProcessed) {
			return recentlyProcessed.contains(nodeIdHash);
		}
		
	}
	
	
	public void clearRecentlyProcessedNodes() {
		long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
		
		synchronized (recentlyProcessed) {
			ListIterator<RecentlyProcessedNodeInfo> iter = recentlyProcessedOrdered.listIterator();
			while (iter.hasNext()) {
				RecentlyProcessedNodeInfo rpni = iter.next();
				if (rpni.processTime + recentlyProcessedNodesRetentionTime <= currTime) {
					//discard
					iter.remove();
					recentlyProcessed.remove(rpni.nodeIdHash);
				}
				else {
					break;
				}
			}
		}
		
	}



}
