package net.hycube.rtnodeselection;

import java.util.HashMap;
import java.util.List;

import net.hycube.core.HyCubeNodeId;
import net.hycube.core.HyCubeRoutingTableSlotInfo;
import net.hycube.core.HyCubeRoutingTableType;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodeId;
import net.hycube.core.NodePointer;
import net.hycube.core.RoutingTableEntry;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.environment.NodeProperties;
import net.hycube.metric.Metric;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public class HyCubeSecureRTNodeSelector extends HyCubeRTNodeSelector {
	
	protected static final String PROP_KEY_METRIC = "Metric";
	protected static final String PROP_KEY_DIMENSIONS = "Dimensions";
	protected static final String PROP_KEY_LEVELS = "Levels";
	protected static final String PROP_KEY_XOR_NODE_ID_CHANGE_AFTER = "XorNodeIdChangeAfter";
	protected static final String PROP_KEY_DIST_FUN_RTE_KEY = "DistFunRteKey";
	
	protected HyCubeNodeId secXorNodeId;
	protected int counter;
	protected Metric metric;
	protected int dimensions;
	protected int digitsCount;
	protected int secureRTNodeSelectorXorNodeIdChangeAfter;
	protected String distFunRteKey;
	
	
	
	@Override
	public void initialize(NodeId nodeId, NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		super.initialize(nodeId, nodeAccessor, properties);
		
		//parameters
		try {
			
			metric = (Metric) properties.getEnumProperty(PROP_KEY_METRIC, Metric.class);
			
			dimensions = (Integer) properties.getProperty(PROP_KEY_DIMENSIONS, MappedType.INT);
			
			digitsCount = (Integer) properties.getProperty(PROP_KEY_LEVELS, MappedType.INT);
			
			secureRTNodeSelectorXorNodeIdChangeAfter = (Integer) properties.getProperty(PROP_KEY_XOR_NODE_ID_CHANGE_AFTER, MappedType.INT);
			
			distFunRteKey = properties.getProperty(PROP_KEY_DIST_FUN_RTE_KEY);
			if (distFunRteKey == null || distFunRteKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_DIST_FUN_RTE_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_DIST_FUN_RTE_KEY));
			
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize rt node instance. Invalid parameter value: " + e.getKey() + ".", e);
		}

		secXorNodeId = HyCubeNodeId.generateRandomNodeId(dimensions, digitsCount);
				
		resetCounter();
		
	}
	

	public int getCounter() {
		synchronized(this) {
			return counter;
		}
	}
	
	public void resetCounter() {
		synchronized(this) {
			counter = 0;
		}
	}
	
	protected void resetSecXorNodeId() {
		secXorNodeId = HyCubeNodeId.generateRandomNodeId(dimensions, digitsCount);
		resetCounter();
	}
	

	@Override
	public void processNode(NodePointer newNode, 
			List<RoutingTableEntry> routingTableSlot, HashMap<Long, RoutingTableEntry> rtMap, 
			HyCubeRoutingTableType rtType, int level, int slotNo, 
			int routingTableSlotSize,
			double dist, long currTimestamp) {
		
		HyCubeNodeId nodeId = (HyCubeNodeId) this.nodeId;
		HyCubeNodeId newNodeId = (HyCubeNodeId) newNode.getNodeId();
		
		if (secureRTNodeSelectorXorNodeIdChangeAfter > 0 && getCounter() >= secureRTNodeSelectorXorNodeIdChangeAfter) resetSecXorNodeId();
		
        int startDigit = (digitsCount - level);
        int length = nodeId.getDigitsCount() - startDigit;
		
		HyCubeNodeId nodeIdSecXored = HyCubeNodeId.xorIDs(newNodeId, secXorNodeId);
		double newDistFun = HyCubeNodeId.calculateDistance(nodeIdSecXored.getSubID(startDigit, length), nodeId.getSubID(startDigit, length), metric);
		
		if (routingTableSlot.size() < routingTableSlotSize) {
			HyCubeRoutingTableSlotInfo slotInfo = new HyCubeRoutingTableSlotInfo(rtType, rtMap, routingTableSlot);
			RoutingTableEntry rte = initializeRoutingTableEntry(newNode, dist, currTimestamp, slotInfo);
        	rte.setData(distFunRteKey, newDistFun);
        	rtMap.put(newNode.getNodeIdHash(), rte);
        	routingTableSlot.add(rte);
        	return;
		}
		else {
			
			//find worst node:
			int worstNodeIndex = -1;
	        
	        double worstDistFun = 0;
	        for (int i = 0; i < routingTableSlot.size(); i++) {
	        	RoutingTableEntry neigh = routingTableSlot.get(i);
	            double distFun = (Double) neigh.getData(distFunRteKey, -1);
	            if (distFun == -1) {
					//if all routing table entries are initialized by this class, this should never happen
	            	HyCubeNodeId neighNodeIdSecXored = HyCubeNodeId.xorIDs((HyCubeNodeId) neigh.getNode().getNodeId(), secXorNodeId);
	        		distFun = HyCubeNodeId.calculateDistance(neighNodeIdSecXored.getSubID(startDigit, length), nodeId.getSubID(startDigit, length), metric);
					neigh.setData(distFunRteKey, distFun);
				}
	            if (worstNodeIndex == -1 || distFun > worstDistFun) {
	                worstDistFun = distFun;
	                worstNodeIndex = i;
	            }
	        }
	
	        
	        //check if new node is better than current node in rt
	        if (newDistFun < worstDistFun) {
	        	//replace the worst node with the new node:
	        	HyCubeRoutingTableSlotInfo slotInfo = new HyCubeRoutingTableSlotInfo(rtType, rtMap, routingTableSlot);
	        	RoutingTableEntry rte = initializeRoutingTableEntry(newNode, dist, currTimestamp, slotInfo);
	        	rte.setData(distFunRteKey, newDistFun);
	        	rtMap.remove(routingTableSlot.get(worstNodeIndex).getNodeIdHash());
	        	routingTableSlot.set(worstNodeIndex, rte);
	        	rtMap.put(newNode.getNodeIdHash(), rte);
	        }
		}

	}



}
