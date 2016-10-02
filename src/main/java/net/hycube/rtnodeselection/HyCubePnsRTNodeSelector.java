package net.hycube.rtnodeselection;

import java.util.HashMap;
import java.util.List;

import net.hycube.core.HyCubeRoutingTableSlotInfo;
import net.hycube.core.HyCubeRoutingTableType;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodeId;
import net.hycube.core.NodePointer;
import net.hycube.core.RoutingTableEntry;
import net.hycube.environment.NodeProperties;

public class HyCubePnsRTNodeSelector extends HyCubeRTNodeSelector {


	@Override
	public void initialize(NodeId nodeId, NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		super.initialize(nodeId, nodeAccessor, properties);
		
		//parameters
//		try {
			
//		} catch (NodePropertiesConversionException e) {
//			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize rt node selector instance. Invalid parameter value: " + e.getKey() + ".", e);
//		}
		
	}
	
	
	@Override
	public void processNode(NodePointer newNode, 
			List<RoutingTableEntry> routingTableSlot, HashMap<Long, RoutingTableEntry> rtMap, 
			HyCubeRoutingTableType rtType, int level, int slotNo, 
			int routingTableSlotSize,
			double dist, long currTimestamp) {
		
		if (routingTableSlot.size() < routingTableSlotSize) {
			HyCubeRoutingTableSlotInfo slotInfo = new HyCubeRoutingTableSlotInfo(rtType, rtMap, routingTableSlot);
			RoutingTableEntry rte = initializeRoutingTableEntry(newNode, dist, currTimestamp, slotInfo);
        	//rte.setData(PNS_DATA...)
        	rtMap.put(newNode.getNodeIdHash(), rte);
        	return;
		}
		else {
			int worstNodeIndex = -1;
			
			//find worst node:
	        double worstFun = 0;
	        for (int i = 0; i < routingTableSlot.size(); i++) {
	        	//RoutingTableEntry neigh = routingTableSlot.get(i);
	        	//PNS_DATA = neigh.getData(...);
	        	//...
	            double fun = 0;		//calculate delay!!!???
	            if (worstNodeIndex == -1 || fun > worstFun) {
	                worstFun = fun;
	                worstNodeIndex = i;
	            }
	        }
	
	        //check if new node is better than current node in rt
	        double newFun = 0;		//calculate delay!!!
	        if (newFun < worstFun) {
	        	//replace the worst node with the new node:
	        	HyCubeRoutingTableSlotInfo slotInfo = new HyCubeRoutingTableSlotInfo(rtType, rtMap, routingTableSlot);
	        	RoutingTableEntry rte = initializeRoutingTableEntry(newNode, dist, currTimestamp, slotInfo);
	        	rtMap.remove(routingTableSlot.get(worstNodeIndex).getNodeIdHash());
	        	routingTableSlot.set(worstNodeIndex, rte);
	        	rtMap.put(newNode.getNodeIdHash(), rte);
	            
	        }
		}
		
	}

}
