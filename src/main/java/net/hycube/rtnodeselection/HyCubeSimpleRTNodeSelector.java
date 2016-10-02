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

public class HyCubeSimpleRTNodeSelector extends HyCubeRTNodeSelector {

	
	@Override
	public void initialize(NodeId nodeId, NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		super.initialize(nodeId, nodeAccessor, properties);
		
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
        	rtMap.put(newNode.getNodeIdHash(), rte);
        	routingTableSlot.add(rte);
        	return;
		}
		else {
			//do nothing, leave the existing nodes
			return;
		}
		
	}

}
