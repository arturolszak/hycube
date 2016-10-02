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

public class HyCubeDistanceNSNodeSelector extends HyCubeNSNodeSelector {

	
	@Override
	public void initialize(NodeId nodeId, NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		super.initialize(nodeId, nodeAccessor, properties);
		
		//parameters
//		try {
			
			
//		} catch (NodePropertiesConversionException e) {
//			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize NS node selector instance. Invalid parameter value: " + e.getKey() + ".", e);
//		}
		
	}
	
	@Override
	public void processNode(NodePointer newNode,
			List<RoutingTableEntry> ns, HashMap<Long, RoutingTableEntry> nsMap,
			int nsSize,
			double dist, long currTimestamp) {
		
		if (ns.size() < nsSize) {
			HyCubeRoutingTableSlotInfo slotInfo = new HyCubeRoutingTableSlotInfo(HyCubeRoutingTableType.NS, nsMap, ns);
			RoutingTableEntry rte = initializeRoutingTableEntry(newNode, dist, currTimestamp, slotInfo);
			ns.add(rte);
			nsMap.put(newNode.getNodeIdHash(), rte);
		}
		else {
			//check if the new node is closer than the most distant neighbor. if so, replace the neighbor with the new one:
			RoutingTableEntry mostDistNode = null;
			int index = -1;
			for (int i = 0; i < ns.size(); i++) {
				RoutingTableEntry rte = ns.get(i);
				if (mostDistNode == null || rte.getDistance() > mostDistNode.getDistance()) {
					mostDistNode = rte;
					index = i;
				}
			}
			if (dist < mostDistNode.getDistance()) {
				//replace the node with the new one: 
				HyCubeRoutingTableSlotInfo slotInfo = new HyCubeRoutingTableSlotInfo(HyCubeRoutingTableType.NS, nsMap, ns);
				RoutingTableEntry rte = initializeRoutingTableEntry(newNode, dist, currTimestamp, slotInfo);
				ns.set(index, rte);
				nsMap.remove(mostDistNode.getNodeIdHash());
				nsMap.put(newNode.getNodeIdHash(), rte);
			}
		}
		
	}

}
