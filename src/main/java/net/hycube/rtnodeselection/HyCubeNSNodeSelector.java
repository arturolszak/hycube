package net.hycube.rtnodeselection;

import java.util.HashMap;
import java.util.List;

import net.hycube.core.HyCubeRoutingTableSlotInfo;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodeId;
import net.hycube.core.NodePointer;
import net.hycube.core.RoutingTableEntry;
import net.hycube.environment.NodeProperties;

public abstract class HyCubeNSNodeSelector {
	
	protected NodeId nodeId;
	protected NodeProperties properties;
	protected NodeAccessor nodeAccessor;
	
	
	protected boolean initialized = false;
	
	public void initialize(NodeId nodeId, NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		this.nodeId = nodeId;
		this.properties = properties;
		this.nodeAccessor = nodeAccessor;
		initialized = true;
	}
	
	protected boolean isInitialized() {
		return initialized;
	}
	
	
	public abstract void processNode(NodePointer newNode,
			List<RoutingTableEntry> ns, HashMap<Long, RoutingTableEntry> nsMap,
			int nsSize,
			double dist, long currTimestamp);	
	
	
	
	protected RoutingTableEntry initializeRoutingTableEntry(NodePointer newNode, double distance, long currTimestamp, HyCubeRoutingTableSlotInfo outerRef) {
		return RoutingTableEntry.initializeRoutingTableEntry(newNode, distance, currTimestamp, outerRef);
	}
	
	
}
