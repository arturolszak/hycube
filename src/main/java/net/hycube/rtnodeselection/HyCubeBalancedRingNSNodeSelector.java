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
import net.hycube.environment.NodeProperties;

public class HyCubeBalancedRingNSNodeSelector extends HyCubeNSNodeSelector {

	
	protected static final String PROP_KEY_SEMIRING_NO_RTE_KEY = "SemiringNoRteKey";
	
	protected String semiringNoRteKey;
	
	@Override
	public void initialize(NodeId nodeId, NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		super.initialize(nodeId, nodeAccessor, properties);
		
		//parameters
//		try {
			
			semiringNoRteKey = properties.getProperty(PROP_KEY_SEMIRING_NO_RTE_KEY);
			if (semiringNoRteKey == null || semiringNoRteKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_SEMIRING_NO_RTE_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_SEMIRING_NO_RTE_KEY));
			
		
//		} catch (NodePropertiesConversionException e) {
//			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize NS node selector instance instance. Invalid parameter value: " + e.getKey() + ".", e);
//		}
		
	}
	
	
	
	@Override
	public void processNode(NodePointer newNode,
			List<RoutingTableEntry> ns, HashMap<Long, RoutingTableEntry> nsMap,
			int nsSize,
			double dist, long currTimestamp) {
		
		HyCubeNodeId nodeId = (HyCubeNodeId)this.nodeId;
		
		//for BalancedOrthants algorithm, NS is ordered by distance to neighbors ascending
		if (ns.size() < nsSize) {
			HyCubeRoutingTableSlotInfo slotInfo = new HyCubeRoutingTableSlotInfo(HyCubeRoutingTableType.NS, nsMap, ns);
			RoutingTableEntry rte = initializeRoutingTableEntry(newNode, dist, currTimestamp, slotInfo);
			int index = ns.size();
			for (int i = 0; i < ns.size(); i++) {
				if (ns.get(i).getDistance() < dist) continue;
				else {
					index = i;
					break;
				}
			}
			ns.add(index, rte);
			nsMap.put(newNode.getNodeIdHash(), rte);
			int o = HyCubeNodeId.getSemiringNo(nodeId, (HyCubeNodeId) newNode.getNodeId());
			rte.setData(semiringNoRteKey, o);
		}
		else {
			int orthantsCount = 2;	//ring
			
			int[] orthantNeighCounts = new int[orthantsCount];
			int[] orthantMaxDistNeighIndexes = new int[orthantsCount];
			
			for (int o = 0; o < orthantsCount; o++) {
				orthantMaxDistNeighIndexes[o] = -1;
			}
			
			//get neighbors' counts in orthants and most distant neighbors' NS indexes
			int maxOrthNodesCount = 0;
			for (int i = 0; i < ns.size(); i++) {
				RoutingTableEntry rte = ns.get(i);
				int o = (Integer) rte.getData(semiringNoRteKey, -1);
				if (o == -1) {
					//if all routing table entries are initialized by this class, this should never happen
					o = HyCubeNodeId.getSemiringNo(nodeId, (HyCubeNodeId) rte.getNode().getNodeId());
					rte.setData(semiringNoRteKey, o);
				}
				orthantNeighCounts[o] = orthantNeighCounts[o] + 1;
				//assuming that ns is ordered by distance:
				orthantMaxDistNeighIndexes[o] = i;
				//update the maximum count of neighbors in orthants
				if (maxOrthNodesCount < orthantNeighCounts[o]) maxOrthNodesCount = orthantNeighCounts[o]; 
			}
			
			int o = HyCubeNodeId.getSemiringNo(nodeId, (HyCubeNodeId) newNode.getNodeId());
			
			
			
			if (orthantNeighCounts[o] == maxOrthNodesCount) {
				//if the number of nodes in the orthant o is the maximum number of nodes among all orthnants, replace the most distant node in the orthant o
				RoutingTableEntry mostDistantNode = ns.get(orthantMaxDistNeighIndexes[o]);
				if (mostDistantNode.getDistance() > dist) {
					HyCubeRoutingTableSlotInfo slotInfo = new HyCubeRoutingTableSlotInfo(HyCubeRoutingTableType.NS, nsMap, ns);
					RoutingTableEntry rte = initializeRoutingTableEntry(newNode, dist, currTimestamp, slotInfo);
					rte.setData(semiringNoRteKey, o);
					ns.set(orthantMaxDistNeighIndexes[o], rte);
					//move the new node left (it is closer) to the appropriate place to keep the NS orderd by distance
					int ind = orthantMaxDistNeighIndexes[o];
					while (ind > 0 && dist < ns.get(ind-1).getDistance()) {
						ns.set(ind, ns.get(ind-1));
						ns.set(ind-1, rte);
						ind--;
					}
					nsMap.remove(mostDistantNode.getNodeIdHash());
					nsMap.put(newNode.getNodeIdHash(), rte);
				}
			}
			else {
				//if the number of nodes in the orthant o is less than the maximum number of nodes among all orthants, find the most distant orthant in orthants containing the maximum number nodes and replace this node with the new node
				int worstNodeIndex = 0;
				double worstNodeDistance = 0;
				for (int or = 0; or < orthantsCount; or++) {
					if (orthantNeighCounts[or] == maxOrthNodesCount) {
						int ind = orthantMaxDistNeighIndexes[or];
						double d = ns.get(ind).getDistance();
						if (d > worstNodeDistance) {
							worstNodeIndex = ind;
							worstNodeDistance = d;
						}
					}
				}
				boolean replace;
				if (orthantNeighCounts[o] == maxOrthNodesCount - 1) {
					//check if the new node is closer than the worst node in the orthants with maximum nuber of nodes
					if (worstNodeDistance > dist) replace = true;
					else {
						//then check the worst node in the new node's orthants and replace it with the new one if it is more distant:
						worstNodeIndex = orthantMaxDistNeighIndexes[o];
						if (worstNodeIndex != -1) {
							double d = ns.get(worstNodeIndex).getDistance();
							if (dist < d) {
								worstNodeDistance = d;
								replace = true;
							}
							else {
								replace = false;
							}
						}
						else {
							replace = false;
						}
					}
				}
				else { // (orthantNeighCounts[o] < maxOrthNodesCount - 1)
					replace = true;
				}
				if (replace) {
					if (worstNodeIndex != -1) {
						//replace the node
						long worstNodeHash = ns.get(worstNodeIndex).getNodeIdHash();
						HyCubeRoutingTableSlotInfo slotInfo = new HyCubeRoutingTableSlotInfo(HyCubeRoutingTableType.NS, nsMap, ns);
						RoutingTableEntry rte = initializeRoutingTableEntry(newNode, dist, currTimestamp, slotInfo);
						rte.setData(semiringNoRteKey, o);
						ns.set(worstNodeIndex, rte);
						//move the new node left (if it is closer) or right (if it is more distant) to the appropriate place to keep the NS orderd by distance
						int ind = worstNodeIndex;
						while (ind > 0 && dist < ns.get(ind-1).getDistance()) {
							ns.set(ind, ns.get(ind-1));
							ns.set(ind-1, rte);
							ind--;
						}
						while (ind < ns.size() - 1 && dist > ns.get(ind+1).getDistance()) {
							ns.set(ind,  ns.get(ind+1));
							ns.set(ind+1, rte);
							ind++;
						}
						nsMap.remove(worstNodeHash);
						nsMap.put(newNode.getNodeIdHash(), rte);
					}
				}
			}
		}
		
	}

}
