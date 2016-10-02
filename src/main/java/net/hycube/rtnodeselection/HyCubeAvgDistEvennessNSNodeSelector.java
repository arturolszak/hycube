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

public class HyCubeAvgDistEvennessNSNodeSelector extends HyCubeNSNodeSelector {

	
	public static final String PROP_KEY_METRIC = "Metric";
	
	public static final String PROP_KEY_DISTANCE_COMP_FACTOR = "DistanceCompFactor";
	public static final String PROP_KEY_DISTANCE_COMP_ELEM_EXP = "DistanceCompElemExp";
	public static final String PROP_KEY_DISTANCE_COMP_EXP = "DistanceCompExp";
	public static final String PROP_KEY_EVENNESS_COMP_FACTOR = "EvennessCompFactor";
	public static final String PROP_KEY_EVENNESS_COMP_ELEM_EXP = "EvennessCompElemExp";
	public static final String PROP_KEY_EVENNESS_COMP_EXP = "EvennessCompExp";
	

	protected Metric metric;
	protected double nsNodeSelectionAlgorithmDistanceCompFactor;
	protected double nsNodeSelectionAlgorithmDistanceCompElemExp;
	protected double nsNodeSelectionAlgorithmDistanceCompExp;
	protected double nsNodeSelectionAlgorithmEvennessCompFactor;
	protected double nsNodeSelectionAlgorithmEvennessCompElemExp;
	protected double nsNodeSelectionAlgorithmEvennessCompExp;
	
	
	@Override
	public void initialize(NodeId nodeId, NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		super.initialize(nodeId, nodeAccessor, properties);
		
		//parameters
		try {
			metric = (Metric) properties.getEnumProperty(PROP_KEY_METRIC, Metric.class);
			
			
			nsNodeSelectionAlgorithmDistanceCompFactor = (Double) properties.getProperty(PROP_KEY_DISTANCE_COMP_FACTOR, MappedType.DOUBLE);
			
			nsNodeSelectionAlgorithmDistanceCompElemExp = (Double) properties.getProperty(PROP_KEY_DISTANCE_COMP_ELEM_EXP, MappedType.DOUBLE);
			
			nsNodeSelectionAlgorithmDistanceCompExp = (Double) properties.getProperty(PROP_KEY_DISTANCE_COMP_EXP, MappedType.DOUBLE);
			
			nsNodeSelectionAlgorithmEvennessCompFactor = (Double) properties.getProperty(PROP_KEY_EVENNESS_COMP_FACTOR, MappedType.DOUBLE);
			
			nsNodeSelectionAlgorithmEvennessCompElemExp = (Double) properties.getProperty(PROP_KEY_EVENNESS_COMP_ELEM_EXP, MappedType.DOUBLE);
			
			nsNodeSelectionAlgorithmEvennessCompExp = (Double) properties.getProperty(PROP_KEY_EVENNESS_COMP_EXP, MappedType.DOUBLE);
			
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize NS node selector instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
	}
	
	@Override
	public void processNode(NodePointer newNode,
			List<RoutingTableEntry> ns, HashMap<Long, RoutingTableEntry> nsMap,
			int nsSize,
			double dist, long currTimestamp) {
		
		HyCubeNodeId nodeId = (HyCubeNodeId)this.nodeId;
		
		if (ns.size() < nsSize) {
			HyCubeRoutingTableSlotInfo slotInfo = new HyCubeRoutingTableSlotInfo(HyCubeRoutingTableType.NS, nsMap, ns);
			RoutingTableEntry rte = initializeRoutingTableEntry(newNode, dist, currTimestamp, slotInfo);
			ns.add(rte);
			nsMap.put(newNode.getNodeIdHash(), rte);
		}
		else {
			//calculate evenness component values:
			double currEvenness;
			double[] newEvenness;

			double sumDist = 0;
			double[] sumDists = new double[ns.size()];
			for (int i = 0; i < ns.size() - 1; i++) {
				for (int j = i + 1; j < ns.size(); j++) {
					double distij = HyCubeNodeId.calculateDistance((HyCubeNodeId)(ns.get(i).getNode().getNodeId()), (HyCubeNodeId)(ns.get(j).getNode().getNodeId()), metric);
					sumDist += Math.pow(distij, nsNodeSelectionAlgorithmEvennessCompElemExp);
					for (int k = 0; k < ns.size(); k++) {
						if (i == k) {
							double distkj = HyCubeNodeId.calculateDistance((HyCubeNodeId)(newNode.getNodeId()), (HyCubeNodeId)(ns.get(j).getNode().getNodeId()), metric);
							sumDists[k] += Math.pow(distkj, nsNodeSelectionAlgorithmEvennessCompElemExp);
						}
						if (j == k) {
							double distik = HyCubeNodeId.calculateDistance((HyCubeNodeId)(ns.get(i).getNode().getNodeId()), (HyCubeNodeId)(newNode.getNodeId()), metric);
							sumDists[k] += Math.pow(distik, nsNodeSelectionAlgorithmEvennessCompElemExp);
						}
						else {
							sumDists[k] += Math.pow(distij, nsNodeSelectionAlgorithmEvennessCompElemExp);
						}
					}
				}
			}
			currEvenness = sumDist;
			newEvenness = sumDists;
			//average:
			double numPairs = (ns.size() * ns.size() - ns.size()) / 2;
			currEvenness = currEvenness / numPairs;
			for (int i = 0; i < ns.size(); i++) newEvenness[i] = newEvenness[i] / numPairs; 
			
			currEvenness = Math.pow(currEvenness, nsNodeSelectionAlgorithmEvennessCompExp);
			for (int i = 0; i < ns.size(); i++) newEvenness[i] = Math.pow(newEvenness[i], nsNodeSelectionAlgorithmEvennessCompExp);
			
			
			//distance component:
			double distComp = 0;
			double[] newDistComps = new double[ns.size()];
			for (int i = 0; i < ns.size(); i++) {
				double disti = HyCubeNodeId.calculateDistance(nodeId, (HyCubeNodeId) ns.get(i).getNode().getNodeId(), metric);
				distComp += Math.pow(disti, nsNodeSelectionAlgorithmDistanceCompElemExp);
				newDistComps[i] = Math.pow(dist, nsNodeSelectionAlgorithmDistanceCompElemExp) - Math.pow(disti, nsNodeSelectionAlgorithmDistanceCompElemExp); 
			}
			for (int i = 0; i < ns.size(); i++) {
				newDistComps[i] = Math.pow(newDistComps[i] + distComp, nsNodeSelectionAlgorithmDistanceCompExp);
				newDistComps[i] = newDistComps[i] / ((double)ns.size());
				
			}
			distComp = Math.pow(distComp, nsNodeSelectionAlgorithmDistanceCompExp);
			distComp = distComp / ((double)ns.size());
			
			
			//find the worst node and replace it with the new node (unless the new node is the worst one)
			
			double objFun;
			int maxObjFunIndex = -1;						
			
			objFun = nsNodeSelectionAlgorithmDistanceCompFactor * distComp + nsNodeSelectionAlgorithmEvennessCompFactor * currEvenness;
			for (int i = 0; i < ns.size(); i++) {
				double newObjFun = nsNodeSelectionAlgorithmDistanceCompFactor * newDistComps[i] + nsNodeSelectionAlgorithmEvennessCompFactor * newEvenness[i];
				if (newObjFun > objFun) {
					maxObjFunIndex = i;
					objFun = newObjFun; 
				}
			}
			
			if (maxObjFunIndex != -1) {
				//replace the node i with the new one: 
				HyCubeRoutingTableSlotInfo slotInfo = new HyCubeRoutingTableSlotInfo(HyCubeRoutingTableType.NS, nsMap, ns);
				RoutingTableEntry rte = initializeRoutingTableEntry(newNode, dist, currTimestamp, slotInfo);
				nsMap.remove(ns.get(maxObjFunIndex).getNodeIdHash());
				ns.set(maxObjFunIndex, rte);
				nsMap.put(newNode.getNodeIdHash(), rte);
				
			}
			
		}
		
	}

}
