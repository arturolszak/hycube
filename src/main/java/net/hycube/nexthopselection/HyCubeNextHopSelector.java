package net.hycube.nexthopselection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.HyCubeNodeId;
import net.hycube.core.HyCubeRoutingTable;
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
import net.hycube.utils.HashMapUtils;
import net.hycube.utils.IntUtils;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public class HyCubeNextHopSelector extends NextHopSelector {

	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeNextHopSelector.class);
	
	
	protected static final String PROP_KEY_DIMENSIONS = "Dimensions";
	protected static final String PROP_KEY_LEVELS = "Levels";
	protected static final String PROP_KEY_USE_RT1 = "UseRT1";
	protected static final String PROP_KEY_USE_RT2 = "UseRT2";
	protected static final String PROP_KEY_USE_NS = "UseNS";
	protected static final String PROP_KEY_USE_RT1_IN_FULL_SCAN_WITHOUT_PMH = "UseRT1InFullScanWithoutPMH";
	protected static final String PROP_KEY_USE_RT2_IN_FULL_SCAN_WITHOUT_PMH = "UseRT2InFullScanWithoutPMH";
	protected static final String PROP_KEY_USE_NS_IN_FULL_SCAN_WITHOUT_PMH = "UseNSInFullScanWithoutPMH";
	protected static final String PROP_KEY_USE_RT1_IN_FULL_SCAN_WITH_PMH = "UseRT1InFullScanWithPMH";
	protected static final String PROP_KEY_USE_RT2_IN_FULL_SCAN_WITH_PMH = "UseRT2InFullScanWithPMH";
	protected static final String PROP_KEY_USE_NS_IN_FULL_SCAN_WITH_PMH = "UseNSInFullScanWithPMH";
	
	protected static final String PROP_KEY_METRIC = "Metric";
	
	protected static final String PROP_KEY_USE_STEINHAUS_TRANSFORM = "UseSteinhausTransform";
	protected static final String PROP_KEY_DYNAMIC_STEINHAUS_TRANSFORM = "DynamicSteinhausTransform";
	protected static final String PROP_KEY_ROUTE_WITH_REGULAR_METRIC_AFTER_STEINHAUS = "RouteWithRegularMetricAfterSteinhaus";
	
	protected static final String PROP_KEY_RESPECT_NUM_OF_COMMON_BITS_IN_NEXT_GROUP = "RespectNumOfCommonBitsInNextGroup";
	
	protected static final String PROP_KEY_PREFIX_MISMATCH_HEURISTIC_ENABLED = "PrefixMismatchHeuristicEnabled";
	protected static final String PROP_KEY_PREFIX_MISMATCH_HEURISTIC_MODE = "PrefixMismatchHeuristicMode";
	protected static final String PROP_KEY_PREFIX_MISMATCH_HEURISTIC_FACTOR = "PrefixMismatchHeuristicFactor";
	protected static final String PROP_KEY_PREFIX_MISMATCH_HEURISTIC_WHEN_NO_NEXT_HOP = "PrefixMismatchHeuristicWhenNoNextHop";
	
	protected static final String PROP_KEY_USE_STEINHAUS_TRANSFORM_ONLY_WITH_PMH = "UseSteinhausTransformOnlyWithPMH";
	
	protected static final String PROP_KEY_USE_SECURE_ROUTING = "UseSecureRouting";
	
	protected static final String PROP_KEY_SKIP_RANDOM_NUM_OF_NODES_ENABLED = "SkipRandomNumberOfNodesEnabled";
	protected static final String PROP_KEY_SKIP_RANDOM_NUM_OF_NODES_MEAN = "SkipRandomNumberOfNodesMean";
	protected static final String PROP_KEY_SKIP_RANDOM_NUM_OF_NODES_STDDEV = "SkipRandomNumberOfNodesStdDev";
	protected static final String PROP_KEY_SKIP_RANDOM_NUM_OF_NODES_ABSOLUTE = "SkipRandomNumberOfNodesAbsolute";
	protected static final String PROP_KEY_SKIP_RANDOM_NUM_OF_NODES_MAX = "SkipNodesNumMax";
	protected static final String PROP_KEY_SKIP_RANDOM_NUM_OF_NODES_DEF_WHEN_EXCEEDS_MAX = "SkipNodesNumWhenRandomExceedsMax";
	protected static final String PROP_KEY_FORCE_SKIP_RANDOM_NUM_OF_NODES = "ForceSkipNodes";
	protected static final String PROP_KEY_SKIP_RANDOM_NUM_OF_NODES_INCLUDE_EXACT_MATCH = "SkipNodesIncludeExcactMatch";
	
	
	protected NodeId nodeId;
	protected HyCubeRoutingTable routingTable;
	protected NodePointer selfNodePointer;
	
	protected boolean useSecureRouting;
	protected Metric metric;
	protected boolean useNS;
	protected boolean useRT1;
	protected boolean useRT2;
	protected boolean useNSInFullScanWithoutPMH;
	protected boolean useRT1InFullScanWithoutPMH;
	protected boolean useRT2InFullScanWithoutPMH;
	protected boolean useNSInFullScanWithPMH;
	protected boolean useRT1InFullScanWithPMH;
	protected boolean useRT2InFullScanWithPMH;
	protected int dimensions;
	protected int digitsCount;
	protected int nsSize;
	
	protected boolean useSteinhausTransform;
	protected boolean dynamicSteinhausTransform;
	protected boolean routeWithRegularMetricAfterSteinhaus;
	
	protected boolean respectNumOfCommonBitsInNextGroup;
	
	protected boolean pmhEnabled;
	protected HyCubePrefixMismatchHeuristicMode pmhMode;
	protected double pmhFactor;
	protected boolean pmhWhenNoNextHop;
	
	protected boolean useSteinhausTransformOnlyWithPMH;
	
	protected boolean skipRandomNumOfNodesEnabled;
	protected double skipRandomNumOfNodesMean;
	protected double skipRandomNumOfNodesStdDev;
	protected boolean skipRandomNumberOfNodesAbsolute;
	protected int skipRandomNumOfNodesMax;
	protected int skipRandomNumOfNodesDefWhenExceedsMax;
	protected boolean forceSkipRandomNumOfNodes;
	protected boolean skipRandomNumberOfNodesIncludeExcactMatch;
	protected Random numNodesToSkipRand = null;
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		super.initialize(nodeAccessor, properties);
		
		initialize(nodeAccessor.getNodeId(), nodeAccessor.getRoutingTable(), nodeAccessor.getNodePointer(), properties);
	}
		
	public void initialize(NodeId nodeId, RoutingTable routingTable, NodePointer selfNodePointer, NodeProperties properties) throws InitializationException {
	
		this.nodeId = nodeId;
		if (!(routingTable instanceof HyCubeRoutingTable)) throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "The routing table is expected to be an instance of: " + HyCubeRoutingTable.class.getName());
		this.routingTable = (HyCubeRoutingTable) routingTable;
		this.selfNodePointer = selfNodePointer;
		
		//parameters
		try {
			useSecureRouting = (Boolean) properties.getProperty(PROP_KEY_USE_SECURE_ROUTING, MappedType.BOOLEAN);

			metric = (Metric) properties.getEnumProperty(PROP_KEY_METRIC, Metric.class);

			useNS = (Boolean) properties.getProperty(PROP_KEY_USE_NS, MappedType.BOOLEAN);

			useRT1 = (Boolean) properties.getProperty(PROP_KEY_USE_RT1, MappedType.BOOLEAN);

			useRT2 = (Boolean) properties.getProperty(PROP_KEY_USE_RT2, MappedType.BOOLEAN);

			useNSInFullScanWithoutPMH = (Boolean) properties.getProperty(PROP_KEY_USE_NS_IN_FULL_SCAN_WITHOUT_PMH, MappedType.BOOLEAN);
			
			useRT1InFullScanWithoutPMH = (Boolean) properties.getProperty(PROP_KEY_USE_RT1_IN_FULL_SCAN_WITHOUT_PMH, MappedType.BOOLEAN);

			useRT2InFullScanWithoutPMH = (Boolean) properties.getProperty(PROP_KEY_USE_RT2_IN_FULL_SCAN_WITHOUT_PMH, MappedType.BOOLEAN);
			
			useNSInFullScanWithPMH = (Boolean) properties.getProperty(PROP_KEY_USE_NS_IN_FULL_SCAN_WITH_PMH, MappedType.BOOLEAN);
			
			useRT1InFullScanWithPMH = (Boolean) properties.getProperty(PROP_KEY_USE_RT1_IN_FULL_SCAN_WITH_PMH, MappedType.BOOLEAN);

			useRT2InFullScanWithPMH = (Boolean) properties.getProperty(PROP_KEY_USE_RT2_IN_FULL_SCAN_WITH_PMH, MappedType.BOOLEAN);

			dimensions = (Integer) properties.getProperty(PROP_KEY_DIMENSIONS, MappedType.INT);
			if (dimensions <= 0) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize HyCubeNextHopSelector instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_DIMENSIONS) + ".");
			}

			digitsCount = (Integer) properties.getProperty(PROP_KEY_LEVELS, MappedType.INT);
			if (digitsCount <= 0) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize HyCubeNextHopSelector instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_LEVELS) + ".");
			}

			useSteinhausTransform = (Boolean) properties.getProperty(PROP_KEY_USE_STEINHAUS_TRANSFORM, MappedType.BOOLEAN);
			
			dynamicSteinhausTransform = (Boolean) properties.getProperty(PROP_KEY_DYNAMIC_STEINHAUS_TRANSFORM, MappedType.BOOLEAN);
			
			routeWithRegularMetricAfterSteinhaus = (Boolean) properties.getProperty(PROP_KEY_ROUTE_WITH_REGULAR_METRIC_AFTER_STEINHAUS, MappedType.BOOLEAN);

			respectNumOfCommonBitsInNextGroup = (Boolean) properties.getProperty(PROP_KEY_RESPECT_NUM_OF_COMMON_BITS_IN_NEXT_GROUP, MappedType.BOOLEAN);
			
			pmhEnabled = (Boolean) properties.getProperty(PROP_KEY_PREFIX_MISMATCH_HEURISTIC_ENABLED, MappedType.BOOLEAN);
			
			pmhMode = (HyCubePrefixMismatchHeuristicMode) properties.getEnumProperty(PROP_KEY_PREFIX_MISMATCH_HEURISTIC_MODE, HyCubePrefixMismatchHeuristicMode.class);

			pmhFactor = (Double) properties.getProperty(PROP_KEY_PREFIX_MISMATCH_HEURISTIC_FACTOR, MappedType.DOUBLE);
			
			pmhWhenNoNextHop = (Boolean) properties.getProperty(PROP_KEY_PREFIX_MISMATCH_HEURISTIC_WHEN_NO_NEXT_HOP, MappedType.BOOLEAN);
			
			useSteinhausTransformOnlyWithPMH = (Boolean) properties.getProperty(PROP_KEY_USE_STEINHAUS_TRANSFORM_ONLY_WITH_PMH, MappedType.BOOLEAN);
			
			
			skipRandomNumOfNodesEnabled = (Boolean) properties.getProperty(PROP_KEY_SKIP_RANDOM_NUM_OF_NODES_ENABLED, MappedType.BOOLEAN);
			
			skipRandomNumOfNodesMean = (Double) properties.getProperty(PROP_KEY_SKIP_RANDOM_NUM_OF_NODES_MEAN, MappedType.DOUBLE);
			
			skipRandomNumOfNodesStdDev = (Double) properties.getProperty(PROP_KEY_SKIP_RANDOM_NUM_OF_NODES_STDDEV, MappedType.DOUBLE);
			
			skipRandomNumberOfNodesAbsolute = (Boolean) properties.getProperty(PROP_KEY_SKIP_RANDOM_NUM_OF_NODES_ABSOLUTE, MappedType.BOOLEAN);
			
			skipRandomNumOfNodesMax = (Integer) properties.getProperty(PROP_KEY_SKIP_RANDOM_NUM_OF_NODES_MAX, MappedType.INT);
			if (skipRandomNumOfNodesMax < 0) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize HyCubeNextHopSelector instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_SKIP_RANDOM_NUM_OF_NODES_MAX) + ".");
			}
			
			skipRandomNumOfNodesDefWhenExceedsMax = (Integer) properties.getProperty(PROP_KEY_SKIP_RANDOM_NUM_OF_NODES_DEF_WHEN_EXCEEDS_MAX, MappedType.INT);
			if (skipRandomNumOfNodesMax < 0) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize HyCubeNextHopSelector instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_SKIP_RANDOM_NUM_OF_NODES_MAX) + ".");
			}
			
			forceSkipRandomNumOfNodes = (Boolean) properties.getProperty(PROP_KEY_FORCE_SKIP_RANDOM_NUM_OF_NODES, MappedType.BOOLEAN);
			
			skipRandomNumberOfNodesIncludeExcactMatch = (Boolean) properties.getProperty(PROP_KEY_SKIP_RANDOM_NUM_OF_NODES_INCLUDE_EXACT_MATCH, MappedType.BOOLEAN);
			
			numNodesToSkipRand = new Random();
			
			
			
			

		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize HyCubeNextHopSelector instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
	}
	
	
	
	
	@Override
	public NodePointer findNextHop(NodeId recipientId, NextHopSelectionParameters parameters) {
		NodePointer[] nextHops = findNextHops(recipientId, parameters, 1);
		if (nextHops.length > 0) return nextHops[0];
		else return null;
	}
	
	
	
	@Override
	public NodePointer[] findNextHops(NodeId recipientId, NextHopSelectionParameters parameters, int numNextHops) {
		return findNextHops((HyCubeNodeId) recipientId, (HyCubeNextHopSelectionParameters) parameters, numNextHops);
		
	}	
	
	
	public NodePointer[] findNextHops(HyCubeNodeId recipientId, HyCubeNextHopSelectionParameters parameters, int numNextHops) {
		
		HyCubeRoutingTable rt = (HyCubeRoutingTable)routingTable;
		
		if (useSecureRouting && parameters.isSecureRoutingApplied()) {
			return findNextHops(rt.getSecRoutingTable1(), rt.getSecRoutingTable2(), rt.getNeighborhoodSet(), rt.getSecRt1Lock(), rt.getSecRt2Lock(), rt.getNsLock(), recipientId, parameters, numNextHops);
		}
		else {
			return findNextHops(rt.getRoutingTable1(), rt.getRoutingTable2(), rt.getNeighborhoodSet(), rt.getRt1Lock(), rt.getRt2Lock(), rt.getNsLock(), recipientId, parameters, numNextHops);
		}
		
		
	}

	
	
	protected NodePointer[] findNextHops(List<RoutingTableEntry>[][] rt1, List<RoutingTableEntry>[][] rt2, List<RoutingTableEntry> ns, ReentrantReadWriteLock rt1Lock, ReentrantReadWriteLock rt2Lock, ReentrantReadWriteLock nsLock, HyCubeNodeId recipientId, HyCubeNextHopSelectionParameters parameters, int numNextHops) {
		
		NodePointer[] bestNodes = null;
		
        HyCubeNodeId nodeId = (HyCubeNodeId)this.nodeId;
        
        int levels = digitsCount;
        
        
        List<RoutingTableEntry> nextHops = new ArrayList<RoutingTableEntry>(GlobalConstants.INITIAL_FOUND_NEXT_HOPS_COLLECTION_SIZE);
        Map<Long, RoutingTableEntry> nextHopsMap = new HashMap<Long, RoutingTableEntry>(HashMapUtils.getHashMapCapacityForElementsNum(GlobalConstants.INITIAL_FOUND_NEXT_HOPS_COLLECTION_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);

        double distToRecipient = HyCubeNodeId.calculateDistance(nodeId, recipientId, metric);
        int commonPrefixLength = HyCubeNodeId.calculatePrefixLength(nodeId, recipientId);
        int numOfCommonBitsInNextGroup = 0;
        int nextDigit = 0;
    	int nextDigitRecipient = 0;
    	if (respectNumOfCommonBitsInNextGroup) {
    		nextDigit = nodeId.getDigitAsInt(commonPrefixLength);
    		nextDigitRecipient = recipientId.getDigitAsInt(commonPrefixLength);
    		numOfCommonBitsInNextGroup = dimensions - IntUtils.getHammingDistance(nextDigit, nextDigitRecipient);
    	}
		
    	
    	
        //acquire the ns lock:
        nsLock.readLock().lock();
        
        //copy the ns:
        ArrayList<RoutingTableEntry> nsCopy = new ArrayList<RoutingTableEntry>(ns);
        
        //release the ns lock
        nsLock.readLock().unlock();
		
		
        
        
        //temporary routing table slot copy
        ArrayList<RoutingTableEntry> tmpRtSlotCopy = new ArrayList<RoutingTableEntry>(routingTable.getRoutingTableSlotSize());
        
        
    	
    	
    	
    	//actualize PMH enabled flag for the message:
		
        //calculate the average and maximum node distance in the neighborhood set:
        double maxNSDist = 0;
        double avgNSDist = 0;
        
        
        for (RoutingTableEntry rte : nsCopy) {
        	double distToNode = rte.getDistance();
        	avgNSDist += distToNode;
        	if (distToNode > maxNSDist) maxNSDist = distToNode;
        }
        avgNSDist = avgNSDist / nsCopy.size();
        
        
        
        //check whether to enable the prefix mismatch heuristic for the message
        if (parameters.isPMHApplied() == false && pmhEnabled) {
            switch (pmhMode) {
	            case AVG:
	            	if (pmhFactor == 0 || distToRecipient < avgNSDist * pmhFactor) {
	            		parameters.setPMHApplied(true);
	            	}
	            	break;
	            case MAX:
	            	if (pmhFactor == 0 || distToRecipient < maxNSDist * pmhFactor) {
	            		parameters.setPMHApplied(true);
	            	}
	            	break;
	           }
        }

        if (devLog.isTraceEnabled()) {
			devLog.trace("Prefix mismatch heuristic enabled: " + (parameters.isPMHApplied() && pmhEnabled));
		}
    	
    	
        
        
		//check if Steinhaus transform should be applied:
        
        HyCubeNodeId steinhausPoint = null;

        
        //update the Steinhaus point - even if the Steinhaus transform will not be used in distance calculations (in case useSteinhausTransformOnlyWithPMH=true and PMH is not applied)
        if (useSteinhausTransform && parameters.isSteinhausTransformApplied()) {
	    	if (parameters.getSteinhausPoint() == null) {
	        	parameters.setSteinhausPoint(nodeId);
	        }
	    	else {
	    		//update the point only if dynamic Steinhaus transform is enabled
	    		if (dynamicSteinhausTransform) {
	        		//update the Steinhaus point:
	        		double distFromSteinhausPointToRecipient = HyCubeNodeId.calculateDistance(parameters.getSteinhausPoint(), recipientId, metric);
	        		if (distToRecipient <= distFromSteinhausPointToRecipient) {
	        			parameters.setSteinhausPoint(nodeId);
	        		}
	    		}
	    		
	    	}
        }
        
    	
        if (useSteinhausTransform && parameters.isSteinhausTransformApplied() && (useSteinhausTransformOnlyWithPMH == false || parameters.isPMHApplied() || pmhFactor==0)) {
        	//set the Steinhaus point to be used
        	steinhausPoint = parameters.getSteinhausPoint();

			//recalculate distance (use Steinhaus transform)
			distToRecipient = HyCubeNodeId.calculateDistance(nodeId, recipientId, steinhausPoint, metric);
        	
        }
        else {
        	//regular metric will be used
        	steinhausPoint = null;
        	
        	//do not set the Steinhaus point value (returned) to null, the message field should not be null
        	//parameters.setSteinhausPoint(null);
        	
        }
        
        if (devLog.isTraceEnabled()) {
			devLog.trace("Steinhaus point: " + (steinhausPoint != null ? steinhausPoint.toHexString() : "NULL"));
		}
        
        
		

        
        

		
		int numNextHopsToFind = numNextHops;
		
		int numNodesToSkip = 0;
        if (skipRandomNumOfNodesEnabled && parameters.isSkipRandomNumOfNodesApplied()) {
        	double numNodesToSkipTemp = 0;
        	if (skipRandomNumberOfNodesAbsolute) {
        		numNodesToSkipTemp = Math.floor(Math.abs((numNodesToSkipRand.nextGaussian() * skipRandomNumOfNodesStdDev) + skipRandomNumOfNodesMean));
        	}
        	else {
        		numNodesToSkipTemp = Math.floor(Math.max((numNodesToSkipRand.nextGaussian() * skipRandomNumOfNodesStdDev) + skipRandomNumOfNodesMean, 0));
        	}
        	if (numNodesToSkipTemp <= skipRandomNumOfNodesMax) numNodesToSkip = (int) numNodesToSkipTemp;
        	else numNodesToSkip = skipRandomNumOfNodesDefWhenExceedsMax;
			numNextHopsToFind = numNextHopsToFind + numNodesToSkip;
        }
		
        
        
        
        //check if the NS contains the recipient node:
        if (useNS && (! parameters.isSkipTargetNode())) {
	        for (RoutingTableEntry rte : nsCopy) {
	        	if (! rte.isEnabled()) continue;
	    		if (rte.getNode().getNodeId().equals(recipientId)) {
	    			if (devLog.isTraceEnabled()) {
	    				devLog.trace("The destination node was found in NS: " + rte.getNode().getNodeId().toHexString());
	    			}
	            	long nodeHash = rte.getNodeIdHash();
	                if (!nextHopsMap.containsKey(nodeHash)) {
	                    nextHops.add(rte);
	                    nextHopsMap.put(nodeHash, rte);
	                }
	                break;
	    		}
	    	}
        }
        

        if (numNextHopsToFind == 1 && (! nextHops.isEmpty()) && (numNodesToSkip == 0 || skipRandomNumberOfNodesIncludeExcactMatch == false)) {
        	if (devLog.isTraceEnabled()) {
        		devLog.trace("Next hop found: " + nextHops.get(0).getNode().getNodeId().toHexString());
			}
        	return new NodePointer[] {nextHops.get(0).getNode()};
        }

        
        
        if (parameters.includeSelf) {
        	long nodeIdHash = nodeId.calculateHash();
        	RoutingTableEntry self = RoutingTableEntry.initializeRoutingTableEntry(selfNodePointer, 0, 0, null);
        	nextHops.add(self);
            nextHopsMap.put(nodeIdHash, self);
        }
        
        
        
        if (! parameters.isPMHApplied()) {
        	
        	int rt1level = levels - commonPrefixLength - 1;
        	int rt1hypercube = 0;
        	
        	if (rt1level != -1) {	//if the requested node id is equal to self, commonPrefixLength equals levels
        		
	        	rt1hypercube = recipientId.getDigitAsInt(levels - 1 - rt1level);
	        	
	        	
	        	//primary routing table:
	        	if (useRT1) {
	        		if (devLog.isTraceEnabled()) {
	        			devLog.trace("Checking RT1 for a node with a closer prefix...");
	        		}
	        		
	        		//acquire the lock
	            	rt1Lock.readLock().lock();
	            	
	            	//copy the rt slot (synchronized with any modifications) and then operate on the copy
	            	tmpRtSlotCopy.clear();
	            	tmpRtSlotCopy.addAll(rt1[rt1level][rt1hypercube]);
	            	
	            	//release the lock
	   	            rt1Lock.readLock().unlock();
	   	            
		            for (RoutingTableEntry rte : tmpRtSlotCopy) {
		            	
		            	if (! rte.isEnabled()) continue;
		            	
		            	HyCubeNodeId rteNodeId = (HyCubeNodeId) rte.getNode().getNodeId();
		            	
		            	//skip the node with the requested id:
		            	if (parameters.isSkipTargetNode() && HyCubeNodeId.compareIds(rteNodeId, recipientId)) continue;
		            	
		            	long nodeHash = rte.getNodeIdHash();
		                if (!nextHopsMap.containsKey(nodeHash)) {
		                    nextHops.add(rte);
		                    nextHopsMap.put(nodeHash, rte);
		                }
		            }
	
	        	}
            
        	}
        	
        	
        	if (nextHops.size() < numNextHopsToFind) {

        		if (rt1level != -1) {
        		
	        		if (useRT1 && (!useRT1InFullScanWithoutPMH)) {
	        			for (int hypercube = 0; hypercube < ((long)1 << dimensions); hypercube++) {
	        				if (hypercube != rt1hypercube) {
	        					
	        					//acquire the lock
	        	            	rt1Lock.readLock().lock();
	        	            	
	        	            	//copy the rt slot (synchronized with any modifications) and then operate on the copy
	        	            	tmpRtSlotCopy.clear();
	        	            	tmpRtSlotCopy.addAll(rt1[rt1level][hypercube]);
	        	            	
	        	            	//release the lock
	        	   	            rt1Lock.readLock().unlock();
	        	   	            
			    	            for (RoutingTableEntry rte : tmpRtSlotCopy) {
			    	            	
			    	            	if (! rte.isEnabled()) continue;
			    	            	
			    	            	HyCubeNodeId rteNodeId = (HyCubeNodeId) rte.getNode().getNodeId();
	
			    	            	long rteHash = rte.getNodeIdHash();
			    	            	
			    	            	//skip the node with the requested id:
			    	            	if (parameters.isSkipTargetNode() && HyCubeNodeId.compareIds(rteNodeId, recipientId)) continue;
	
			    	            	if (parameters.isIncludeMoreDistantNodes()) {
			    	            		if (! nextHopsMap.containsKey(rteHash)) {
	                                        nextHops.add(rte);
	                                        nextHopsMap.put(rteHash, rte);
	                                    }
			    	            	}
			    	            	else {
			    	            		//filter the nodes:
			    	            		
				    	            	int nextDigitRte = 0;
		            	            	int numOfCommonBitsInNextGroupWithRte = 0;
		            	            	if (respectNumOfCommonBitsInNextGroup) {
		            	            		nextDigitRte = rteNodeId.getDigitAsInt(commonPrefixLength);
		            	            		numOfCommonBitsInNextGroupWithRte = dimensions - IntUtils.getHammingDistance(nextDigitRte, nextDigitRecipient);
		            	            	}
		                            	
		                            	double distanceFromRte = HyCubeNodeId.calculateDistance(rteNodeId, recipientId, steinhausPoint, metric);
		                            	
		                            	if ((respectNumOfCommonBitsInNextGroup && numOfCommonBitsInNextGroupWithRte > numOfCommonBitsInNextGroup)
		                            			|| ((respectNumOfCommonBitsInNextGroup == false || numOfCommonBitsInNextGroupWithRte == numOfCommonBitsInNextGroup) && distanceFromRte < distToRecipient)) {
		                                    if (! nextHopsMap.containsKey(rteHash)) {
		                                        nextHops.add(rte);
		                                        nextHopsMap.put(rteHash, rte);
		                                    }
		                                }
		                            	
		                            	
	                            	
			    	            	}
			    	            			    	            	
			    	            }
	
	        				}
	        			}
	        		}
        		}
            	
        		if (useRT2 && (!useRT2InFullScanWithoutPMH)) {
            		
            		if (devLog.isTraceEnabled()) {
            			devLog.trace("Checking RT2...");
            		}
            		
	            	int additionalLevelsToCheckUp = 0;
	                int additionalLevelsToCheckDown = 0;
	                
	            	for (int dim = 0; dim < dimensions; dim++) {
	            		
	                    double distInDim = HyCubeNodeId.calculateDistanceInDimension(nodeId, recipientId, dim);
	                    
	                    int logDistInDim = (int)(Math.log(distInDim)/Math.log(2));
	                    int startLevel = logDistInDim - additionalLevelsToCheckDown;
	                    int endLevel = logDistInDim + 1 + additionalLevelsToCheckUp;
	                    
	                    if (startLevel < 0) startLevel = 0;
	                    if (endLevel > levels - 1) endLevel = levels - 1;
	
	                    for (int level = startLevel; level <= endLevel; level++) {
	
	                        int digitInd = levels - 1 - level;
	
	                        HyCubeNodeId hypercube1 = nodeId.addBitInDimension(dim, digitInd);
	                        HyCubeNodeId hypercube2 = nodeId.subBitInDimension(dim, digitInd);
	
	                        HyCubeNodeId rt1HypercubePrefix;
	
	                        if ((useRT1 && level == levels - 1) || HyCubeNodeId.compareIds(hypercube1.getSubID(0, levels - level - 1), nodeId.getSubID(0, levels - level - 1))) {
	                        	//either a highest level (covered by RT1) or hypercube1 and nodeId are in the same hypercube one level higher (which means it is covered by RT1)
	                            //the RT2 slot represents hypercube2; hycercube1 is represented by a RT1 slot
	                            rt1HypercubePrefix = hypercube1.getSubID(0, levels - level);
	                        }
	                        else {
	                            //the RT2 slot represents hypercube1; hypercube2 is represented by a RT1 slot
	                            rt1HypercubePrefix = hypercube2.getSubID(0, levels - level);
	                        }
	                        
	                        
	                        
	                        //---------------------------
	                        //sibling hypercube from RT2:
	                        
	                        //acquire the lock
	        	            rt2Lock.readLock().lock();
	        	            
	        	            //copy the rt slot (synchronized with any modifications) and then operate on the copy
	        	            tmpRtSlotCopy.clear();
	        	            tmpRtSlotCopy.addAll(rt2[level][dim]);
	        	            
	        	            //release the lock
	        	   	        rt2Lock.readLock().unlock();
	                        
	                        for (RoutingTableEntry rte : tmpRtSlotCopy) {
	                        
	                            if (! rte.isEnabled()) continue;
	                        	
	                            HyCubeNodeId rteNodeId = (HyCubeNodeId) rte.getNode().getNodeId();

	                            long rteHash = rte.getNodeIdHash();
	                            	
	                            //skip the node with the requested id:
	                            if (parameters.isSkipTargetNode() && HyCubeNodeId.compareIds(rteNodeId, recipientId)) continue;

	                            if (parameters.isIncludeMoreDistantNodes()) {
			    	            	if (! nextHopsMap.containsKey(rteHash)) {
	                                    nextHops.add(rte);
	                                    nextHopsMap.put(rteHash, rte);
	                                }
			    	            }
	                            else {
	                            	//filter:
	                            
	                            	int commonPrefixLengthWithRte = HyCubeNodeId.calculatePrefixLength(rteNodeId, recipientId);
		                            
		                            int nextDigitRte = 0;
		            	            int numOfCommonBitsInNextGroupWithRte = 0;
		            	            if (respectNumOfCommonBitsInNextGroup) {
		            	            	nextDigitRte = rteNodeId.getDigitAsInt(commonPrefixLength);
		            	            	nextDigitRecipient = recipientId.getDigitAsInt(commonPrefixLength);
		            	            	numOfCommonBitsInNextGroupWithRte = dimensions - IntUtils.getHammingDistance(nextDigitRte, nextDigitRecipient);
		            	            }
		                            
		                            double distanceFromRte = HyCubeNodeId.calculateDistance(rteNodeId, recipientId, steinhausPoint, metric);
		                            
		                            if (commonPrefixLengthWithRte > commonPrefixLength
		                            		|| (respectNumOfCommonBitsInNextGroup && commonPrefixLengthWithRte == commonPrefixLength && numOfCommonBitsInNextGroupWithRte > numOfCommonBitsInNextGroup)
		                            		|| (commonPrefixLengthWithRte == commonPrefixLength && (respectNumOfCommonBitsInNextGroup == false || numOfCommonBitsInNextGroupWithRte == numOfCommonBitsInNextGroup) && distanceFromRte < distToRecipient)) {
		                            	if (! nextHopsMap.containsKey(rteHash)) {
		                                    nextHops.add(rte);
		                                    nextHopsMap.put(rteHash, rte);
		                                }
		                            }
	                            }
	                        }

	                        
	                        
	                        
	                        //---------------------------
	                        //sibling hypercube from RT1:
	                        
                            int hypercube = rt1HypercubePrefix.getDigitAsInt(levels - 1 - level);
	
	                        //acquire the lock
	        	            rt1Lock.readLock().lock();
	        	            
	        	            //copy the rt slot (synchronized with any modifications) and then operate on the copy
	        	            tmpRtSlotCopy.clear();
	        	            tmpRtSlotCopy.addAll(rt1[level][hypercube]);
	        	            
	        	            //release the lock
	        	   	        rt1Lock.readLock().unlock();
	        	   	        
	                        for (RoutingTableEntry rte : tmpRtSlotCopy) {
    	
	                            if (! rte.isEnabled()) continue;
	                            
	                            HyCubeNodeId rteNodeId = (HyCubeNodeId) rte.getNode().getNodeId();
	                            
	                            long rteHash = rte.getNodeIdHash();
	                            
	                            //skip the node with the requested id:
	                            if (parameters.isSkipTargetNode() && HyCubeNodeId.compareIds(rteNodeId, recipientId)) continue;
	                            
	                            if (parameters.isIncludeMoreDistantNodes()) {
			    	            	if (! nextHopsMap.containsKey(rteHash)) {
	                                    nextHops.add(rte);
	                                    nextHopsMap.put(rteHash, rte);
	                                }
			    	            }
	                            else {
	                            
	                            	//filter:
	                            	
		                            int commonPrefixLengthWithRte = HyCubeNodeId.calculatePrefixLength(rteNodeId, recipientId);
		                            
		                            int nextDigitRte = 0;
		            	            int numOfCommonBitsInNextGroupWithRte = 0;
		            	            if (respectNumOfCommonBitsInNextGroup) {
		            	            	nextDigitRte = rteNodeId.getDigitAsInt(commonPrefixLength);
		            	            	nextDigitRecipient = recipientId.getDigitAsInt(commonPrefixLength);
		            	            	numOfCommonBitsInNextGroupWithRte = dimensions - IntUtils.getHammingDistance(nextDigitRte, nextDigitRecipient);
		            	            }
		            	            
		                            double distanceFromRte = HyCubeNodeId.calculateDistance(rteNodeId, recipientId, steinhausPoint, metric);
		                            
		                            if (commonPrefixLengthWithRte > commonPrefixLength 
		                            		|| (respectNumOfCommonBitsInNextGroup && commonPrefixLengthWithRte == commonPrefixLength && numOfCommonBitsInNextGroupWithRte > numOfCommonBitsInNextGroup)
		                            		|| (commonPrefixLengthWithRte == commonPrefixLength && (respectNumOfCommonBitsInNextGroup == false || numOfCommonBitsInNextGroupWithRte == numOfCommonBitsInNextGroup) && distanceFromRte < distToRecipient)) {
		                                if (! nextHopsMap.containsKey(rteHash)) {
		                                    nextHops.add(rte);
		                                    nextHopsMap.put(rteHash, rte);
		                                }
		                            }
		                            	
	                            }
	                            	
	                        }

	                        
	
	                    }
	                }
            	}
        		
        		
        		
        		if (useNSInFullScanWithoutPMH) {
		        	for (RoutingTableEntry rte : nsCopy) {

		        		if (! rte.isEnabled()) continue;
		        		
		        		HyCubeNodeId rteNodeId = (HyCubeNodeId) rte.getNode().getNodeId();
		        		
		        		//skip the node with the requested id:
		        		if (parameters.isSkipTargetNode() && HyCubeNodeId.compareIds(rteNodeId, recipientId)) continue;

		        		
		        		long rteHash = rte.getNodeIdHash();
		        		

		        		if (parameters.isIncludeMoreDistantNodes()) {
		        			if (! nextHopsMap.containsKey(rteHash)) {
		        				nextHops.add(rte);
                                nextHopsMap.put(rteHash, rte);
                            }
		    	        }
		        		else {
		        			//filter:
		        		
			        		int commonPrefixLengthWithRte = HyCubeNodeId.calculatePrefixLength(rteNodeId, recipientId);
	                    	
	                    	int nextDigitRte = 0;
	    	            	int numOfCommonBitsInNextGroupWithRte = 0;
	    	            	if (respectNumOfCommonBitsInNextGroup) {
	    	            		nextDigitRte = rteNodeId.getDigitAsInt(commonPrefixLength);
	    	            		numOfCommonBitsInNextGroupWithRte = dimensions - IntUtils.getHammingDistance(nextDigitRte, nextDigitRecipient);
	    	            	}
	    	            	
	                    	double distanceFromRte = HyCubeNodeId.calculateDistance(rteNodeId, recipientId, steinhausPoint, metric);
	                    	
	                    	if (commonPrefixLengthWithRte > commonPrefixLength 
	                    			|| (respectNumOfCommonBitsInNextGroup && commonPrefixLengthWithRte == commonPrefixLength && numOfCommonBitsInNextGroupWithRte > numOfCommonBitsInNextGroup)
	                    			|| (commonPrefixLengthWithRte == commonPrefixLength && (respectNumOfCommonBitsInNextGroup == false || numOfCommonBitsInNextGroupWithRte == numOfCommonBitsInNextGroup) && distanceFromRte < distToRecipient)) {
	                            if (! nextHopsMap.containsKey(rteHash)) {
	                                nextHops.add(rte);
	                                nextHopsMap.put(rteHash, rte);
	                            }
	                        }
	          
		        		}
		        		
		        	}
	        	}
		  
	        	if (useRT1InFullScanWithoutPMH) {
	        		
	        		int maxLevelToCheck = -1;
	        		if (parameters.isIncludeMoreDistantNodes()) {
	        			//all levels should be checked
	        			maxLevelToCheck = levels - 1;
	        		}
	        		else {
	        			if (rt1level != -1) {
			        		//only lower levels must still be checked
	        				maxLevelToCheck = rt1level;
	        			}
	        			else {
	        				maxLevelToCheck = -1;	//don't check, no closer nodes
	        			}
	        		}
	        		
	        		for (int level = maxLevelToCheck; level >= 0; level--) {
		        		
	        			for (int hypercube = 0; hypercube < ((long)1 << dimensions); hypercube++) {
	      					
		        			if (level == rt1level && hypercube == rt1hypercube) continue;	//was already checked
	        				
	       					//acquire the lock
	       	            	rt1Lock.readLock().lock();
	       	            	
	       	            	//copy the rt slot (synchronized with any modifications) and then operate on the copy
	       	            	tmpRtSlotCopy.clear();
	       	            	tmpRtSlotCopy.addAll(rt1[level][hypercube]);
	       	            	
	       	            	//release the lock
	       	   	            rt1Lock.readLock().unlock();
	       	   	            
		    	            for (RoutingTableEntry rte : tmpRtSlotCopy) {
			    	           	
			    	           	if (! rte.isEnabled()) continue;
			    	           	
			    	           	HyCubeNodeId rteNodeId = (HyCubeNodeId) rte.getNode().getNodeId();
	
			    	           	long rteHash = rte.getNodeIdHash();
			    	            
			    	            //skip the node with the requested id:
			    	            if (parameters.isSkipTargetNode() && HyCubeNodeId.compareIds(rteNodeId, recipientId)) continue;
	
			    	            if (parameters.isIncludeMoreDistantNodes()) {
			    	            	if (! nextHopsMap.containsKey(rteHash)) {
	                                       nextHops.add(rte);
	                                       nextHopsMap.put(rteHash, rte);
	                                   }
			    	            }
			    	            else {
			    	            	//filter the nodes:
			    	            	
			    	            	int commonPrefixLengthWithRte = HyCubeNodeId.calculatePrefixLength(rteNodeId, recipientId);
			    	            	
				    	           	int nextDigitRte = 0;
		            	           	int numOfCommonBitsInNextGroupWithRte = 0;
		            	           	if (respectNumOfCommonBitsInNextGroup) {
		            	           		nextDigitRte = rteNodeId.getDigitAsInt(commonPrefixLength);
		            	           		numOfCommonBitsInNextGroupWithRte = dimensions - IntUtils.getHammingDistance(nextDigitRte, nextDigitRecipient);
		            	           	}
		                           	
		                           	double distanceFromRte = HyCubeNodeId.calculateDistance(rteNodeId, recipientId, steinhausPoint, metric);
		                           	
		                           	if (commonPrefixLengthWithRte > commonPrefixLength 
			                    			|| (respectNumOfCommonBitsInNextGroup && commonPrefixLengthWithRte == commonPrefixLength && numOfCommonBitsInNextGroupWithRte > numOfCommonBitsInNextGroup)
			                    			|| (commonPrefixLengthWithRte == commonPrefixLength && (respectNumOfCommonBitsInNextGroup == false || numOfCommonBitsInNextGroupWithRte == numOfCommonBitsInNextGroup) && distanceFromRte < distToRecipient)) {
			                            if (! nextHopsMap.containsKey(rteHash)) {
			                                nextHops.add(rte);
			                                nextHopsMap.put(rteHash, rte);
			                            }
			                        }
		                            	
		                            	
			    	            }
	
	        				}
		        		}
        			}
	        		
	        		
	        	}
	        	

	        	if (useRT2InFullScanWithoutPMH) {
		            for (int i = 0; i < levels; i++)
		            {
		                for (int ii = 0; ii < dimensions; ii++)
		                {

		                	//acquire the lock
        	            	rt2Lock.readLock().lock();
        	            	
        	            	//copy the rt slot (synchronized with any modifications) and then operate on the copy
        	            	tmpRtSlotCopy.clear();
        	            	tmpRtSlotCopy.addAll(rt2[i][ii]);
        	            	
        	            	//release the lock
        	   	            rt2Lock.readLock().unlock();
		                	
		                	for (RoutingTableEntry rte : tmpRtSlotCopy) {
		                		
		                		if (! rte.isEnabled()) continue;
		                		
		                		HyCubeNodeId rteNodeId = (HyCubeNodeId) rte.getNode().getNodeId();
		                		
		                		long rteHash = rte.getNodeIdHash();
		                		
		                		//skip the node with the requested id:
		                		if (parameters.isSkipTargetNode() && HyCubeNodeId.compareIds(rteNodeId, recipientId)) continue;
		                		
		                		if (parameters.isIncludeMoreDistantNodes()) {
		    	            		if (! nextHopsMap.containsKey(rteHash)) {
                                        nextHops.add(rte);
                                        nextHopsMap.put(rteHash, rte);
                                    }
		    	            	}
		                		else {
		                			//filter
		                			
			                		int commonPrefixLengthWithRte = HyCubeNodeId.calculatePrefixLength(rteNodeId, recipientId);
			                    	
			                    	int nextDigitRte = 0;
			    	            	int numOfCommonBitsInNextGroupWithRte = 0;
			    	            	if (respectNumOfCommonBitsInNextGroup) {
			    	            		nextDigitRte = ((HyCubeNodeId)(rte.getNode().getNodeId())).getDigitAsInt(commonPrefixLength);
			    	            		nextDigitRecipient = recipientId.getDigitAsInt(commonPrefixLength);
			    	            		numOfCommonBitsInNextGroupWithRte = dimensions - IntUtils.getHammingDistance(nextDigitRte, nextDigitRecipient);
			    	            	}
			    	            	
			                    	double distanceFromRte = HyCubeNodeId.calculateDistance(rteNodeId, recipientId, steinhausPoint, metric);
			                    	
			                    	if (commonPrefixLengthWithRte > commonPrefixLength 
			                    			|| (respectNumOfCommonBitsInNextGroup && commonPrefixLengthWithRte == commonPrefixLength && numOfCommonBitsInNextGroupWithRte > numOfCommonBitsInNextGroup)
			                    			|| (commonPrefixLengthWithRte == commonPrefixLength && (respectNumOfCommonBitsInNextGroup == false || numOfCommonBitsInNextGroupWithRte == numOfCommonBitsInNextGroup) && distanceFromRte < distToRecipient)) {
			                            if (! nextHopsMap.containsKey(rteHash)) {
			                                nextHops.add(rte);
			                                nextHopsMap.put(rteHash, rte);
			                            }
			                            
			                        }
			                    	
		                		}	
		                		
		                	}

		            		
		                }
		            }
	        	}

        		
        		
            }
        	
        	        	
        	

        	//select the best hop:
        	if (! nextHops.isEmpty()) {
        		
        		int numNextHopsFoundNum = numNextHopsToFind;
        		if (numNextHopsFoundNum > nextHops.size()) numNextHopsFoundNum = nextHops.size();
        		bestNodes = new NodePointer[numNextHopsFoundNum];
        		for (int k = 0; k < numNextHopsFoundNum; k++) {
            		
        			//choose the best next hop in nextHops:
            		RoutingTableEntry bestNode = null;
                	int bestPrefixLength = 0;
                	int bestNumOfCommonBitsInNextGroup = 0;
                	double bestDistance = 0;
                	int bestIndex = 0;
                	
                	for (int i = 0; i < nextHops.size(); i++) {
                    	
                		RoutingTableEntry rte = nextHops.get(i);
                    	
                    	HyCubeNodeId rteNodeId = (HyCubeNodeId) rte.getNode().getNodeId();
                    	
                    	int commonPrefixLengthWithRte = HyCubeNodeId.calculatePrefixLength(rteNodeId, recipientId);
                    	
                    	int nextDigitRte = 0;
                    	int numOfCommonBitsInNextGroupWithRte = 0;
                    	if (respectNumOfCommonBitsInNextGroup) {
                    		nextDigitRte = rteNodeId.getDigitAsInt(commonPrefixLength);
                    		nextDigitRecipient = recipientId.getDigitAsInt(commonPrefixLength);
                    		numOfCommonBitsInNextGroupWithRte = dimensions - IntUtils.getHammingDistance(nextDigitRte, nextDigitRecipient);
                    	}

                    	double distanceFromRte = HyCubeNodeId.calculateDistance(rteNodeId, recipientId, steinhausPoint, metric);

                    	if (HyCubeNodeId.compareIds(rteNodeId, recipientId) || bestNode == null 
                    			|| commonPrefixLengthWithRte > bestPrefixLength 
                    			|| (respectNumOfCommonBitsInNextGroup && commonPrefixLengthWithRte == bestPrefixLength && numOfCommonBitsInNextGroupWithRte > bestNumOfCommonBitsInNextGroup)
                    			|| (commonPrefixLengthWithRte == bestPrefixLength && (respectNumOfCommonBitsInNextGroup == false || numOfCommonBitsInNextGroupWithRte == bestNumOfCommonBitsInNextGroup) && distanceFromRte < bestDistance)) {
                			bestNode = rte;
                			bestPrefixLength = commonPrefixLengthWithRte;
                			if (respectNumOfCommonBitsInNextGroup) bestNumOfCommonBitsInNextGroup = numOfCommonBitsInNextGroupWithRte;
                			bestDistance = distanceFromRte;
                			bestIndex = i;
                		}
                	}

        			
        			
        			//remove it from nextHops:
                	nextHops.remove(bestIndex);
                	nextHopsMap.remove(bestNode.getNodeIdHash());
                	
                	
                	//add it to the bestNodes:
        			bestNodes[k] = bestNode.getNode();
                	
        		}
        		
        		
        		int startIndex;
        		int endIndex;
        		
        		if (numNextHopsFoundNum >= numNextHops + numNodesToSkip || forceSkipRandomNumOfNodes) {
        			startIndex = numNodesToSkip;
        			endIndex = numNodesToSkip + numNextHops;
        			if (endIndex >= bestNodes.length) endIndex = bestNodes.length - 1;
        		}
        		else {
        			endIndex = bestNodes.length -1;
        			startIndex = endIndex - numNextHops;
        			if (startIndex < 0) startIndex = 0;
        		}
        		
        		if (startIndex > 0 && skipRandomNumberOfNodesIncludeExcactMatch == false) {
        			if (bestNodes[0].getNodeId().equals(recipientId)) { //the exact match is expected to be the first element in the sorted array
        				//include the exact match as the first element of the result array
        				int elemsReturned = endIndex = startIndex;
	        			startIndex--;
	        			if (elemsReturned == numNextHops) {
	        				//if limit the number of the returned nodes to the number specified in the method call
	        				endIndex--;
	        			}
	        			bestNodes[startIndex] = bestNodes[0];
        			}
        		}
        		
        		bestNodes = Arrays.copyOfRange(bestNodes, startIndex, endIndex+1);
        		
        		if (devLog.isTraceEnabled()) {
        			StringBuilder sb = new StringBuilder();
        			for (int i = 0; i < bestNodes.length; i++) {
        				sb.append(bestNodes[i].getNodeId().toHexString());
        				if (i < bestNodes.length - 1) sb.append(", ");
        			}
	        		devLog.trace("Next hops found: " + sb.toString());
	        	}

        		
            	        	
        	}

	        
        }
        else {	//PMH enabled
        	if (useRT2 && (!useRT2InFullScanWithPMH)) {
        		
        		if (devLog.isTraceEnabled()) {
        			devLog.trace("Checking RT2...");
        		}
        		
            	int additionalLevelsToCheckUp = 0;
                int additionalLevelsToCheckDown = 0;
                
            	for (int dim = 0; dim < dimensions; dim++) {
            		
                    double distInDim = HyCubeNodeId.calculateDistanceInDimension(nodeId, recipientId, dim);
                    
                    int logDistInDim = (int)(Math.log(distInDim)/Math.log(2));
                    int startLevel = logDistInDim - additionalLevelsToCheckDown;
                    int endLevel = logDistInDim + 1 + additionalLevelsToCheckUp;
                    
                    if (startLevel < 0) startLevel = 0;
                    if (endLevel > levels - 1) endLevel = levels - 1;

                    for (int level = startLevel; level <= endLevel; level++) {

                        int digit = levels - 1 - level;

                        HyCubeNodeId hypercube1 = nodeId.addBitInDimension(dim, digit);
                        HyCubeNodeId hypercube2 = nodeId.subBitInDimension(dim, digit);

                        HyCubeNodeId rt1HypercubePrefix;

                        if ((useRT1 && level == levels - 1) || HyCubeNodeId.compareIds(hypercube1.getSubID(0, levels - level - 1), nodeId.getSubID(0, levels - level - 1))) {
                            //the RT2 slot represents hypercube2; hycercube1 is represented by a RT1 slot
                            rt1HypercubePrefix = hypercube1.getSubID(0, levels - level);
                        }
                        else {
                            //the RT2 slot represents hypercube1; hypercube2 is represented by a RT1 slot
                            rt1HypercubePrefix = hypercube2.getSubID(0, levels - level);
                        }
                        

                        
                        
                        //---------------------------
                        //sibling hypercube from RT2:                        	
                        
                        //acquire the lock
                        rt2Lock.readLock().lock();

                        //copy the rt slot (synchronized with any modifications) and then operate on the copy
                        tmpRtSlotCopy.clear();
                        tmpRtSlotCopy.addAll(rt2[level][dim]);

                        //release the lock
                        rt2Lock.readLock().unlock();

                        for (RoutingTableEntry rte : tmpRtSlotCopy) {

                        	if (! rte.isEnabled()) continue;

                        	HyCubeNodeId rteNodeId = (HyCubeNodeId) rte.getNode().getNodeId();

                        	long rteHash = rte.getNodeIdHash();

                        	//skip the node with the requested id:
                        	if (parameters.isSkipTargetNode() && HyCubeNodeId.compareIds(rteNodeId, recipientId)) continue;

                        	if (parameters.isIncludeMoreDistantNodes()) {
                        		if (! nextHopsMap.containsKey(rteHash)) {
                        			nextHops.add(rte);
                        			nextHopsMap.put(rteHash, rte);
                        		}
                        	}
                        	else {
                        		//filter:
                        		double distanceFromRte = HyCubeNodeId.calculateDistance(rteNodeId, recipientId, steinhausPoint, metric);
                        		if (distanceFromRte < distToRecipient) {
                        			if (! nextHopsMap.containsKey(rteHash)) {
                        				nextHops.add(rte);
                        				nextHopsMap.put(rteHash, rte);
                        			}
                        		}
                        	}
                        }
                            

                        
                        
                        //---------------------------
                        //sibling hypercube from RT1:
                        
                        int hypercube = rt1HypercubePrefix.getDigitAsInt(levels - 1 - level);

                        //acquire the lock
                        rt1Lock.readLock().lock();

                        //copy the rt slot (synchronized with any modifications) and then operate on the copy
                        tmpRtSlotCopy.clear();
                        tmpRtSlotCopy.addAll(rt1[level][hypercube]);

                        //release the lock
                        rt1Lock.readLock().unlock();

                        for (RoutingTableEntry rte : tmpRtSlotCopy) {

                        	if (! rte.isEnabled()) continue;

                        	HyCubeNodeId rteNodeId = (HyCubeNodeId) rte.getNode().getNodeId();

                        	long rteHash = rte.getNodeIdHash();

                        	//skip the node with the requested id:
                        	if (parameters.isSkipTargetNode() && HyCubeNodeId.compareIds(rteNodeId, recipientId)) continue;

                        	if (parameters.isIncludeMoreDistantNodes()) {
                        		if (! nextHopsMap.containsKey(rteHash)) {
                        			nextHops.add(rte);
                        			nextHopsMap.put(rteHash, rte);
                        		}
                        	}
                        	else {
                        		//filter
                        		double distanceFromRte = HyCubeNodeId.calculateDistance(rteNodeId, recipientId, steinhausPoint, metric);
                        		if (distanceFromRte < distToRecipient) {
                        			if (! nextHopsMap.containsKey(rteHash)) {
                        				nextHops.add(rte);
                        				nextHopsMap.put(rteHash, rte);
                        			}
                        		}
                        	}
                        }



                    }
                }
        	}
        	
	        	
        	if (useNSInFullScanWithPMH) {
        		
	        	for (RoutingTableEntry rte : nsCopy) {
	        		
	        		if (! rte.isEnabled()) continue;
	        		
	        		HyCubeNodeId rteNodeId = (HyCubeNodeId) rte.getNode().getNodeId();
	        		
	        		//skip the node with the requested id:
	        		if (parameters.isSkipTargetNode() && HyCubeNodeId.compareIds(rteNodeId, recipientId)) continue;

	        		long rteHash = rte.getNodeIdHash();
		        	
		        	if (parameters.isIncludeMoreDistantNodes()) {
    	            	if (! nextHopsMap.containsKey(rteHash)) {
                            nextHops.add(rte);
                            nextHopsMap.put(rteHash, rte);
                        }
	            	}
		        	else {
		        		//filter:
	                   	double distanceFromRte = HyCubeNodeId.calculateDistance(rteNodeId, recipientId, steinhausPoint, metric);
	                   	if (distanceFromRte < distToRecipient) {
	                           if (! nextHopsMap.containsKey(rteHash)) {
	                            nextHops.add(rte);
	                            nextHopsMap.put(rteHash, rte);
	                        }
	                    }
		        	}                	
		        }
		        		        		
	        }

        	if (useRT1InFullScanWithPMH) {
	        	for (int i = 0; i < levels; i++) {
	                for (int ii = 0; ii < Integer.rotateLeft(2, dimensions); ii++) {
	                	
	                	//acquire the lock
       	            	rt1Lock.readLock().lock();
    	            	
        	            //copy the rt slot (synchronized with any modifications) and then operate on the copy
        	            tmpRtSlotCopy.clear();
        	            tmpRtSlotCopy.addAll(rt1[i][ii]);
        	            
        	            //release the lock
        	   	        rt1Lock.readLock().unlock();
		                	
		                for (RoutingTableEntry rte : tmpRtSlotCopy) {
		                	
		                	if (! rte.isEnabled()) continue;
		                	
		                	HyCubeNodeId rteNodeId = (HyCubeNodeId) rte.getNode().getNodeId();
		                	
		                	//skip the node with the requested id:
		                	if (parameters.isSkipTargetNode() && HyCubeNodeId.compareIds(rteNodeId, recipientId)) continue;

	                		long rteHash = rte.getNodeIdHash();
	                		
	                		if (parameters.isIncludeMoreDistantNodes()) {
	    	            		if (! nextHopsMap.containsKey(rteHash)) {
                                    nextHops.add(rte);
                                    nextHopsMap.put(rteHash, rte);
                                }
	    	            	}
	                		else {
	                			//filter:
                            	double distanceFromRte = HyCubeNodeId.calculateDistance(rteNodeId, recipientId, steinhausPoint, metric);
                            	if (distanceFromRte < distToRecipient) {
                                    if (! nextHopsMap.containsKey(rteHash)) {
                                        nextHops.add(rte);
                                        nextHopsMap.put(rteHash, rte);
                                    }
                                }
	                		}        	
	                	}

		        		
	                }
	            }
        	}		        	
	
	       	if (useRT2InFullScanWithPMH) {
		        for (int i = 0; i < levels; i++)
		        {
		            for (int ii = 0; ii < dimensions; ii++)
		            {
	                	
	                	//acquire the lock
    	            	rt2Lock.readLock().lock();
    	            	
    	            	//copy the rt slot (synchronized with any modifications) and then operate on the copy
    	            	tmpRtSlotCopy.clear();
    	            	tmpRtSlotCopy.addAll(rt2[i][ii]);
    	            	
    	            	//release the lock
    	   	            rt2Lock.readLock().unlock();
	                	
	                	for (RoutingTableEntry rte : tmpRtSlotCopy) {
	                		
	                		if (! rte.isEnabled()) continue;
	                		
	                		HyCubeNodeId rteNodeId = (HyCubeNodeId) rte.getNode().getNodeId();
	                		
	                		long rteHash = rte.getNodeIdHash();
	                		
	                		//skip the node with the requested id:
	                		if (parameters.isSkipTargetNode() && HyCubeNodeId.compareIds(rteNodeId, recipientId)) continue;
	                		
	                		if (parameters.isIncludeMoreDistantNodes()) {
	    	            		if (! nextHopsMap.containsKey(rteHash)) {
                                    nextHops.add(rte);
                                    nextHopsMap.put(rteHash, rte);
                                }
	    	            	}
	                		else {
	                			//filter
                            	double distanceFromRte = HyCubeNodeId.calculateDistance(rteNodeId, recipientId, steinhausPoint, metric);
                            	if (distanceFromRte < distToRecipient) {
                                    if (! nextHopsMap.containsKey(rteHash)) {
                                        nextHops.add(rte);
                                        nextHopsMap.put(rteHash, rte);
                                    }
                                }
	                		}
	                	}
		        		
	                }
	            }
        	}
        	

	       	
        	//choose the best next hops:
        	if (! nextHops.isEmpty()) {
        		
        		int numNextHopsFoundNum = numNextHopsToFind;
        		if (numNextHopsFoundNum > nextHops.size()) numNextHopsFoundNum = nextHops.size();
        		bestNodes = new NodePointer[numNextHopsFoundNum]; 
        		for (int k = 0; k < numNextHopsFoundNum; k++) {
        			//choose the best next hop (based only on the distance left):
                	RoutingTableEntry bestNode = null;
                	double bestDistance = 0;
                	int bestIndex = 0;
                	for (int i = 0; i < nextHops.size(); i++) {
                    	
                		RoutingTableEntry rte = nextHops.get(i);
                    	
                		HyCubeNodeId rteNodeId = (HyCubeNodeId) rte.getNode().getNodeId();
                    	
                    	double distanceFromRte = HyCubeNodeId.calculateDistance(rteNodeId, recipientId, steinhausPoint, metric);
                    	
                    	if (HyCubeNodeId.compareIds(rteNodeId, recipientId) || bestNode == null || distanceFromRte < bestDistance) {
                			bestNode = rte;
                			bestDistance = distanceFromRte;
                			bestIndex = i;
                		}
                	}
                    	
            		//remove it from nextHops:
                    nextHops.remove(bestIndex);
                    nextHopsMap.remove(bestNode.getNodeIdHash());
                    	
                    	
                    //add it to the bestNodes:
            		bestNodes[k] = bestNode.getNode();

                    	
        		}

        		int startIndex;
        		int endIndex;
        		
        		if (numNextHopsFoundNum >= numNextHops + numNodesToSkip || forceSkipRandomNumOfNodes) {
        			startIndex = numNodesToSkip;
        			endIndex = numNodesToSkip + numNextHops;
        			if (endIndex >= bestNodes.length) endIndex = bestNodes.length - 1;
        		}
        		else {
        			endIndex = bestNodes.length -1;
        			startIndex = endIndex - numNextHops;
        			if (startIndex < 0) startIndex = 0;
        		}
        		
        		
        		if (startIndex > 0 && skipRandomNumberOfNodesIncludeExcactMatch == false) {
        			if (bestNodes[0].getNodeId().equals(recipientId)) { //the exact match is expected to be the first element in the sorted array
        				//include the exact match as the first element of the result array
        				int elemsReturned = endIndex = startIndex;
	        			startIndex--;
	        			if (elemsReturned == numNextHops) {
	        				//if limit the number of the returned nodes to the number specified in the method call
	        				endIndex--;
	        			}
	        			bestNodes[startIndex] = bestNodes[0];
        			}
        		}
        		
        		bestNodes = Arrays.copyOfRange(bestNodes, startIndex, endIndex+1);
        		
        		if (devLog.isTraceEnabled()) {
        			StringBuilder sb = new StringBuilder();
        			for (int i = 0; i < bestNodes.length; i++) {
        				sb.append(bestNodes[i].getNodeId().toHexString());
        				if (i < bestNodes.length - 1) sb.append(", ");
        			}
	        		devLog.trace("Next hops found: " + sb.toString());
	        	}

        	
        	}
                  
	        	
        }	//PMH enabled
   
        
        
        
        
        //if no next hop found, enablePMH and/or disable Steinhaus transform and search for next hop again - high probability of getting a closer node
        boolean searchAgain = false;
        
        //if Steinhaus transform was enabled and the search was done already with PMH, disable Steinhaus transform for the message
        if ((bestNodes == null || bestNodes.length == 0) && parameters.isPMHApplied() && parameters.isSteinhausTransformApplied() && routeWithRegularMetricAfterSteinhaus == true) {
        	if (devLog.isTraceEnabled()) {
    			devLog.trace("Next hop not found - the search will be repeated with Stainhaus transform disabled.");
    		}
        	parameters.setSteinhausTransformApplied(false);
        	searchAgain = true;
        }
        
        //if PMH was disabled, enable PMH for the message
        if ((bestNodes == null || bestNodes.length == 0) && pmhWhenNoNextHop) {
	        if (parameters.isPMHApplied() == false && pmhEnabled == true) {
	        	if (devLog.isTraceEnabled()) {
	        		devLog.trace("Next hop not found - the search will be repeated with prefix mismatch heuristic enabled.");
	    		}
	        	parameters.setPMHApplied(true);
	        	searchAgain = true;
	        }
        }
                
        //and repeat the search
        if (searchAgain) {
        	if (devLog.isTraceEnabled()) {
    			devLog.trace("Next hop not found. The search will be repeated with new Stainhaus transform enabled and prefix mismatch heuristic enabled values.");
    		}
        	bestNodes = findNextHops(rt1, rt2, ns, rt1Lock, rt2Lock, nsLock, recipientId, parameters, numNextHops);
        }
        
        if (devLog.isTraceEnabled()) {
        	if (bestNodes != null && bestNodes.length > 0) {
				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < bestNodes.length; i++) {
					sb.append(bestNodes[i].getNodeId().toHexString());
					if (i < bestNodes.length - 1) sb.append(", ");
				}
	    		devLog.trace("Next hops found: " + sb.toString());
        	}
        	else {
    	        if (devLog.isTraceEnabled()) {
    				devLog.trace("Next hop not found.");
    			}
            }
    	}
        
        if (bestNodes == null) bestNodes = new NodePointer[0];
        return bestNodes;

		
	}


	
	


}
