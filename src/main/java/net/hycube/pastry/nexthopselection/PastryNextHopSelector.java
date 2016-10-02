package net.hycube.pastry.nexthopselection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.HyCubeNodeId;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodeId;
import net.hycube.core.NodePointer;
import net.hycube.core.RoutingTable;
import net.hycube.core.RoutingTableEntry;
import net.hycube.environment.NodeProperties;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.logging.LogHelper;
import net.hycube.nexthopselection.HyCubePrefixMismatchHeuristicMode;
import net.hycube.nexthopselection.NextHopSelectionParameters;
import net.hycube.nexthopselection.NextHopSelector;
import net.hycube.pastry.core.PastryRoutingTable;
import net.hycube.utils.HashMapUtils;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public class PastryNextHopSelector extends NextHopSelector {

	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(PastryNextHopSelector.class);
	
	
	
	protected static final String PROP_KEY_DIMENSIONS = "Dimensions";
	protected static final String PROP_KEY_LEVELS = "Levels";
	protected static final String PROP_KEY_USE_RT = "UseRT";
	protected static final String PROP_KEY_USE_LS = "UseLS";
	protected static final String PROP_KEY_USE_RT_IN_FULL_SCAN_WITHOUT_PMH = "UseRTInFullScanWithoutPMH";
	protected static final String PROP_KEY_USE_LS_IN_FULL_SCAN_WITHOUT_PMH = "UseLSInFullScanWithoutPMH";
	protected static final String PROP_KEY_USE_RT_IN_FULL_SCAN_WITH_PMH = "UseRTInFullScanWithPMH";
	protected static final String PROP_KEY_USE_LS_IN_FULL_SCAN_WITH_PMH = "UseLSInFullScanWithPMH";
	
	protected static final String PROP_KEY_PREFIX_MISMATCH_HEURISTIC_ENABLED = "PrefixMismatchHeuristicEnabled";
	protected static final String PROP_KEY_PREFIX_MISMATCH_HEURISTIC_MODE = "PrefixMismatchHeuristicMode";
	protected static final String PROP_KEY_PREFIX_MISMATCH_HEURISTIC_FACTOR = "PrefixMismatchHeuristicFactor";
	protected static final String PROP_KEY_PREFIX_MISMATCH_HEURISTIC_WHEN_NO_NEXT_HOP = "PrefixMismatchHeuristicWhenNoNextHop";
	
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
	protected PastryRoutingTable routingTable;
	protected NodePointer selfNodePointer;
	
	protected boolean useSecureRouting;
	protected boolean useLS;
	protected boolean useRT;
	protected boolean useLSInFullScanWithoutPMH;
	protected boolean useRTInFullScanWithoutPMH;
	protected boolean useLSInFullScanWithPMH;
	protected boolean useRTInFullScanWithPMH;
	protected int dimensions;
	protected int digitsCount;
	
	protected boolean pmhEnabled;
	protected HyCubePrefixMismatchHeuristicMode pmhMode;
	protected double pmhFactor;
	protected boolean pmhWhenNoNextHop;
	
	
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
		super.initialize(nodeAccessor, properties);
		
		this.nodeId = nodeId;
		if (!(routingTable instanceof PastryRoutingTable)) throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "The routing table is expected to be an instance of: " + PastryRoutingTable.class.getName());
		this.routingTable = (PastryRoutingTable) routingTable;
		this.selfNodePointer = selfNodePointer;
		
		//parameters
		try {
			useSecureRouting = (Boolean) properties.getProperty(PROP_KEY_USE_SECURE_ROUTING, MappedType.BOOLEAN);

			useLS = (Boolean) properties.getProperty(PROP_KEY_USE_LS, MappedType.BOOLEAN);

			useRT = (Boolean) properties.getProperty(PROP_KEY_USE_RT, MappedType.BOOLEAN);
			
			useLSInFullScanWithoutPMH = (Boolean) properties.getProperty(PROP_KEY_USE_LS_IN_FULL_SCAN_WITHOUT_PMH, MappedType.BOOLEAN);

			useRTInFullScanWithoutPMH = (Boolean) properties.getProperty(PROP_KEY_USE_RT_IN_FULL_SCAN_WITHOUT_PMH, MappedType.BOOLEAN);

			useLSInFullScanWithPMH = (Boolean) properties.getProperty(PROP_KEY_USE_LS_IN_FULL_SCAN_WITH_PMH, MappedType.BOOLEAN);

			useRTInFullScanWithPMH = (Boolean) properties.getProperty(PROP_KEY_USE_RT_IN_FULL_SCAN_WITH_PMH, MappedType.BOOLEAN);
			
			dimensions = (Integer) properties.getProperty(PROP_KEY_DIMENSIONS, MappedType.INT);
			if (dimensions <= 0) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize PastryNextHopSelector instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_DIMENSIONS) + ".");
			}

			digitsCount = (Integer) properties.getProperty(PROP_KEY_LEVELS, MappedType.INT);
			if (digitsCount <= 0) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize PastryNextHopSelector instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_LEVELS) + ".");
			}

			
			pmhEnabled = (Boolean) properties.getProperty(PROP_KEY_PREFIX_MISMATCH_HEURISTIC_ENABLED, MappedType.BOOLEAN);
			
			pmhMode = (HyCubePrefixMismatchHeuristicMode) properties.getEnumProperty(PROP_KEY_PREFIX_MISMATCH_HEURISTIC_MODE, HyCubePrefixMismatchHeuristicMode.class);

			pmhFactor = (Double) properties.getProperty(PROP_KEY_PREFIX_MISMATCH_HEURISTIC_FACTOR, MappedType.DOUBLE);
			
			pmhWhenNoNextHop = (Boolean) properties.getProperty(PROP_KEY_PREFIX_MISMATCH_HEURISTIC_WHEN_NO_NEXT_HOP, MappedType.BOOLEAN);

			
			
			
			
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
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize PastryNextHopSelector instance. Invalid parameter value: " + e.getKey() + ".", e);
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
		return findNextHops((HyCubeNodeId) recipientId, (PastryNextHopSelectionParameters) parameters, numNextHops);
	}
	
	
	

	
	
	public NodePointer[] findNextHops(HyCubeNodeId recipientId, PastryNextHopSelectionParameters parameters, int numNextHops) {
		
		PastryRoutingTable rt = (PastryRoutingTable)routingTable;
		
		if (useSecureRouting && parameters.isSecureRoutingApplied()) {
			return findNextHops(rt.getSecRoutingTable(), rt.getLeafSet(), rt.getSecRtLock(), rt.getLsLock(), recipientId, parameters, numNextHops);
		}
		else {
			return findNextHops(rt.getRoutingTable(), rt.getLeafSet(), rt.getRtLock(), rt.getLsLock(), recipientId, parameters, numNextHops);
		}
		
	}


	
	protected NodePointer[] findNextHops(List<RoutingTableEntry>[][] rt, List<RoutingTableEntry> ls, ReentrantReadWriteLock rtLock, ReentrantReadWriteLock lsLock,  HyCubeNodeId recipientId, PastryNextHopSelectionParameters parameters, int numNextHops) {
		
		NodePointer[] bestNodes = null;
		
        HyCubeNodeId nodeId = (HyCubeNodeId)this.nodeId;
		
		int levels = digitsCount;
		
        
        List<RoutingTableEntry> nextHops = new ArrayList<RoutingTableEntry>(GlobalConstants.INITIAL_FOUND_NEXT_HOPS_COLLECTION_SIZE);
        Map<Long, RoutingTableEntry> nextHopsMap = new HashMap<Long, RoutingTableEntry>(HashMapUtils.getHashMapCapacityForElementsNum(GlobalConstants.INITIAL_FOUND_NEXT_HOPS_COLLECTION_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);

        double distToRecipient = HyCubeNodeId.calculateRingDistance(nodeId, recipientId);
        int commonPrefixLength = HyCubeNodeId.calculatePrefixLength(nodeId, recipientId);

		
        
        
        
        //acquire the ns lock:
        lsLock.readLock().lock();
        
        //copy the ls:
        ArrayList<RoutingTableEntry> lsCopy = new ArrayList<RoutingTableEntry>(ls);
        
        //release the ls lock
        lsLock.readLock().unlock();
        
        
        
        //temporary routing table slot copy
        ArrayList<RoutingTableEntry> tmpRtSlotCopy = new ArrayList<RoutingTableEntry>(routingTable.getRoutingTableSlotSize());
        
        
        
		
		//actualize PMH enabled flag for the message:
		
        //calculate the average and maximum node distance in the neighborhood set:
        double maxLSDist = 0;
        double avgLSDist = 0;
        
        for (RoutingTableEntry rte : lsCopy) {
        	double distToNode = rte.getDistance();
        	avgLSDist += distToNode;
        	if (distToNode > maxLSDist) maxLSDist = distToNode;
        }
        avgLSDist = avgLSDist / lsCopy.size();
		
        //check whether to enable the prefix mismatch heuristic for the message
        if (parameters.isPMHApplied() == false && pmhEnabled) {
            switch (pmhMode) {
	            case AVG:
	            	if (pmhFactor == 0 || distToRecipient < avgLSDist * pmhFactor) {
	            		parameters.setPMHApplied(true);
	            	}
	            	break;
	            case MAX:
	            	if (pmhFactor == 0 || distToRecipient < maxLSDist * pmhFactor) {
	            		parameters.setPMHApplied(true);
	            	}
	            	break;
	        }
            
        }

        if (devLog.isTraceEnabled()) {
			devLog.trace("Prefix mismatch heuristic enabled: " + parameters.isPMHApplied());
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
        
        
        
        
        //check if the LS contains the recipient node:
        if (useLS && (! parameters.isSkipTargetNode())) {
	        for (RoutingTableEntry rte : lsCopy) {
	        	if (! rte.isEnabled()) continue;
	    		if (rte.getNode().getNodeId().equals(recipientId)) {
	    			if (devLog.isTraceEnabled()) {
	    				devLog.trace("The destination node was found in LS: " + rte.getNode().getNodeId().toHexString());
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
        	
        	int rtLevel = levels - commonPrefixLength - 1;
        	int rtHypercube = 0;
        	
        	if (rtLevel != -1) {	//if the requested node id is equal to self, commonPrefixLength equals levels
        		rtHypercube = recipientId.getDigitAsInt(levels - 1 - rtLevel);
        		
        	
	        	//primary routing table:
	        	if (useRT) {
	        		
	        		if (devLog.isTraceEnabled()) {
	        			devLog.trace("Checking RT...");
	        		}
	        		
	        		//acquire the lock
	            	rtLock.readLock().lock();
	            	
	            	//copy the rt slot (synchronized with any modifications) and then operate on the copy
	            	tmpRtSlotCopy.clear();
	            	tmpRtSlotCopy.addAll(rt[rtLevel][rtHypercube]);
	            	
	            	//release the lock
	   	            rtLock.readLock().unlock();
	            	
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

        		if (useRT && (!useRTInFullScanWithoutPMH)) {
        			
        			if (rtLevel != -1) {
        			
	        			for (int hypercube = 0; hypercube < ((long)1 << dimensions); hypercube++) {
	        				if (hypercube != rtHypercube) {
	
	        					//acquire the lock
	        	            	rtLock.readLock().lock();
	        	            	
	        	            	//copy the rt slot (synchronized with any modifications) and then operate on the copy
	        	            	tmpRtSlotCopy.clear();
	        	            	tmpRtSlotCopy.addAll(rt[rtLevel][hypercube]);
	        	            	
	        	            	//release the lock
	        	   	            rtLock.readLock().unlock();
	        					
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
		                            	double distanceFromRte = HyCubeNodeId.calculateRingDistance(rteNodeId, recipientId);                            	
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
        		}

        		
	        	if (useLSInFullScanWithoutPMH) {
	        		
		        	for (RoutingTableEntry rte : lsCopy) {
		        		
		        		HyCubeNodeId rteNodeId = (HyCubeNodeId) rte.getNode().getNodeId();
    	            	
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
	    	            	double distanceFromRte = HyCubeNodeId.calculateRingDistance(rteNodeId, recipientId);
	    	            	
	    	            	if (commonPrefixLengthWithRte > commonPrefixLength
	                    			|| (commonPrefixLengthWithRte == commonPrefixLength && distanceFromRte < distToRecipient)) {
	                            if (! nextHopsMap.containsKey(rteHash)) {
	                                nextHops.add(rte);
	                                nextHopsMap.put(rteHash, rte);
	                            }
	                        }
			        		
			        	}
		        		
		        		
		        	}
	        	}
		  
	        	if (useRTInFullScanWithoutPMH) {
	        		
	        		int maxLevelToCheck = -1;
	        		if (parameters.isIncludeMoreDistantNodes()) {
	        			//all levels should be checked
	        			maxLevelToCheck = levels - 1;
	        		}
	        		else {
	        			if (rtLevel != -1) {
			        		//only lower levels must still be checked
	        				maxLevelToCheck = rtLevel;
	        			}
	        			else {
	        				maxLevelToCheck = -1;	//don't check, no closer nodes
	        			}
	        		}
	        		
        			//level (levels - commonPrefixLength-1) was already checked
	        		//only the lower levels must still be checked
        			for (int level = maxLevelToCheck; level >= 0; level--) { 
        				
        				for (int hypercube = 0; hypercube < ((long)1 << dimensions); hypercube++) {

        					if (level == rtLevel && hypercube == rtHypercube) continue;	//was already checked
        					
        					//acquire the lock
	        	            rtLock.readLock().lock();
	        	            
	        	            //copy the rt slot (synchronized with any modifications) and then operate on the copy
	        	            tmpRtSlotCopy.clear();
	        	            tmpRtSlotCopy.addAll(rt[level][hypercube]);
	        	            
	        	            //release the lock
	        	   	        rtLock.readLock().unlock();
	        				
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
			    	            	double distanceFromRte = HyCubeNodeId.calculateRingDistance(rteNodeId, recipientId);
			    	            	
			    	            	if (commonPrefixLengthWithRte > commonPrefixLength
			                    			|| (commonPrefixLengthWithRte == commonPrefixLength && distanceFromRte < distToRecipient)) {
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
                	double bestDistance = 0;
                	int bestIndex = 0;
                	
                	for (int i = 0; i < nextHops.size(); i++) {
                    	
                		RoutingTableEntry rte = nextHops.get(i);
                    	
                    	HyCubeNodeId rteNodeId = (HyCubeNodeId) rte.getNode().getNodeId();
                    	
                    	int commonPrefixLengthWithRte = HyCubeNodeId.calculatePrefixLength(rteNodeId, recipientId);

                    	double distanceFromRte = HyCubeNodeId.calculateRingDistance(rteNodeId, recipientId);

                    	if (HyCubeNodeId.compareIds(rteNodeId, recipientId) || bestNode == null 
                    			|| commonPrefixLengthWithRte > bestPrefixLength 
                    			|| (commonPrefixLengthWithRte == bestPrefixLength && distanceFromRte < bestDistance)) {
                			bestNode = rte;
                			bestPrefixLength = commonPrefixLengthWithRte;
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

        	if (useLSInFullScanWithPMH) {
        		for (RoutingTableEntry rte : lsCopy) {
        			
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
		            	double distanceFromRte = HyCubeNodeId.calculateRingDistance(rteNodeId, recipientId);
		            	if (distanceFromRte < distToRecipient) {
	                        if (! nextHopsMap.containsKey(rteHash)) {
	                            nextHops.add(rte);
	                            nextHopsMap.put(rteHash, rte);
	                        }
	                    }
	        		}
	        		
	        		
        		}
        	}

        	
        	if (useRTInFullScanWithPMH) {
	        	for (int i = 0; i < levels; i++) {
	                for (int ii = 0; ii < Integer.rotateLeft(2, dimensions); ii++) {
	                	
	                	//acquire the lock
	                	rtLock.readLock().lock();
	                	
	                	//copy the rt slot (synchronized with any modifications) and then operate on the copy
	                	tmpRtSlotCopy.clear();
	                	tmpRtSlotCopy.addAll(rt[i][ii]);
	                	
	                	//release the lock
	       	            rtLock.readLock().unlock();
	                	
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
	                        	double distanceFromRte = HyCubeNodeId.calculateRingDistance(rteNodeId, recipientId);
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
                    	
                    	double distanceFromRte = HyCubeNodeId.calculateRingDistance(rteNodeId, recipientId);
                    	
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
        	bestNodes = findNextHops(rt, ls, rtLock, lsLock, recipientId, parameters, numNextHops);
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
