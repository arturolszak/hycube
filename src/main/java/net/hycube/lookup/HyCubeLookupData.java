package net.hycube.lookup;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.HyCubeNodeId;
import net.hycube.core.NodePointer;
import net.hycube.utils.HashMapUtils;

public class HyCubeLookupData {



	protected int lookupId;
	protected HyCubeNodeId lookupNodeId;
	
	protected LookupCallback lookupCallback;
	protected Object callbackArg;
	
	protected short beta;
	protected short gamma;
	protected boolean secureLookup;
	protected boolean skipRandomNextHops;
	
	protected boolean finalLookup;
	
	
	protected double minDistance;
	protected NodePointer closestNode;
	
	protected NodePointer nextNode;
	protected boolean nextNodePmhApplied;
	protected boolean nextNodeSteinhausTransformApplied;
	protected HyCubeNodeId nextSteinhausPoint;
	
	protected LinkedList<NodePointer> closestNodes;
	protected LinkedList<Double> distances;
	protected HashSet<Long> closestNodesSet;	//ordered from closest to the most distant
	protected HashMap<Long, Boolean> pmhApplied;
	protected HashMap<Long, Boolean> steinhausTransformApplied;
	protected HashMap<Long, HyCubeNodeId> steinhausPoints;
	protected HashSet<Long> nodesRequested;
	protected HashSet<Long> nodesResponded;
	protected HashSet<Long> nodesRequestedFinal;
	protected HashSet<Long> nodesRespondedFinal;
	
	
	
	
	
	public int getLookupId() {
		return lookupId;
	}

	public void setLookupId(int lookupId) {
		this.lookupId = lookupId;
	}


	public HyCubeNodeId getLookupNodeId() {
		return lookupNodeId;
	}


	public void setLookupNodeId(HyCubeNodeId lookupNodeId) {
		this.lookupNodeId = lookupNodeId;
	}


	public LookupCallback getLookupCallback() {
		return lookupCallback;
	}


	public void setLookupCallback(LookupCallback lookupCallback) {
		this.lookupCallback = lookupCallback;
	}


	public Object getCallbackArg() {
		return callbackArg;
	}


	public void setCallbackArg(Object callbackArg) {
		this.callbackArg = callbackArg;
	}


	public short getBeta() {
		return beta;
	}


	public void setBeta(short beta) {
		this.beta = beta;
	}


	public short getGamma() {
		return gamma;
	}


	public void setGamma(short gamma) {
		this.gamma = gamma;
	}


	public boolean isSecureLookup() {
		return secureLookup;
	}


	public void setSecureLookup(boolean secureLookup) {
		this.secureLookup = secureLookup;
	}


	public boolean isSkipRandomNextHops() {
		return skipRandomNextHops;
	}


	public void setSkipRandomNextHops(boolean skipRandomNextHops) {
		this.skipRandomNextHops = skipRandomNextHops;
	}

	
	public boolean isFinalLookup() {
		return finalLookup;
	}


	public void setFinalLookup(boolean finalLookup) {
		this.finalLookup = finalLookup;
	}


	public double getMinDistance() {
		return minDistance;
	}


	public void setMinDistance(double minDistance) {
		this.minDistance = minDistance;
	}


	public NodePointer getClosestNode() {
		return closestNode;
	}


	public void setClosestNode(NodePointer closestNode) {
		this.closestNode = closestNode;
	}

	
	public NodePointer getNextNode() {
		return nextNode;
	}


	public void setNextNode(NodePointer nextNode) {
		this.nextNode = nextNode;
	}

	
	public boolean isNextNodePmhApplied() {
		return nextNodePmhApplied;
	}

	public void setNextNodePmhApplied(boolean nextNodePmhApplied) {
		this.nextNodePmhApplied = nextNodePmhApplied;
	}

	public boolean isNextNodeSteinhausTransformApplied() {
		return nextNodeSteinhausTransformApplied;
	}

	public void setNextNodeSteinhausTransformApplied(
			boolean nextNodeSteinhausTransformApplied) {
		this.nextNodeSteinhausTransformApplied = nextNodeSteinhausTransformApplied;
	}

	public HyCubeNodeId getNextSteinhausPoint() {
		return nextSteinhausPoint;
	}

	public void setNextSteinhausPoint(HyCubeNodeId nextSteinhausPoint) {
		this.nextSteinhausPoint = nextSteinhausPoint;
	}
	
	
	public LinkedList<NodePointer> getClosestNodes() {
		return closestNodes;
	}


	public void setClosestNodes(LinkedList<NodePointer> closestNodes) {
		this.closestNodes = closestNodes;
	}


	public LinkedList<Double> getDistances() {
		return distances;
	}


	public void setDistances(LinkedList<Double> distances) {
		this.distances = distances;
	}


	public HashSet<Long> getClosestNodesSet() {
		return closestNodesSet;
	}


	public void setClosestNodesSet(HashSet<Long> closestNodesSet) {
		this.closestNodesSet = closestNodesSet;
	}


	public HashMap<Long, Boolean> getPmhApplied() {
		return pmhApplied;
	}


	public void setPmhApplied(HashMap<Long, Boolean> pmhApplied) {
		this.pmhApplied = pmhApplied;
	}


	public HashMap<Long, Boolean> getSteinhausTransformApplied() {
		return steinhausTransformApplied;
	}


	public void setSteinhausTransformApplied(
			HashMap<Long, Boolean> steinhausTransformApplied) {
		this.steinhausTransformApplied = steinhausTransformApplied;
	}


	public HashMap<Long, HyCubeNodeId> getSteinhausPoints() {
		return steinhausPoints;
	}


	public void setSteinhausPoints(HashMap<Long, HyCubeNodeId> steinhausPoints) {
		this.steinhausPoints = steinhausPoints;
	}


	public HashSet<Long> getNodesRequested() {
		return nodesRequested;
	}


	public void setNodesRequested(HashSet<Long> nodesRequested) {
		this.nodesRequested = nodesRequested;
	}


	public HashSet<Long> getNodesResponded() {
		return nodesResponded;
	}


	public void setNodesResponded(HashSet<Long> nodesResponded) {
		this.nodesResponded = nodesResponded;
	}


	public HashSet<Long> getNodesRequestedFinal() {
		return nodesRequestedFinal;
	}


	public void setNodesRequestedFinal(HashSet<Long> nodesRequestedFinal) {
		this.nodesRequestedFinal = nodesRequestedFinal;
	}


	public HashSet<Long> getNodesRespondedFinal() {
		return nodesRespondedFinal;
	}

	
	public void setNodesRespondedFinal(HashSet<Long> nodesRespondedFinal) {
		this.nodesRespondedFinal = nodesRespondedFinal;
	}
	
	
	
	
	
	public HyCubeLookupData(int lookupId, HyCubeNodeId lookupNodeId, short beta, short gamma, boolean secureLookup, boolean skipRandomNextHops) {
		
		this.lookupId = lookupId;
		
		this.lookupNodeId = lookupNodeId;
		
		this.beta = beta;
		this.gamma = gamma;
		
		this.secureLookup = secureLookup;
		this.skipRandomNextHops = skipRandomNextHops;
		
		this.closestNodes = new LinkedList<NodePointer>();
		this.distances = new LinkedList<Double>();
		this.closestNodesSet = new HashSet<Long>(HashMapUtils.getHashMapCapacityForElementsNum(gamma, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		this.pmhApplied = new HashMap<Long, Boolean>(HashMapUtils.getHashMapCapacityForElementsNum(gamma, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		this.steinhausTransformApplied = new HashMap<Long, Boolean>(HashMapUtils.getHashMapCapacityForElementsNum(gamma, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		this.steinhausPoints = new HashMap<Long, HyCubeNodeId>(HashMapUtils.getHashMapCapacityForElementsNum(gamma, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		this.nodesRequested = new HashSet<Long>(HashMapUtils.getHashMapCapacityForElementsNum(gamma, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		this.nodesResponded = new HashSet<Long>(HashMapUtils.getHashMapCapacityForElementsNum(gamma, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		this.nodesRequestedFinal = new HashSet<Long>(HashMapUtils.getHashMapCapacityForElementsNum(gamma, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		this.nodesRespondedFinal = new HashSet<Long>(HashMapUtils.getHashMapCapacityForElementsNum(gamma, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		
		
	}
	
	
	
	
	
	public void discard() {
		
	}
	
	
	
}
