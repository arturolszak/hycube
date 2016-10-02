package net.hycube.search;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.HyCubeNodeId;
import net.hycube.core.NodePointer;
import net.hycube.utils.HashMapUtils;

public class HyCubeSearchData {


	protected int searchId;
	protected HyCubeNodeId searchNodeId;
	
	protected SearchCallback searchCallback;
	protected Object callbackArg;
	
	protected short k;
	protected short alpha;
	protected short beta;
	protected short gamma;
	protected boolean ignoreTargetNode;
	protected boolean secureSearch;
	protected boolean skipRandomNextHops;
	
	protected boolean finalSearch;
	
	
	protected int closestNodesStoredNum;
	
	
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
	
	
	
	
	public int getSearchId() {
		return searchId;
	}


	public void setSearchId(int searchId) {
		this.searchId = searchId;
	}


	public HyCubeNodeId getSearchNodeId() {
		return searchNodeId;
	}


	public void setSearchNodeId(HyCubeNodeId searchNodeId) {
		this.searchNodeId = searchNodeId;
	}


	public SearchCallback getSearchCallback() {
		return searchCallback;
	}


	public void setSearchCallback(SearchCallback searchCallback) {
		this.searchCallback = searchCallback;
	}


	public Object getCallbackArg() {
		return callbackArg;
	}


	public void setCallbackArg(Object callbackArg) {
		this.callbackArg = callbackArg;
	}


	public short getK() {
		return k;
	}


	public void setK(short k) {
		this.k = k;
	}


	public short getAlpha() {
		return alpha;
	}


	public void setAlpha(short alpha) {
		this.alpha = alpha;
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


	public boolean isIgnoreTargetNode() {
		return ignoreTargetNode;
	}


	public void setIgnoreTargetNode(boolean ignoreTargetNode) {
		this.ignoreTargetNode = ignoreTargetNode;
	}


	public boolean isSecureSearch() {
		return secureSearch;
	}


	public void setSecureSearch(boolean secureSearch) {
		this.secureSearch = secureSearch;
	}


	public boolean isSkipRandomNextHops() {
		return skipRandomNextHops;
	}


	public void setSkipRandomNextHops(boolean skipRandomNextHops) {
		this.skipRandomNextHops = skipRandomNextHops;
	}

	
	public boolean isFinalSearch() {
		return finalSearch;
	}


	public void setFinalSearch(boolean finalSearch) {
		this.finalSearch = finalSearch;
	}


	public int getClosestNodesStoredNum() {
		return closestNodesStoredNum;
	}


	public void setClosestNodesStoredNum(int closestNodesStoredNum) {
		this.closestNodesStoredNum = closestNodesStoredNum;
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

	
	
	
	
	public HyCubeSearchData(int searchId, HyCubeNodeId searchNodeId, short k, boolean ignoreTargetNode, short alpha, short beta, short gamma, boolean secureSearch, boolean skipRandomNextHops) {
		
		this.searchId = searchId;
		
		this.searchNodeId = searchNodeId;
		
		this.k = k;
		
		this.alpha = alpha;
		this.beta = beta;
		this.gamma = gamma;
		this.ignoreTargetNode = ignoreTargetNode;
		this.secureSearch = secureSearch;
		this.skipRandomNextHops = skipRandomNextHops;
		
		this.closestNodesStoredNum = k;
		if (alpha > this.closestNodesStoredNum) this.closestNodesStoredNum = alpha;
		if (gamma > this.closestNodesStoredNum) this.closestNodesStoredNum = gamma;
		
		this.closestNodes = new LinkedList<NodePointer>();
		this.distances = new LinkedList<Double>();
		this.closestNodesSet = new HashSet<Long>(HashMapUtils.getHashMapCapacityForElementsNum(this.closestNodesStoredNum, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		this.pmhApplied = new HashMap<Long, Boolean>(HashMapUtils.getHashMapCapacityForElementsNum(this.closestNodesStoredNum, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		this.steinhausTransformApplied = new HashMap<Long, Boolean>(HashMapUtils.getHashMapCapacityForElementsNum(this.closestNodesStoredNum, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		this.steinhausPoints = new HashMap<Long, HyCubeNodeId>(HashMapUtils.getHashMapCapacityForElementsNum(this.closestNodesStoredNum, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		this.nodesRequested = new HashSet<Long>(HashMapUtils.getHashMapCapacityForElementsNum(this.closestNodesStoredNum, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		this.nodesResponded = new HashSet<Long>(HashMapUtils.getHashMapCapacityForElementsNum(this.closestNodesStoredNum, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		this.nodesRequestedFinal = new HashSet<Long>(HashMapUtils.getHashMapCapacityForElementsNum(this.closestNodesStoredNum, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		this.nodesRespondedFinal = new HashSet<Long>(HashMapUtils.getHashMapCapacityForElementsNum(this.closestNodesStoredNum, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		
		this.finalSearch = false;
		
	}

	
	
	
	public void discard() {
		
	}
	
	
	
}
