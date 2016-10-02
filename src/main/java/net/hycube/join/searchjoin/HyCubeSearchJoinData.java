package net.hycube.join.searchjoin;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.HyCubeNodeId;
import net.hycube.core.NodePointer;
import net.hycube.join.JoinCallback;
import net.hycube.utils.HashMapUtils;

public class HyCubeSearchJoinData {


	protected int joinId;
	protected String[] bootstrapNodeAddresses;
	
	protected JoinCallback joinCallback;
	protected Object callbackArg;
	
	protected short alpha;
	protected short beta;
	protected short gamma;
	
	protected boolean secureSearch;
	protected boolean skipRandomNextHops;
	
	protected boolean initialSearch;
	protected boolean finalSearch;
	
	protected boolean publicNetworkAddressDiscovered;
	
	
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
	
	protected HashSet<String> bootstrapNodes;
	protected HashSet<String> bootstrapNodesResponded;
	
	
	
	
	
	public int getJoinId() {
		return joinId;
	}


	public void setJoinId(int joinId) {
		this.joinId = joinId;
	}


	public String[] getBootstrapNodeAddresses() {
		return bootstrapNodeAddresses;
	}


	public void setBootstrapNodeAddresses(String[] bootstrapNodeAddresses) {
		this.bootstrapNodeAddresses = bootstrapNodeAddresses;
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

	
	public boolean isInitialSearch() {
		return initialSearch;
	}


	public void setInitialSearch(boolean initialSearch) {
		this.initialSearch = initialSearch;
	}


	public boolean isFinalSearch() {
		return finalSearch;
	}


	public void setFinalSearch(boolean finalSearch) {
		this.finalSearch = finalSearch;
	}


	public boolean isPublicNetworkAddressDiscovered() {
		return publicNetworkAddressDiscovered;
	}


	public void setPublicNetworkAddressDiscovered(
			boolean publicNetworkAddressDiscovered) {
		this.publicNetworkAddressDiscovered = publicNetworkAddressDiscovered;
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


	public HashSet<String> getBootstrapNodes() {
		return bootstrapNodes;
	}


	public void setBootstrapNodes(HashSet<String> bootstrapNodes) {
		this.bootstrapNodes = bootstrapNodes;
	}


	public HashSet<String> getBootstrapNodesResponded() {
		return bootstrapNodesResponded;
	}


	public void setBootstrapNodesResponded(HashSet<String> bootstrapNodesResponded) {
		this.bootstrapNodesResponded = bootstrapNodesResponded;
	}
	
	
	
	
	
	
	public HyCubeSearchJoinData(int joinId, String[] bootstrapNodeAddresses, short alpha, short beta, short gamma, boolean secureSearch, boolean skipRandomNextHops) {
		
		this.joinId = joinId;
		
		this.bootstrapNodeAddresses = bootstrapNodeAddresses;
		
		this.alpha = alpha;
		this.beta = beta;
		this.gamma = gamma;
		
		this.secureSearch = secureSearch;
		this.skipRandomNextHops = skipRandomNextHops;
		
		this.closestNodesStoredNum = 1;
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
		
		this.bootstrapNodes = new HashSet<String>(HashMapUtils.getHashMapCapacityForElementsNum(bootstrapNodeAddresses.length, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		this.bootstrapNodesResponded = new HashSet<String>(HashMapUtils.getHashMapCapacityForElementsNum(bootstrapNodeAddresses.length, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		
		this.initialSearch = false;
		this.finalSearch = false;
		
		this.publicNetworkAddressDiscovered = false;
		
	}

	
	public JoinCallback getJoinCallback() {
		return joinCallback;
	}
	
	public void setJoinCallback(JoinCallback joinCallback) {
		this.joinCallback = joinCallback;
	}

	
	
	public Object getCallbackArg() {
		return callbackArg;
	}
	
	public void setCallbackArg(Object callbackArg) {
		this.callbackArg = callbackArg;
	}
	
	
	
	public void discard() {
		
	}
	
	
	
}
