/**
 * 
 */
package net.hycube.core;

import java.util.HashMap;

import net.hycube.configuration.GlobalConstants;
import net.hycube.utils.HashMapUtils;


/**
 * @author Artur Olszak
 * Routing table entry
 * Because instances of this class are used by multiple threads concurrently (updates and reads), all methods accessing the data that may be read/modified concurrently are synchronized
 * 
 */
public class RoutingTableEntry {
	
	protected static double INITIAL_PING_RESPONSE_INDICATOR_VALUE = 2;
	
	
	/**
	 * Node info
	 */
	protected NodePointer node;

	/**
	 * Date and time of the routing table entry creation
	 */
	protected long created;

	/*
	 * Distance to the node - cached
	 */
	protected double distance;
	
	/*
	 * Node ID hash - cached
	 */
	protected long nodeIdHash;
	
	
	/*
	 * Stores cashed data for various classes operating on RoutingTableEntry objects 
	 */
	protected HashMap<String, Object> dataMap;
	

	
	/*
	 * Optional - contains a reference to the outer object for this routing table entry (e.g. routing table slot, routing table - semantics rt specific)
	 */
	protected Object outerRef;
	
	
	
	protected boolean enabled;
	
	protected boolean discarded;
	
	
	
	public NodePointer getNode() {
		return node;
	}

	public void setNode(NodePointer node) {
		this.node = node;
	}


	
	public synchronized long getCreated() {
		return created;
	}

	public synchronized void setCreated(long created) {
		this.created = created;
	}

	public double getDistance() {
		return distance;
	}
	
	public void setDistance(double distance) {
		this.distance = distance;
	}
	
	public long getNodeIdHash() {
		return nodeIdHash;
	}
	
	public void setNodeIdHash(long nodeIdHash) {
		this.nodeIdHash = nodeIdHash;
	}
	
	
	
	public Object getData(String key, Object defaultValue) {
		if (dataMap.containsKey(key)) return dataMap.get(key);
		else return defaultValue;
	}
	
	public Object getData(String key) {
		return dataMap.get(key);
	}
	
	public void setData(String key, Object data) {
		this.dataMap.put(key, data);
	}
	
	public void removeData(String key) {
		this.dataMap.remove(key);
	}
	

	public Object getOuterRef() {
		return outerRef;
	}
	
	public void setOuterRef(Object outerRef) {
		this.outerRef = outerRef;
	}
	
	
	
	public boolean isEnabled() {
		return enabled;
	}
	
	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}
	
	public boolean isDiscarded() {
		return discarded;
	}
	
	public void setDiscarded(boolean discarded) {
		this.discarded = discarded;
	}
	
	
	
	
//	public RoutingTableEntry clone() {
//		return initializeRoutingTableEntry(this.node, this.distance, this.created);
//	}
	
	
	public static RoutingTableEntry initializeRoutingTableEntry(NodePointer newNode, double distance, long created, Object outerRef) {
		
		RoutingTableEntry rte = new RoutingTableEntry();
		rte.setNode(newNode);
		rte.setDistance(distance);
		rte.setNodeIdHash(newNode.getNodeIdHash());
		rte.setCreated(created);
		rte.dataMap = new HashMap<String, Object>(HashMapUtils.getHashMapCapacityForElementsNum(GlobalConstants.INITIAL_RTE_DATA_MAP_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		
		rte.outerRef = outerRef;
		
		rte.enabled = true;
		rte.discarded = false;
		
		return rte;
		
	}
	
	
	

}
