/**
 * 
 */
package net.hycube.core;

import java.io.Serializable;
import java.util.HashMap;

import net.hycube.transport.NetworkAdapter;
import net.hycube.transport.NetworkNodePointer;

/**
 * @author Artur Olszak
 *
 */
public class NodePointer implements Serializable {
	
	private static final long serialVersionUID = 4956213976340088921L;
	
	
	protected NodeId nodeId;
	protected NetworkNodePointer networkNodePointer;
	protected long nodeIdHash;
	
	protected HashMap<String, Object> nodePointerDetails;
	
	
	public NodePointer() {
		
	}
	
	public NodePointer(NetworkAdapter networkAdapter, String networkAddress, NodeId nodeId) {
		this.nodeId = nodeId;
		this.nodeIdHash = nodeId.calculateHash();
		this.networkNodePointer = networkAdapter.createNetworkNodePointer(networkAddress);
	}
	
	public NodePointer(NetworkAdapter networkAdapter, byte[] networkAddress, NodeId nodeId) {
		this.nodeId = nodeId;
		this.nodeIdHash = nodeId.calculateHash();
		this.networkNodePointer = networkAdapter.createNetworkNodePointer(networkAddress);
	}
	
	public NodeId getNodeId() {
		return nodeId;
	}
	
	public void setNodeId(NodeId nodeId) {
		this.nodeId = nodeId;
		this.nodeIdHash = nodeId.calculateHash();
	}
	
	public long getNodeIdHash() {
		return nodeIdHash;
	}
	
	public NetworkNodePointer getNetworkNodePointer() {
		return networkNodePointer;
	}
	
	public void setNetworkNodePointer(NetworkNodePointer networkNodePointer) {
		this.networkNodePointer = networkNodePointer;
	}
	
	
	public Object getNodePointerDetail(String key) {
		if (this.nodePointerDetails == null) return null;
		return this.nodePointerDetails.get(key);
	}
	
	public boolean containsNodePointerDetail(String key) {
		if (this.nodePointerDetails == null) return false;
		return this.nodePointerDetails.containsKey(key);
	}
	
	public Object setNodePointerDetail(String key, Object value) {
		if (this.nodePointerDetails == null) this.nodePointerDetails = new HashMap<String, Object>();
		return this.nodePointerDetails.put(key, value);
	}
	
	public Object removeNodePointerDetail(String key) {
		if (this.nodePointerDetails == null) return null;
		return this.nodePointerDetails.remove(key);
	}
	
	
	
}
