package net.hycube.maintenance;

import net.hycube.core.NodeId;
import net.hycube.transport.NetworkNodePointer;

public class HyCubePingResponseIndicatorInfo {
	
	protected NodeId nodeId;
	protected long nodeIdHash;
	protected NetworkNodePointer networkNodePointer;
	protected double pingResponseIndicator;
	protected long discardTime;
	protected boolean removed;
	
	public HyCubePingResponseIndicatorInfo(NodeId nodeId, long nodeIdHash, NetworkNodePointer networkNodePointer, double pingResponseIndicator, long discardTime) {
		this.nodeId = nodeId;
		this.nodeIdHash = nodeIdHash;
		this.networkNodePointer = networkNodePointer;
		this.pingResponseIndicator = pingResponseIndicator;
		this.discardTime = discardTime;
		this.removed = false;
	}

	public NodeId getNodeId() {
		return nodeId;
	}

	public void setNodeId(NodeId nodeId) {
		this.nodeId = nodeId;
	}

	public long getNodeIdHash() {
		return nodeIdHash;
	}

	public void setNodeIdHash(long nodeIdHash) {
		this.nodeIdHash = nodeIdHash;
	}

	public NetworkNodePointer getNetworkNodePointer() {
		return networkNodePointer;
	}

	public void setNetworkNodePointer(NetworkNodePointer networkNodePointer) {
		this.networkNodePointer = networkNodePointer;
	}

	public double getPingResponseIndicator() {
		return pingResponseIndicator;
	}

	public void setPingResponseIndicator(double pingResponseIndicator) {
		this.pingResponseIndicator = pingResponseIndicator;
	}

	public long getDiscardTime() {
		return discardTime;
	}

	public void setDiscardTime(long discardTime) {
		this.discardTime = discardTime;
	}

	public boolean isRemoved() {
		return removed;
	}

	public void setRemoved(boolean removed) {
		this.removed = removed;
	}

}

