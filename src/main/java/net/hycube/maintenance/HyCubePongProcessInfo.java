package net.hycube.maintenance;

import net.hycube.core.NodeId;
import net.hycube.messaging.messages.Message;


public class HyCubePongProcessInfo {
	
	protected int pingSerialNo;
	protected long discardTimestamp;
	
	protected NodeId nodeId;
	protected long nodeIdHash;
	protected byte[] nodeNetworkAddress;
	
	protected boolean processed;
	
	public HyCubePongProcessInfo(NodeId nodeId, long nodeIdHash, byte[] nodeNetworkAddress) {
		this.nodeId = nodeId;
		this.nodeIdHash = nodeIdHash;
		this.nodeNetworkAddress = nodeNetworkAddress;
		
		this.processed = false;
	}
	
	public int getPingSerialNo() {
		return pingSerialNo;
	}
	
	public void setPingSerialNo(int pingSerialNo) {
		this.pingSerialNo = pingSerialNo;
	}

	public long getDiscardTimestamp() {
		return discardTimestamp;
	}
	
	public void setDiscardTimestamp(long discardTimestamp) {
		this.discardTimestamp = discardTimestamp;
	}

	
	public synchronized boolean process(Message msg) {		
		processed = true;
		return true;
	
	}

	public synchronized void discard() {
		
	}

	public boolean isProcessed() {
		return processed;
	}

	public NodeId getNodeId() {
		return nodeId;
	}

	public long getNodeIdHash() {
		return nodeIdHash;
	}

	public byte[] getNodeNetworkAddress() {
		return nodeNetworkAddress;
	}
	
	
}
