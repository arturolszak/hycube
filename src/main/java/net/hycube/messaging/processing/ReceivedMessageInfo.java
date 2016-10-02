package net.hycube.messaging.processing;

import net.hycube.core.NodeId;

public class ReceivedMessageInfo {
	
	protected long nodeIdHash;
	protected NodeId nodeId;
	protected long msgSerialNo;
	protected int crc;
	protected long receiveTime;
	protected String str;
	
	
	public ReceivedMessageInfo(long nodeIdHash, NodeId nodeId, long msgSerialNo, int crc, long receiveTime) {
		
		this.nodeIdHash = nodeIdHash;
		this.nodeId = nodeId;
		this.msgSerialNo = msgSerialNo;
		this.crc = crc;
		this.receiveTime = receiveTime;
		this.str = null;
		
	}
	
	
	public ReceivedMessageInfo(long nodeIdHash, NodeId nodeId, long msgSerialNo, int crc, long receiveTime, String str) {
		
		this.nodeIdHash = nodeIdHash;
		this.nodeId = nodeId;
		this.msgSerialNo = msgSerialNo;
		this.crc = crc;
		this.receiveTime = receiveTime;
		this.str = str;
		
	}


	public long getNodeIdHash() {
		return nodeIdHash;
	}


	public void setNodeIdHash(long nodeIdHash) {
		this.nodeIdHash = nodeIdHash;
	}


	public NodeId getNodeId() {
		return nodeId;
	}


	public void setNodeId(NodeId nodeId) {
		this.nodeId = nodeId;
	}


	public long getMsgSerialNo() {
		return msgSerialNo;
	}


	public void setMsgSerialNo(long msgSerialNo) {
		this.msgSerialNo = msgSerialNo;
	}


	public int getCrc() {
		return crc;
	}


	public void setCrc(int crc) {
		this.crc = crc;
	}


	public long getReceiveTime() {
		return receiveTime;
	}


	public void setReceiveTime(long receiveTime) {
		this.receiveTime = receiveTime;
	}


	public String getStr() {
		return str;
	}


	public void setStr(String str) {
		this.str = str;
	}
	
	
	
	public boolean compareTo(ReceivedMessageInfo rmi) {
		return compare(this, rmi);
	}
	

	public static boolean compare(ReceivedMessageInfo rmi1, ReceivedMessageInfo rmi2) {
		if (rmi1.nodeIdHash == rmi2.nodeIdHash
				&& rmi1.nodeId.compareTo(rmi2.nodeId)
				&& rmi1.msgSerialNo == rmi2.msgSerialNo
				&& rmi1.crc == rmi2.crc) {
			return true;
		}
		else {
			return false;
		}
	}

	
	
}