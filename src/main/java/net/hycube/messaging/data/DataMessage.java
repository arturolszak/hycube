package net.hycube.messaging.data;

import java.io.Serializable;

import net.hycube.core.NodeId;

public class DataMessage implements Serializable {
	
	private static final long serialVersionUID = 4480401556300330790L;
	
	protected NodeId recipientId;
	protected String recipientNetworkAddress;
	protected short sourcePort;
	protected short destinationPort;
	
	protected byte[] data;
	
	
	public DataMessage(NodeId recipientId, String recipientNetworkAddress, short sourcePort, short destinationPort, byte[] data) {
		this.recipientId = recipientId;
		this.recipientNetworkAddress = recipientNetworkAddress;
		this.sourcePort = sourcePort;
		this.destinationPort = destinationPort;
		this.data = data;
	}
	
	
	public NodeId getRecipientId() {
		return recipientId;
	}

	public String getRecipientNetworkAddress() {
		return recipientNetworkAddress;
	}

	public short getSourcePort() {
		return sourcePort;
	}

	public short getDestinationPort() {
		return destinationPort;
	}
	
	public byte[] getData() {
		return data;
	}

	
}
