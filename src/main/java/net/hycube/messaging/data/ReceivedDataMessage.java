package net.hycube.messaging.data;

import java.io.Serializable;
import java.nio.ByteOrder;

import net.hycube.core.NodeId;
import net.hycube.utils.HexFormatter;

public class ReceivedDataMessage implements Serializable {

	private static final long serialVersionUID = 7083839373840506888L;

	protected int serialNo;
	protected NodeId senderId;
	protected NodeId recipientId;
	protected String senderNetworkAddress;
	protected short sourcePort;
	protected short destinationPort;
	protected boolean registeredRoute;
	protected boolean routedBack;
	protected int routeId;
	protected boolean anonymous;
	protected boolean secureRouting;
	protected boolean skipRandomNextHops;
	
	protected byte[] data;
	
	
	public ReceivedDataMessage(int serialNo, NodeId senderId, NodeId recipientId, String senderNetworkAddress, short sourcePort, short destinationPort, byte[] data, boolean registeredRoute, boolean routedBack, int routeId, boolean anonymous, boolean secureRouting, boolean skipRandomNextHops) {
		this.serialNo = serialNo;
		this.senderId = senderId;
		this.recipientId = recipientId;
		this.senderNetworkAddress = senderNetworkAddress;
		this.sourcePort = sourcePort;
		this.destinationPort = destinationPort;
		this.data = data;
		this.registeredRoute = registeredRoute;
		this.routedBack = routedBack;
		this.routeId = routeId;
		this.anonymous = anonymous;
		this.secureRouting = secureRouting;
		this.skipRandomNextHops = skipRandomNextHops;
		
	}


	public int getSerialNo() {
		return serialNo;
	}
	
	public NodeId getSenderId() {
		return senderId;
	}

	public NodeId getRecipientId() {
		return recipientId;
	}

	public String getSenderNetworkAddress() {
		return senderNetworkAddress;
	}

	public short getSourcePort() {
		return sourcePort;
	}

	public short getDestinationPort() {
		return destinationPort;
	}
	
	public boolean isRegisteredRoute() {
		return registeredRoute;
	}
	
	public boolean isRoutedBack() {
		return routedBack;
	}
	
	public int getRouteId() {
		return routeId;
	}
	
	public byte[] getData() {
		return data;
	}

	public boolean isAnonymous() {
		return anonymous;
	}
	
	public boolean isSecureRouting() {
		return secureRouting;
	}
	
	public boolean isSkipRandomNextHops() {
		return skipRandomNextHops;
	}
	
	
	
	public String getSerialNoAndSenderString() {
		StringBuilder sb = new StringBuilder();
		sb.append(serialNo);
		if (senderId != null) {
			sb.append(" (").append(HexFormatter.getHex(senderId.getBytes(ByteOrder.BIG_ENDIAN))).append(")");
		}
		return sb.toString();
	}

	
}
