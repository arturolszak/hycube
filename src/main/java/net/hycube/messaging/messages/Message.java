package net.hycube.messaging.messages;

import net.hycube.core.NodeId;

public interface Message extends Cloneable {

	public int getSerialNo();
	public void setSerialNo(int serialNo);
	
	public NodeId getSenderId();
	public void setSenderId(NodeId senderId);
	
	public NodeId getRecipientId();
	public void setRecipientId(NodeId recipientId);

	public byte[] getSenderNetworkAddress();
	public void setSenderNetworkAddress(byte[] senderNetworkAddress);

	public short getTtl();
	public void setTtl(short ttl);

	public short getHopCount();
	public void setHopCount(short hopCount);
	
	public short getSourcePort();
	public void setSourcePort(short sourcePort);

	public short getDestinationPort();
	public void setDestinationPort(short destinationPort);
	
	public boolean isSecureRoutingApplied();
	public void setSecureRoutingApplied(boolean secureRouting);
	
	public byte[] getData();
	public void setData(byte[] data);
	
	public int getCRC32();
	public void setCRC32(int crc32);
	
	public void updateCRC32();
	
	public boolean validateCRC32(int crc32);
	
	public String getSerialNoAndSenderString();
		
	public int calculateCRC32();
	
	public byte[] getBytes();
	
	public byte[] getBytes(boolean recalculateCRC);
	
	public int getByteLength();
	
	public boolean checkMessageFieldsSet();
	
	
	public void validate() throws MessageErrorException;
	
	public boolean isSystemMessage();
	
	
	public Message clone() throws CloneNotSupportedException;
	
	
	public int getHeaderLength();
	
	
	
}
