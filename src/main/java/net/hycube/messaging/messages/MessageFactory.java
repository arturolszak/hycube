package net.hycube.messaging.messages;

import net.hycube.core.InitializationException;
import net.hycube.core.NodeId;
import net.hycube.environment.NodeProperties;

public interface MessageFactory {

	public void initialize(NodeProperties properties) throws InitializationException;
	
	public Message newDataMessage(int messageSerialNo, NodeId senderId, NodeId recipientId, byte[] senderNetworkAddress, short ttl, short hopCount, short sourcePort, short destinationPort, byte[] data);
	public Message fromBytes(byte[] byteArray) throws MessageByteConversionException;
	
	public int calculateCRC32FromMessageBytes(byte[] messageByte) throws MessageErrorException;
	public int calculateCRC32FromMessageBytesInPlace(byte[] messageBytes) throws MessageErrorException;
	public int getCRC32FromMessageBytes(byte[] messageBytes) throws MessageErrorException;
	public boolean validateCRC32FromMessageBytes(byte[] messageBytes) throws MessageErrorException;
	public boolean validateCRC32FromMessageBytesInPlace(byte[] messageBytes) throws MessageErrorException;

	public int getMaxMessageLength();
	
	public int getMessageHeaderLength();

	public void discard();

	
}
