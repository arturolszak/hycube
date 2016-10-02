package net.hycube.messaging.messages;

import java.math.BigInteger;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.HyCubeNodeId;
import net.hycube.core.HyCubeNodeIdFactory;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeId;
import net.hycube.core.NodeIdFactory;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.environment.NodeProperties;
import net.hycube.utils.ClassInstanceLoadException;
import net.hycube.utils.ClassInstanceLoader;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public class HyCubeMessageFactory implements MessageFactory {

	protected static final String PROP_KEY_NODE_ID_FACTORY = "NodeIdFactory";
	protected static final String PROP_KEY_NETWORK_ADDRESS_BYTE_LENGTH = "NetworkAddressByteLength";
	protected static final String PROP_KEY_HEADER_EXTENSIONS_COUNT = "HeaderExtensionsCount";
	protected static final String PROP_KEY_HEADER_EXTENSION_LENGTHS = "HeaderExtensionLengths";
	
	
	protected HyCubeNodeIdFactory nodeIdFactory;
	
	protected int networkAddressByteLength;
	
	protected int headerExtensionsCount;
	protected int[] headerExtensionLengths;
	
	
	protected HyCubeNodeId zeroNodeId;
	
	
	protected boolean initialized = false;
	
	

	@Override
	public void initialize(NodeProperties properties) throws InitializationException {
		
		try {
			String nodeIdFactoryKey = properties.getProperty(PROP_KEY_NODE_ID_FACTORY);
			if (nodeIdFactoryKey == null || nodeIdFactoryKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_NODE_ID_FACTORY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_NODE_ID_FACTORY));
			NodeProperties nodeIdFactoryProperties = properties.getNestedProperty(PROP_KEY_NODE_ID_FACTORY, nodeIdFactoryKey);
			String nodeIdFactoryClass = nodeIdFactoryProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
			
			try {
				nodeIdFactory = (HyCubeNodeIdFactory) ClassInstanceLoader.newInstance(nodeIdFactoryClass, NodeIdFactory.class);
			}
			catch (ClassCastException e) {
				throw new IllegalArgumentException("node ID factory should be an instance of: " + HyCubeNodeIdFactory.class.getName());
			}
			
			nodeIdFactory.initialize(nodeIdFactoryProperties);
			
			
			
		} catch (ClassInstanceLoadException e) {
			throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create node id factory instance.", e);
		}
		
		
		try {

			networkAddressByteLength = (Integer) properties.getProperty(PROP_KEY_NETWORK_ADDRESS_BYTE_LENGTH, MappedType.INT);
			if (networkAddressByteLength < 0) {
				throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_NETWORK_ADDRESS_BYTE_LENGTH), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_NETWORK_ADDRESS_BYTE_LENGTH));
			}
					
			
			headerExtensionsCount = (Integer) properties.getProperty(PROP_KEY_HEADER_EXTENSIONS_COUNT, MappedType.INT);
			if (headerExtensionsCount < 0) {
				throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_HEADER_EXTENSIONS_COUNT), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_HEADER_EXTENSIONS_COUNT)); 
			}

			
			headerExtensionLengths = new int[headerExtensionsCount];
			Integer[] hel = properties.getListProperty(PROP_KEY_HEADER_EXTENSION_LENGTHS, MappedType.INT).toArray(new Integer[0]);
			if (hel.length != headerExtensionsCount) {
				if (networkAddressByteLength < 0) {
					throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_HEADER_EXTENSION_LENGTHS), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_HEADER_EXTENSION_LENGTHS) + ". Nie number of header extensions specified does not match the number of header extensions lengths.");
				}

			}
			for (int i = 0; i < headerExtensionsCount; i++) {
				headerExtensionLengths[i] = hel[i];
			}
			
			
					
			
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, e.getKey(), "An error occured while reading a node parameter. The property could not be converted: " + e.getKey(), e);
		}
		
		this.zeroNodeId = nodeIdFactory.fromBigInteger(BigInteger.ZERO);
		
		this.initialized = true;
		
	}
	

	
	public HyCubeMessage newMessage(int messageSerialNo, NodeId senderId, NodeId recipientId, byte[] senderNetworkAddress, HyCubeMessageType type, short ttl, short hopCount, boolean secureRouting, boolean skipRandomNextHops, short sourcePort, short destinationPort) {
		return newMessage(messageSerialNo, (HyCubeNodeId)senderId, (HyCubeNodeId)recipientId, senderNetworkAddress, false, false, 0, false, type, ttl, hopCount, secureRouting, skipRandomNextHops, sourcePort, destinationPort, null);
	}
	
	public HyCubeMessage newMessage(int messageSerialNo, NodeId senderId, NodeId recipientId, byte[] senderNetworkAddress, boolean registerRoute, boolean routeBack, int routeId, boolean anonymousRoute, HyCubeMessageType type, short ttl, short hopCount, boolean secureRouting, boolean skipRandomNextHops, short sourcePort, short destinationPort) {
		return newMessage(messageSerialNo, (HyCubeNodeId)senderId, (HyCubeNodeId)recipientId, senderNetworkAddress, registerRoute, routeBack, routeId, anonymousRoute, type, ttl, hopCount, secureRouting, skipRandomNextHops, sourcePort, destinationPort, null);
	}
	
	public HyCubeMessage newMessage(int messageSerialNo, NodeId senderId, NodeId recipientId, byte[] senderNetworkAddress, HyCubeMessageType type, short ttl, short hopCount, boolean secureRouting, boolean skipRandomNextHops, short sourcePort, short destinationPort, byte[] data) {
		return newMessage(messageSerialNo, (HyCubeNodeId)senderId, (HyCubeNodeId)recipientId, senderNetworkAddress, false, false, 0, false, type, ttl, hopCount, secureRouting, skipRandomNextHops, sourcePort, destinationPort, data);
	}
	
	public HyCubeMessage newMessage(int messageSerialNo, NodeId senderId, NodeId recipientId, byte[] senderNetworkAddress, boolean registerRoute, boolean routeBack, int routeId, boolean anonymousRoute, HyCubeMessageType type, short ttl, short hopCount, boolean secureRouting, boolean skipRandomNextHops, short sourcePort, short destinationPort, byte[] data) {
		return newMessage(messageSerialNo, (HyCubeNodeId)senderId, (HyCubeNodeId)recipientId, senderNetworkAddress, registerRoute, routeBack, routeId, anonymousRoute, type, ttl, hopCount, secureRouting, skipRandomNextHops, sourcePort, destinationPort, data);
	}

	public HyCubeMessage newMessage(int messageSerialNo, HyCubeNodeId senderId, HyCubeNodeId recipientId, byte[] senderNetworkAddress, HyCubeMessageType type, short ttl, short hopCount, boolean secureRouting, boolean skipRandomNextHops, short sourcePort, short destinationPort) {
		return newMessage(messageSerialNo, senderId, recipientId, senderNetworkAddress, false, false, 0, false, type, ttl, hopCount, secureRouting, skipRandomNextHops, sourcePort, destinationPort, null);
	}
	
	public HyCubeMessage newMessage(int messageSerialNo, HyCubeNodeId senderId, HyCubeNodeId recipientId, byte[] senderNetworkAddress, boolean registerRoute, boolean routeBack, int routeId, boolean anonymousRoute, HyCubeMessageType type, short ttl, short hopCount, boolean secureRouting, boolean skipRandomNextHops, short sourcePort, short destinationPort) {
		return newMessage(messageSerialNo, senderId, recipientId, senderNetworkAddress, registerRoute, routeBack, routeId, anonymousRoute, type, ttl, hopCount, secureRouting, skipRandomNextHops, sourcePort, destinationPort, null);
	}

	public HyCubeMessage newMessage(int messageSerialNo, HyCubeNodeId senderId, HyCubeNodeId recipientId, byte[] senderNetworkAddress, HyCubeMessageType type, short ttl, short hopCount, boolean secureRouting, boolean skipRandomNextHops, short sourcePort, short destinationPort, byte[] data) {
		return newMessage(messageSerialNo, senderId, recipientId, senderNetworkAddress, false, false, 0, false, type, ttl, hopCount, secureRouting, skipRandomNextHops, sourcePort, destinationPort, data);
	}
	
	public HyCubeMessage newMessage(int messageSerialNo, HyCubeNodeId senderId, HyCubeNodeId recipientId, byte[] senderNetworkAddress, boolean registerRoute, boolean routeBack, int routeId, boolean anonymousRoute, HyCubeMessageType type, short ttl, short hopCount, boolean secureRouting, boolean skipRandomNextHops, short sourcePort, short destinationPort, byte[] data) {
		
		//initial values for new messages should be set outside of this class
		
		//as the steinhaus point value is not allowed to be null, set the default node id value to it - zero node id (it should be set to the appropriate algorithm-specific value outside this initialization)
		HyCubeNodeId initialSteinhausPoint = zeroNodeId;
		
		return new HyCubeMessage(messageSerialNo, nodeIdFactory.getDimensions(), nodeIdFactory.getDigitsCount(), senderId, recipientId, initialSteinhausPoint, senderNetworkAddress, registerRoute, routeBack, routeId, anonymousRoute, type, ttl, hopCount, false, false, secureRouting, skipRandomNextHops, sourcePort, destinationPort, networkAddressByteLength, headerExtensionLengths, data);
		
	}

	
	
	@Override
	public HyCubeMessage newDataMessage(int messageSerialNo, NodeId senderId, NodeId recipientId, byte[] senderNetworkAddress, short ttl, short hopCount, short sourcePort, short destinationPort, byte[] data) {
		return newMessage(messageSerialNo, (HyCubeNodeId)senderId, (HyCubeNodeId)recipientId, senderNetworkAddress, HyCubeMessageType.DATA, ttl, hopCount, false, false, sourcePort, destinationPort, data);
	}

	
	
	
	@Override
	public HyCubeMessage fromBytes(byte[] byteArray) throws MessageByteConversionException {
		return HyCubeMessage.fromBytes(byteArray, nodeIdFactory.getDimensions(), nodeIdFactory.getDigitsCount(), networkAddressByteLength, headerExtensionLengths);
	}

	@Override
	public int calculateCRC32FromMessageBytes(byte[] messageBytes) throws MessageErrorException {
		return HyCubeMessage.calculateCRC32FromMessageBytes(messageBytes);
	}

	@Override
	public int calculateCRC32FromMessageBytesInPlace(byte[] messageBytes) throws MessageErrorException {
		return HyCubeMessage.calculateCRC32FromMessageBytesInPlace(messageBytes);
	}

	@Override
	public int getCRC32FromMessageBytes(byte[] messageBytes) throws MessageErrorException {
		return HyCubeMessage.getCRC32FromMessageBytes(messageBytes);
	}

	@Override
	public boolean validateCRC32FromMessageBytes(byte[] messageBytes) throws MessageErrorException {
		return HyCubeMessage.validateCRC32FromMessageBytes(messageBytes);
	}

	@Override
	public boolean validateCRC32FromMessageBytesInPlace(byte[] messageBytes) throws MessageErrorException {
		return HyCubeMessage.validateCRC32FromMessageBytesInPlace(messageBytes);
	}



	@Override
	public int getMaxMessageLength() {
		
		//no limitation from the message factory
		return 0;
		
	}




	@Override
	public int getMessageHeaderLength() {
		
		int headerExtensionsLength = 0;
		for (int i = 0; i < headerExtensionLengths.length; i++) {
			headerExtensionsLength += headerExtensionLengths[i];
		}
		
		int headerLength = 0;

		int nodeIdByteLength = HyCubeNodeId.getByteLength(nodeIdFactory.getDimensions(), nodeIdFactory.getDigitsCount());

		headerLength =
			Short.SIZE / 8								//version
			+ Short.SIZE / 8							//reserved
			+ Short.SIZE / 8							//message type
			+ Short.SIZE / 8							//extended message type
			+ Integer.SIZE / 8							//message length
			+ 4											//CRC32 -> 4 bytes
			+ Integer.SIZE / 8							//message serial number
			+ Short.SIZE / 8							//ttl
			+ Short.SIZE / 8							//hop count
			+ Short.SIZE / 8							//source HyCube port
			+ Short.SIZE / 8							//destination HyCube port
			+ nodeIdByteLength							//senderId
			+ nodeIdByteLength							//recipientId
			+ nodeIdByteLength							//steinhausPointId
			+ networkAddressByteLength					//sender network address
			+ HyCubeMessage.MSG_HEADER_OPTIONS_LENGTH	//options
			+ headerExtensionsLength					//header extensions
			;
		
		return (short)headerLength;
		
		
	}
	
	

	@Override
	public void discard() {	
	}
	
	
	
}
