/**
 * 
 */
package net.hycube.messaging.messages;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.HyCubeNodeId;
import net.hycube.core.NodeId;
import net.hycube.core.NodeIdByteConversionException;
import net.hycube.utils.CRC32Helper;
import net.hycube.utils.HexFormatter;

/**
 * @author Artur Olszak
 *
 */
public class HyCubeMessage implements Message {

	public static final short MSG_CODE_DEFAULT = 				0;
	public static final short MSG_CODE_EXT = 					-1;		//(short) 0xFFFF all 1s
	public static final short MSG_CODE_DATA = 					1;
	public static final short MSG_CODE_DATA_ACK = 				2;
	public static final short MSG_CODE_LOOKUP =					3;
	public static final short MSG_CODE_LOOKUP_REPLY = 			4;
	public static final short MSG_CODE_SEARCH = 				5;
	public static final short MSG_CODE_SEARCH_REPLY = 			6;
	public static final short MSG_CODE_JOIN = 					7;
	public static final short MSG_CODE_JOIN_REPLY = 			8;
	public static final short MSG_CODE_LEAVE = 					9;
	public static final short MSG_CODE_RECOVERY = 				10;
	public static final short MSG_CODE_RECOVERY_REPLY = 		11;
	public static final short MSG_CODE_NOTIFY = 				12;
	public static final short MSG_CODE_PING = 					13;
	public static final short MSG_CODE_PONG = 					14;
	public static final short MSG_CODE_PUT = 					15;
	public static final short MSG_CODE_PUT_REPLY = 				16;
	public static final short MSG_CODE_GET = 					17;
	public static final short MSG_CODE_GET_REPLY = 				18;
	public static final short MSG_CODE_DELETE = 				19;
	public static final short MSG_CODE_DELETE_REPLY = 			20;
	public static final short MSG_CODE_REFRESH_PUT = 			21;
	public static final short MSG_CODE_REFRESH_PUT_REPLY =		22;
	public static final short MSG_CODE_REPLICATE =				23;
	
	
	
	//Message byte order
	public static final ByteOrder MESSAGE_BYTE_ORDER = ByteOrder.BIG_ENDIAN;
	
	//Charset for encoding strings in messages
	public static final String MSG_STRING_CHARSET = "UTF-8";
	
	//Charset for encoding queue names in messages
	public static final String MSG_QUEUE_NAME_CHARSET = "UTF-8";
		
	//Charset for encoding network addresses in messages - should encode each character in one byte
	public static final String MSG_NETWORK_ADDR_CHARSET = "ISO-8859-1";
	
	//options field size (message header) in bytes
	public static final int MSG_HEADER_OPTIONS_LENGTH = 2;
	
	//number of bit for pmh enabled option
	public static final int MSG_OPTION_NUM_PMH = 0;
	
	//number of bit for Steinhaus transform enabled option
	public static final int MSG_OPTION_NUM_STEINHAUS_TRANSFORM = 1;
	
	//number of bit for secure routing option
	public static final int MSG_OPTION_NUM_SECURE_ROUTING = 2;
	
	//number of bit for skipRandomNumOfNodes option
	public static final int MSG_OPTION_NUM_SKIP_RANDOM_NUM_OF_NODES = 3;
	
	//number of bit for registerRoute option
	public static final int MSG_OPTION_NUM_REGISTER_ROUTE = 4;
	
	//number of bit for routeBack option
	public static final int MSG_OPTION_NUM_ROUTE_BACK = 5;
	
	//number of bit for anonymourRoute option
	public static final int MSG_OPTION_NUM_ANONYMOUS_ROUTE = 6;
	
	

	
	
		
	
	protected int networkAddressByteLength;
	
	protected int headerExtensionsCount;	
	protected int[] headerExtensionLengths;

	
	protected int serialNo;
	protected int nodeIdDimensions;
	protected int nodeIdDigitsCount;
	protected HyCubeNodeId senderId;
	protected HyCubeNodeId recipientId;
	protected HyCubeNodeId steinhausPointId;
	protected byte[] senderNetworkAddress;
	protected boolean registerRoute;
	protected boolean routeBack;
	protected boolean anonymousRoute;
	protected int routeId;
	protected HyCubeMessageType type;
	protected short extType;					//extended message type - may be used by applications and extensions
	protected short ttl;
	protected short hopCount;
	protected short sourcePort;
	protected short destinationPort;
	protected boolean pmhApplied;
	protected boolean steinhausTransformApplied;
	protected boolean secureRoutingApplied;
	protected boolean skipRandomNumOfNodesApplied;
	protected byte[] data;
	protected int crc32;
	
	protected byte[][] headerExtensions;
	
	
	
	
	
	
	
	protected HyCubeMessage() {

	}

	
	public HyCubeMessage(int messageSerialNo, int nodeIdDimensions, int nodeIdDigitsCount, HyCubeNodeId senderId, HyCubeNodeId recipientId, HyCubeNodeId steinhausPointId, byte[] senderNetworkAddress, HyCubeMessageType type, short ttl, short hopCount, boolean pmhEnabled, boolean steinhausTransformEnabled, boolean secureRouting, boolean skipRandomNextHops, short sourcePort, short destinationPort, int networkAddressByteLength, int[] headerExtensionLengths) {
		this(messageSerialNo, nodeIdDimensions, nodeIdDigitsCount, senderId, recipientId, steinhausPointId, senderNetworkAddress, type, ttl, hopCount, pmhEnabled, steinhausTransformEnabled, secureRouting, skipRandomNextHops, sourcePort, destinationPort, networkAddressByteLength, headerExtensionLengths, null);
	}
	
	public HyCubeMessage(int messageSerialNo, int nodeIdDimensions, int nodeIdDigitsCount, HyCubeNodeId senderId, HyCubeNodeId recipientId, HyCubeNodeId steinhausPointId, byte[] senderNetworkAddress, boolean registerRoute, boolean routeBack, int routeId, boolean anonymousRoute, HyCubeMessageType type, short ttl, short hopCount, boolean pmhEnabled, boolean steinhausTransformEnabled, boolean secureRouting, boolean skipRandomNextHops, short sourcePort, short destinationPort, int networkAddressByteLength, int[] headerExtensionLengths) {
		this(messageSerialNo, nodeIdDimensions, nodeIdDigitsCount, senderId, recipientId, steinhausPointId, senderNetworkAddress, registerRoute, routeBack, routeId, anonymousRoute, type, ttl, hopCount, pmhEnabled, steinhausTransformEnabled, secureRouting, skipRandomNextHops, sourcePort, destinationPort, networkAddressByteLength, headerExtensionLengths, null);
	}
	
	
	
	/**
	 * Takes dimensions and digitsCount from the senderId
	 */
	public HyCubeMessage(int messageSerialNo, HyCubeNodeId senderId, HyCubeNodeId recipientId, HyCubeNodeId steinhausPointId, byte[] senderNetworkAddress, HyCubeMessageType type, short ttl, short hopCount, boolean pmhEnabled, boolean steinhausTransformEnabled, boolean secureRouting, boolean skipRandomNextHops, short sourcePort, short destinationPort, int networkAddressByteLength, int[] headerExtensionLengths) {
		this(messageSerialNo, senderId.getDimensions(), senderId.getDigitsCount(), senderId, recipientId, steinhausPointId, senderNetworkAddress, type, ttl, hopCount, pmhEnabled, steinhausTransformEnabled, secureRouting, skipRandomNextHops, sourcePort, destinationPort, networkAddressByteLength, headerExtensionLengths, null);
	}
	
	
	/**
	 * Takes dimensions and digitsCount from the senderId
	 */
	public HyCubeMessage(int messageSerialNo, HyCubeNodeId senderId, HyCubeNodeId recipientId, HyCubeNodeId steinhausPointId, byte[] senderNetworkAddress, boolean registerRoute, boolean routeBack, int routeId, boolean anonymousRoute, HyCubeMessageType type, short ttl, short hopCount, boolean pmhEnabled, boolean steinhausTransformEnabled, boolean secureRouting, boolean skipRandomNextHops, short sourcePort, short destinationPort, int networkAddressByteLength, int[] headerExtensionLengths) {
		this(messageSerialNo, senderId.getDimensions(), senderId.getDigitsCount(), senderId, recipientId, steinhausPointId, senderNetworkAddress, registerRoute, routeBack, routeId, anonymousRoute, type, ttl, hopCount, pmhEnabled, steinhausTransformEnabled, secureRouting, skipRandomNextHops, sourcePort, destinationPort, networkAddressByteLength, headerExtensionLengths, null);
	}
	
	
	/**
	 * Takes dimensions and digitsCount from the senderId
	 */
	public HyCubeMessage(int messageSerialNo, HyCubeNodeId senderId, HyCubeNodeId recipientId, HyCubeNodeId steinhausPointId, byte[] senderNetworkAddress, HyCubeMessageType type, short ttl, short hopCount, boolean pmhEnabled, boolean steinhausTransformEnabled, boolean secureRouting, boolean skipRandomNextHops, short sourcePort, short destinationPort, int networkAddressByteLength, int[] headerExtensionLengths, byte[] data) {
		this(messageSerialNo, senderId.getDimensions(), senderId.getDigitsCount(), senderId, recipientId, steinhausPointId, senderNetworkAddress, type, ttl, hopCount, pmhEnabled, steinhausTransformEnabled, secureRouting, skipRandomNextHops, sourcePort, destinationPort, networkAddressByteLength, headerExtensionLengths, data);
	}
	
	
	
	public HyCubeMessage(int messageSerialNo, int nodeIdDimensions, int nodeIdDigitsCount, HyCubeNodeId senderId, HyCubeNodeId recipientId, HyCubeNodeId steinhausPointId, byte[] senderNetworkAddress, HyCubeMessageType type, short ttl, short hopCount, boolean pmhEnabled, boolean steinhausTransformEnabled, boolean secureRouting, boolean skipRandomNextHops, short sourcePort, short destinationPort, int networkAddressByteLength, int[] headerExtensionLengths, byte[] data) {
		this(messageSerialNo, nodeIdDimensions, nodeIdDigitsCount, senderId, recipientId, steinhausPointId, senderNetworkAddress, false, false, 0, false, type, ttl, hopCount, pmhEnabled, steinhausTransformEnabled, secureRouting, skipRandomNextHops, sourcePort, destinationPort, networkAddressByteLength, headerExtensionLengths, data);
	}
	
	
	public HyCubeMessage(int messageSerialNo, int nodeIdDimensions, int nodeIdDigitsCount, HyCubeNodeId senderId, HyCubeNodeId recipientId, HyCubeNodeId steinhausPointId, byte[] senderNetworkAddress, boolean registerRoute, boolean routeBack, int routeId, boolean anonymousRoute, HyCubeMessageType type, short ttl, short hopCount, boolean pmhEnabled, boolean steinhausTransformEnabled, boolean secureRouting, boolean skipRandomNextHops, short sourcePort, short destinationPort, int networkAddressByteLength, int[] headerExtensionLengths, byte[] data) {
		
		if (nodeIdDimensions <= 0 || nodeIdDimensions > HyCubeNodeId.MAX_NODE_ID_DIMENSIONS) {
			throw new IllegalArgumentException("Error while creating a message object. Not allowed dimensions count.");
		}
		if (nodeIdDigitsCount <= 0 || nodeIdDigitsCount > HyCubeNodeId.MAX_NODE_ID_LEVELS) {
			throw new IllegalArgumentException("Error while creating a message object. Not allowed levels count.");
		}


		
		this.serialNo = messageSerialNo;
		
		this.nodeIdDimensions = nodeIdDimensions;
		this.nodeIdDigitsCount = nodeIdDigitsCount;
		
		this.networkAddressByteLength = networkAddressByteLength;
		
		this.senderId = senderId;
		this.recipientId = recipientId;
		this.steinhausPointId = steinhausPointId;
		this.senderNetworkAddress = senderNetworkAddress;
		this.registerRoute = registerRoute;
		this.routeBack = routeBack;
		this.routeId = routeId;
		this.anonymousRoute = anonymousRoute;
		this.type = type;
		this.extType = 0;
		this.ttl = ttl;
	    this.hopCount = hopCount;
	    this.pmhApplied = pmhEnabled;
	    this.steinhausTransformApplied = steinhausTransformEnabled;
	    this.secureRoutingApplied = secureRouting;
	    this.skipRandomNumOfNodesApplied = skipRandomNextHops;
	    this.sourcePort = sourcePort;
	    this.destinationPort = destinationPort;

	    if (headerExtensionLengths != null) {
			this.headerExtensionsCount = headerExtensionLengths.length;
			this.headerExtensionLengths = headerExtensionLengths;
			this.headerExtensions = new byte[headerExtensionsCount][];
		}
		else {
			this.headerExtensionsCount = 0;
			this.headerExtensionLengths = new int[0];
		    this.headerExtensions = new byte[0][];			
		}


	    
	    
		this.data = data;

		//updateCRC32();

	}
	
	


	
	
	@Override
	public int getHeaderLength() {
		
		int headerExtensionsLength = 0;
		for (int i = 0; i < headerExtensionLengths.length; i++) {
			headerExtensionsLength += headerExtensionLengths[i];
		}
		
		int headerLength = 0;

		int nodeIdByteLength = HyCubeNodeId.getByteLength(nodeIdDimensions, nodeIdDigitsCount);
		
		headerLength =
			Short.SIZE / 8							//version
			+ Short.SIZE / 8						//reserved
			+ Short.SIZE / 8						//message type
			+ Short.SIZE / 8						//extended message type
			+ Integer.SIZE / 8						//message length
			+ 4										//CRC32 -> 4 bytes
			+ Integer.SIZE / 8						//message serial number
			+ Short.SIZE / 8						//ttl
			+ Short.SIZE / 8						//hop count
			+ Short.SIZE / 8						//source HyCube port
			+ Short.SIZE / 8						//destination HyCube port
			+ nodeIdByteLength						//senderId
			+ nodeIdByteLength						//recipientId
			+ nodeIdByteLength						//steinhausPointId
			+ networkAddressByteLength				//sender network address
			+ Integer.SIZE / 8						//route id
			+ MSG_HEADER_OPTIONS_LENGTH				//options
			+ headerExtensionsLength				//header extensions
			;
		
		
		return (short)headerLength;
		
		
	}
	
	
	protected static final int crcPosition =
			Short.SIZE / 8								//version
			+ Short.SIZE / 8							//reserved
			+ Short.SIZE / 8							//message type
			+ Short.SIZE / 8							//extended message type
			+ Integer.SIZE / 8							//message length
			;

	protected static int getCRCPosition() {
		return crcPosition;
	}

	
	
	
	
	public int getSerialNo() {
		return serialNo;
	}

	public void setSerialNo(int serialNo) {
		this.serialNo = serialNo;
	}
	
	public HyCubeNodeId getSenderId() {
		return senderId;
	}

	public void setSenderId(HyCubeNodeId senderId) {
		this.senderId = senderId;
	}
	

	public void setSenderId(NodeId senderId) {
		if (senderId instanceof HyCubeNodeId) {
			setSenderId((HyCubeNodeId)senderId);
		}
		else {
			throw new IllegalArgumentException("senderId should be an instance of HyCubeNodeId,");
		}
	}

	public HyCubeNodeId getRecipientId() {
		return recipientId;
	}

	public void setRecipientId(HyCubeNodeId recipientId) {
		this.recipientId = recipientId;
	}

	@Override
	public void setRecipientId(NodeId recipientId) {
		if (recipientId instanceof HyCubeNodeId) {
			setRecipientId((HyCubeNodeId)recipientId);
		}
		else {
			throw new IllegalArgumentException("recipientId should be an instance of HyCubeNodeId,");
		}
	}

	
	public HyCubeNodeId getSteinhausPointId() {
		return steinhausPointId;
	}

	public void setSteinhausPoint(HyCubeNodeId steinhausPointId) {
		this.steinhausPointId = steinhausPointId;
	}
	
	public byte[] getSenderNetworkAddress() {
		return senderNetworkAddress;
	}

	public void setSenderNetworkAddress(byte[] senderNetworkAddress) {
		this.senderNetworkAddress = senderNetworkAddress;
	}
	
	public boolean isRegisterRoute() {
		return this.registerRoute;
	}
	
	public void setRegisterRoute(boolean registerRoute) {
		this.registerRoute = registerRoute;
	}
	
	public boolean isRouteBack() {
		return this.routeBack;
	}
	
	public void setRouteBack(boolean routeBack) {
		this.routeBack = routeBack;
	}
	
	public int getRouteId() {
		return routeId;
	}
	
	public void setRouteId(int routeId) {
		this.routeId = routeId;
	}
	
	public boolean isAnonymousRoute() {
		return this.anonymousRoute;
	}
	
	public void setAnonymousRoute(boolean anonymousRoute) {
		this.anonymousRoute = anonymousRoute;
	}

	public HyCubeMessageType getType() {
		return type;
	}

	public void setType(HyCubeMessageType type) {
		this.type = type;
	}

	//extended message type - may be used by applications and extensions
	public short getExtType() {
		return extType;
	}

	//extended message type - may be used by applications and extensions
	public void setExtType(short extType) {
		this.extType = extType;
	}
	
	public short getTtl() {
		return ttl;
	}

	public void setTtl(short ttl) {
		this.ttl = ttl;
	}

	public short getHopCount() {
		return hopCount;
	}

	public void setHopCount(short hopCount) {
		this.hopCount = hopCount;
	}

	public short getSourcePort() {
		return sourcePort;
	}

	public void setSourcePort(short sourcePort) {
		this.sourcePort = sourcePort;
	}


	public short getDestinationPort() {
		return destinationPort;
	}


	public void setDestinationPort(short destinationPort) {
		this.destinationPort = destinationPort;
	}
	

	
	public String getSerialNoAndSenderString() {
		StringBuilder sb = new StringBuilder();
		sb.append(serialNo);
		if (senderId != null) {
			sb.append(" (").append(HexFormatter.getHex(senderId.getBytes(ByteOrder.BIG_ENDIAN))).append(")");
		}
		return sb.toString();
	}
	
	public boolean isPMHApplied() {
		return pmhApplied;
	}
	
	public void setPMHApplied(boolean pmhApplied) {
		this.pmhApplied = pmhApplied;
	}
	
	
	public boolean isSteinhausTransformApplied() {
		return steinhausTransformApplied;
	}
	
	public void setSteinhausTransformApplied(boolean steinhausTransformApplied) {
		this.steinhausTransformApplied = steinhausTransformApplied;
	}
	
	public boolean isSecureRoutingApplied() {
		return secureRoutingApplied;
	}
	
	public void setSecureRoutingApplied(boolean secureRoutingApplied) {
		this.secureRoutingApplied = secureRoutingApplied;
	}
	
	public boolean isSkipRandomNumOfNodesApplied() {
		return skipRandomNumOfNodesApplied;
	}
	
	public void setSkipRandomNumOfNodesApplied(boolean skipRandomNumOfNodesApplied) {
		this.skipRandomNumOfNodesApplied = skipRandomNumOfNodesApplied;
	}
	
	
	public int getHeaderExtensionsCount() {
		return headerExtensionsCount;
	}
	

	public byte[] getHeaderExtension(int index) {
		return headerExtensions[index];
	}
	
	public void setHeaderExtension(int index, byte[] data) {
		if (data != null && data.length != headerExtensionLengths[index]) {
			throw new IllegalArgumentException("Incorrect header extension data length.");
		}
		this.headerExtensions[index] = data;
	}
	
	
	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}
	
	public int getCRC32() {
		return crc32;
	}

	public void setCRC32(int crc32) {
		this.crc32 = crc32;
	}


	
	public int getNodeIdDimensions() {
		return nodeIdDimensions;
	}
	
	public int getNodeIdDigitsCount() {
		return nodeIdDigitsCount;
	}
	
	
	
	
	public int calculateCRC32() {
		
		if (!checkMessageFieldsSet()) return 0;
		
		int length = getHeaderLength();
		if (data != null) length += data.length;
		
		ByteBuffer b = ByteBuffer.allocate(length);
		b.order(MESSAGE_BYTE_ORDER);
		
		//header:
		b.putShort(GlobalConstants.PROTOCOL_VERSION);								//version
		b.putShort((short) 0);														//reserved - should be 0
		b.putShort(type.getCode());													//message type
		b.putShort(extType);														//extended message type
		b.putInt(length);															//message length
		b.putInt(0);																//zero crc32
		b.putInt(serialNo);															//message serial number
		b.putShort(ttl);															//ttl
		b.putShort(hopCount);														//hop count
		b.putShort(sourcePort);														//source HyCube port
		b.putShort(destinationPort);												//destination HyCube port
//		b.putShort((short)this.nodeIdDimensions);									//dimensions
//		b.putShort((short)this.nodeIdDigitsCount);									//levels
		b.put(senderId.getBytes(MESSAGE_BYTE_ORDER));				//senderId
		b.put(recipientId.getBytes(MESSAGE_BYTE_ORDER));			//recipientId
		b.put(steinhausPointId.getBytes(MESSAGE_BYTE_ORDER));		//steinhausPointId
		b.put(senderNetworkAddress);								//sender network address
		b.putInt(routeId);											//routeId
		
		//options:
		byte[] options = new byte[MSG_HEADER_OPTIONS_LENGTH];
		
		//set prefix mismatch heuristic enabled option
		int pmhByte = MSG_OPTION_NUM_PMH / 8;
		int pmhBit = MSG_OPTION_NUM_PMH % 8;
		if (pmhApplied) options[pmhByte] = (byte) (options[pmhByte] | (1 << (7-pmhBit)));
		
		//set steinhaus transform enabled option
		int stByte = MSG_OPTION_NUM_STEINHAUS_TRANSFORM / 8;
		int stBit = MSG_OPTION_NUM_STEINHAUS_TRANSFORM % 8;
		if (steinhausTransformApplied) options[stByte] = (byte) (options[stByte] | (1 << (7-stBit)));
		
		//set secure routing option
		int srByte = MSG_OPTION_NUM_SECURE_ROUTING / 8;
		int srBit = MSG_OPTION_NUM_SECURE_ROUTING % 8;
		if (secureRoutingApplied) options[srByte] = (byte) (options[srByte] | (1 << (7-srBit)));
		
		//set secure routing option
		int skipRandomNumOfNodesByte = MSG_OPTION_NUM_SKIP_RANDOM_NUM_OF_NODES / 8;
		int skipRandomNumOfNodesBit = MSG_OPTION_NUM_SKIP_RANDOM_NUM_OF_NODES % 8;
		if (skipRandomNumOfNodesApplied) options[skipRandomNumOfNodesByte] = (byte) (options[skipRandomNumOfNodesByte] | (1 << (7-skipRandomNumOfNodesBit)));
		
		
		//register route:
		int registerRouteByte = MSG_OPTION_NUM_REGISTER_ROUTE / 8;
		int registerRouteBit = MSG_OPTION_NUM_REGISTER_ROUTE % 8;
		if (registerRoute) options[registerRouteByte] = (byte) (options[registerRouteByte] | (1 << (7-registerRouteBit)));
		
		//route back
		int routeBackByte = MSG_OPTION_NUM_ROUTE_BACK / 8;
		int routeBackBit = MSG_OPTION_NUM_ROUTE_BACK % 8;
		if (routeBack) options[routeBackByte] = (byte) (options[routeBackByte] | (1 << (7-routeBackBit)));
		
		//anonymous route:
		int anonymousRouteByte = MSG_OPTION_NUM_ANONYMOUS_ROUTE / 8;
		int anonymousRouteBit = MSG_OPTION_NUM_ANONYMOUS_ROUTE % 8;
		if (anonymousRoute) options[anonymousRouteByte] = (byte) (options[anonymousRouteByte] | (1 << (7-anonymousRouteBit)));
		
		
		b.put(options);
		
		
		//header extensions
		for (int i = 0; i < headerExtensionsCount; i++) {
			if (headerExtensions[i] != null) {
				b.put(headerExtensions[i]);
			}
			else {
				b.put(new byte[headerExtensionLengths[i]]);
			}
		}
		
		
		
		//data
		if (data != null) b.put(data);
		
		//calculate CRC32
		byte[] bytes = b.array();
		int crc = CRC32Helper.calculateCRC32(bytes);
		
		return crc;
		
	}
	
	
	//calculates CRC32 of the message, using message byte array, operates on bytes, does not perform object->byte[] nor byte[]->object conversion, but performs the copy of the byte array, so this method should be used (is faster) when the byte representation is available and only crc should be calculated
	public static int calculateCRC32FromMessageBytes(byte[] messageBytes) throws MessageErrorException {
		
		if (messageBytes == null || messageBytes.length == 0) {
			throw new MessageErrorException("Error while calculating the CRC32 for the Message. The input byte array is null or empty.");
		}
		
		//operating on the copy of the messageBytes:
		byte[] byteArray = Arrays.copyOf(messageBytes, messageBytes.length);
		
		
		ByteBuffer b = ByteBuffer.wrap(byteArray);
		b.order(MESSAGE_BYTE_ORDER);
		
		
		//move to the CRC position:
			
		int crcPosition = getCRCPosition();

		if (crcPosition > byteArray.length - Integer.SIZE/8) {
			throw new MessageErrorException("Error while calculating the CRC32 for the Message. The input array is shorter than expected.");
		}
		b.position(crcPosition);

		//put 0 crc
		b.putInt(0);
		
		//calculate the crc
		int crc = CRC32Helper.calculateCRC32(byteArray);

		return crc;
		
	}

	
	//calculates CRC32 of the message, using message byte array, operates on bytes, does not perform object->byte[] nor byte[]->object conversion, performs operations on the input byte array changing its content while calculating (is not thread safe), so this method should be used (is faster) when the byte representation is available and only crc should be calculated
	public static int calculateCRC32FromMessageBytesInPlace(byte[] messageBytes) throws MessageErrorException {
		
		if (messageBytes == null || messageBytes.length == 0) {
			throw new MessageErrorException("Error while calculating the CRC32 for the Message. The input byte array is null or empty.");
		}
		
		byte[] byteArray = messageBytes;
		
		
		ByteBuffer b = ByteBuffer.wrap(byteArray);
		b.order(MESSAGE_BYTE_ORDER);
		
		
		//move to the CRC position:
			
		int crcPosition = getCRCPosition();

		if (crcPosition > byteArray.length - Integer.SIZE/8) {
			throw new MessageErrorException("Error while calculating the CRC32 for the Message. The input array is shorter than expected.");
		}

		//get crc from the msg:
		b.position(crcPosition);
		int msgCrc = b.getInt();
		
		//put 0 crc
		b.position(crcPosition);
		b.putInt(0);
		
		//calculate the crc
		int crc = CRC32Helper.calculateCRC32(byteArray);

		//put the old value of the crc to the message:
		b.position(crcPosition);
		b.putInt(msgCrc);
		
		return crc;
		
	}

	
	//gets the CRC32 of the message (byte array representation), operates on bytes, does not perform byte[]->object conversion, so this method should be used when only the byte representation is available 
	public static int getCRC32FromMessageBytes(byte[] messageBytes) throws MessageErrorException {
		if (messageBytes == null || messageBytes.length == 0) {
			throw new MessageErrorException("Error while calculating the CRC32 for the Message. The input byte array is null or empty.");
		}
				
		ByteBuffer b = ByteBuffer.wrap(messageBytes);
		b.order(MESSAGE_BYTE_ORDER);
		
		
		//move to the CRC position:
			
		int crcPosition = getCRCPosition();

		if (crcPosition > messageBytes.length - Integer.SIZE/8) {
			throw new MessageErrorException("Error while calculating the CRC32 for the Message. The input array is shorter than expected.");
		}
		b.position(crcPosition);

		int crcFromMessage = b.getInt();
		return crcFromMessage;

	}

	//validates the CRC32 of the message (byte array representation), operates on bytes, does not perform byte[]->object conversion, but performs the copy of the byte array, so this method should be used (is faster) when the byte representation is available amd only crc should be validated
	public static boolean validateCRC32FromMessageBytes(byte[] messageBytes) throws MessageErrorException {
		if (messageBytes == null || messageBytes.length == 0) {
			throw new MessageErrorException("Error while calculating the CRC32 for the message. The input byte array is null or empty.");
		}
		
		//operating on the copy of the messageBytes:
		byte[] byteArray = Arrays.copyOf(messageBytes, messageBytes.length);
		
		
		ByteBuffer b = ByteBuffer.wrap(byteArray);
		b.order(MESSAGE_BYTE_ORDER);
		
		
		//move to the CRC position:
			
		int crcPosition = getCRCPosition();

		if (crcPosition > byteArray.length - Integer.SIZE/8) {
			throw new MessageErrorException("Error while calculating the CRC32 for the message. The input array is shorter than expected.");
		}
		b.position(crcPosition);

		int crcFromMessage = b.getInt();
		
		b.position(crcPosition);
		
		//put 0 crc
		b.putInt(0);
		
		//calculate the crc
		int crc = CRC32Helper.calculateCRC32(byteArray);

		if (crc == crcFromMessage) return true;
		else return false;
	}


	//validates the CRC32 of the message (byte array representation), operates on bytes, does not perform byte[]->object conversion, operates on the input byte array changing its content during calculation (not thread safe), so this method should be used (is faster) when the byte representation is available amd only crc should be validated
	public static boolean validateCRC32FromMessageBytesInPlace(byte[] messageBytes) throws MessageErrorException {
		if (messageBytes == null || messageBytes.length == 0) {
			throw new MessageErrorException("Error while calculating the CRC32 for the message. The input byte array is null or empty.");
		}
		
		byte[] byteArray = messageBytes;
		
		
		ByteBuffer b = ByteBuffer.wrap(byteArray);
		b.order(MESSAGE_BYTE_ORDER);
		
		
		//move to the CRC position:
			
		int crcPosition = getCRCPosition();

		if (crcPosition > byteArray.length - Integer.SIZE/8) {
			throw new MessageErrorException("Error while calculating the CRC32 for the message. The input array is shorter than expected.");
		}
		
		//get the crc from the message
		b.position(crcPosition);
		int crcFromMessage = b.getInt();
		
		//put 0 crc
		b.position(crcPosition);
		b.putInt(0);
		
		//calculate the crc
		int crc = CRC32Helper.calculateCRC32(byteArray);

		//put the previous value of the crc to the message:
		b.position(crcPosition);
		b.putInt(crcFromMessage);
		
		if (crc == crcFromMessage) return true;
		else return false;
	}
	
	
	public void updateCRC32() {
		crc32 = calculateCRC32();
	}
	
	
	public boolean validateCRC32(int crc32) {
		int calculatedCRC32 = calculateCRC32();
		if (calculatedCRC32 == crc32) {
			return true;
		}
		else {
			return false;
		}
	}
	
	
	
	public int getByteLength() {
		int headerLength = getHeaderLength();
		int dataLength;
		if (data != null) dataLength = data.length;
		else dataLength = 0;
		
		return headerLength + dataLength;
				
	}
	
	
	//converts the message to byte array as it is (without recalculating the checksum)
	public byte[] getBytes() {
		return getBytes(false);
	}
	
	//converts the message to byte array (the parameter specifies if the checksum should be recalculated or taken from the message object as it is)
	public byte[] getBytes(boolean recalculateCRC) {

		if (!checkMessageFieldsSet()) {
			throw new MessageByteConversionRuntimeException("The message cannot be converted to a byte array. All message fields should be set.");
		}
		
		if (recipientId.getDimensions() != nodeIdDimensions || recipientId.getDigitsCount() != nodeIdDigitsCount || steinhausPointId.getDimensions() != nodeIdDimensions || steinhausPointId.getDigitsCount() != nodeIdDigitsCount) {
			throw new MessageByteConversionRuntimeException("The message cannot be converted to a byte array. SenderId, RecipientId, SteinhausPointId should have the same number of dimensions and digit count.");
		}
		if (senderNetworkAddress.length != networkAddressByteLength) {
			throw new MessageByteConversionRuntimeException("Invalid network address byte length.");
		}
		
		int length = getHeaderLength();
		if (data != null) length += data.length;
		
		ByteBuffer b = ByteBuffer.allocate(length);
		b.order(MESSAGE_BYTE_ORDER);
		
		//header:
		b.putShort(GlobalConstants.PROTOCOL_VERSION);								//version
		b.putShort((short) 0);														//reserved - should be 0
		b.putShort(type.getCode());													//message type
		b.putShort(extType);														//extended message type
		b.putInt(length);															//message length
		b.putInt(0);																//crc will be updated later
		b.putInt(serialNo);															//message serial number
		b.putShort(ttl);															//ttl
		b.putShort(hopCount);														//hop count
		b.putShort(sourcePort);														//source port
		b.putShort(destinationPort);												//destination port
//		b.putShort((short)nodeIdDimensions);										//dimensions
//		b.putShort((short)nodeIdDigitsCount);										//levels
		b.put(senderId.getBytes(MESSAGE_BYTE_ORDER));				//senderId
		b.put(recipientId.getBytes(MESSAGE_BYTE_ORDER));			//recipientId
		b.put(steinhausPointId.getBytes(MESSAGE_BYTE_ORDER));		//steinhausPointId
		b.put(senderNetworkAddress);								//sender network address
		b.putInt(routeId);											//routeId

		//options:
		byte[] options = new byte[MSG_HEADER_OPTIONS_LENGTH];
				
		//set prefix mismatch heuristic enabled option
		int pmhByte = MSG_OPTION_NUM_PMH / 8;
		int pmhBit = MSG_OPTION_NUM_PMH % 8;
		if (pmhApplied) options[pmhByte] = (byte) (options[pmhByte] | (1 << (7-pmhBit)));
		
		//set steinhaus transform enabled option
		int stByte = MSG_OPTION_NUM_STEINHAUS_TRANSFORM / 8;
		int stBit = MSG_OPTION_NUM_STEINHAUS_TRANSFORM % 8;
		if (steinhausTransformApplied) options[stByte] = (byte) (options[stByte] | (1 << (7-stBit)));
		
		//set secure routing option
		int srByte = MSG_OPTION_NUM_SECURE_ROUTING / 8;
		int srBit = MSG_OPTION_NUM_SECURE_ROUTING % 8;
		if (secureRoutingApplied) options[srByte] = (byte) (options[srByte] | (1 << (7-srBit)));
		
		//set secure routing option
		int skipRandomNumOfNodesByte = MSG_OPTION_NUM_SKIP_RANDOM_NUM_OF_NODES / 8;
		int skipRandomNumOfNodesBit = MSG_OPTION_NUM_SKIP_RANDOM_NUM_OF_NODES % 8;
		if (skipRandomNumOfNodesApplied) options[skipRandomNumOfNodesByte] = (byte) (options[skipRandomNumOfNodesByte] | (1 << (7-skipRandomNumOfNodesBit)));
		
		
		//register route:
		int registerRouteByte = MSG_OPTION_NUM_REGISTER_ROUTE / 8;
		int registerRouteBit = MSG_OPTION_NUM_REGISTER_ROUTE % 8;
		if (registerRoute) options[registerRouteByte] = (byte) (options[registerRouteByte] | (1 << (7-registerRouteBit)));
		
		//route back
		int routeBackByte = MSG_OPTION_NUM_ROUTE_BACK / 8;
		int routeBackBit = MSG_OPTION_NUM_ROUTE_BACK % 8;
		if (routeBack) options[routeBackByte] = (byte) (options[routeBackByte] | (1 << (7-routeBackBit)));

		//register route:
		int anonymousRouteByte = MSG_OPTION_NUM_ANONYMOUS_ROUTE / 8;
		int anonymousRouteBit = MSG_OPTION_NUM_ANONYMOUS_ROUTE % 8;
		if (anonymousRoute) options[anonymousRouteByte] = (byte) (options[anonymousRouteByte] | (1 << (7-anonymousRouteBit)));

		
		b.put(options);
		
		

		
		//header extensions
		for (int i = 0; i < headerExtensionsCount; i++) {
			if (headerExtensions[i] != null) {
				b.put(headerExtensions[i]);
			}
			else {
				b.put(new byte[headerExtensionLengths[i]]);
			}
		}

		
		
		
		//data
		if (data != null) b.put(data);
		
		
		
		//crc:
		if (recalculateCRC) {
			
			//calculate CRC32
			
			byte[] bytes = b.array();
			int crc = CRC32Helper.calculateCRC32(bytes);
			crc32 = crc;
			
		}

		b.position(getCRCPosition());
		b.putInt(crc32);
				
				
		//return the message byte array
		return b.array();
		
	}

	
	
	public static HyCubeMessage fromBytes(byte[] byteArray, int dimensions, int digitsCount, int networkAddressByteLength, int[] headerExtensionLengths) throws MessageByteConversionException {
		
		if (dimensions <= 0 || dimensions > HyCubeNodeId.MAX_NODE_ID_DIMENSIONS) {
			throw new IllegalArgumentException("Error while converting a byte array to message object. Not allowed dimensions count.");
		}
		if (digitsCount <= 0 || digitsCount > HyCubeNodeId.MAX_NODE_ID_LEVELS) {
			throw new IllegalArgumentException("Error while converting a byte array to message object. Not allowed levels count.");
		}
		
		if (byteArray == null || byteArray.length == 0) {
			throw new MessageByteConversionException("Error while converting a byte array to message object. Input byte array is null or empty.");
		}
		
		HyCubeMessage msg = new HyCubeMessage();

		msg.nodeIdDimensions = dimensions;
		msg.nodeIdDigitsCount = digitsCount;
		
		msg.networkAddressByteLength = networkAddressByteLength;

		if (headerExtensionLengths != null) {
			msg.headerExtensionsCount = headerExtensionLengths.length;
			msg.headerExtensionLengths = headerExtensionLengths;
		}
		else {
			msg.headerExtensionsCount = 0;
			msg.headerExtensionLengths = new int[0];
		}


		
		ByteBuffer b = ByteBuffer.wrap(byteArray);
		b.order(MESSAGE_BYTE_ORDER);
		
		
		try {
			//header:
			short version = b.getShort();
			if (version != GlobalConstants.PROTOCOL_VERSION) {
				throw new MessageByteConversionException("Error while converting a byte array to message object. Incompatible protocol version.");
			}
			
			b.getShort();	//ship two bytes (reserved)
			
			msg.type = HyCubeMessageType.fromCode(b.getShort());
			if (msg.type == null) {
				throw new MessageByteConversionException("Error while converting a byte array to message object. Incorrect message type.");
			}
	
			msg.extType = b.getShort();
			
			
			int length = b.getInt();
			if (length > byteArray.length) {
				throw new MessageByteConversionException("Error while converting a byte array to message object. The message length from the message header is not equal to the byte array length.");
			}
			
			msg.crc32 = b.getInt();
			
			msg.serialNo = b.getInt();
			
			msg.ttl = b.getShort();
			msg.hopCount = b.getShort();
			
			msg.sourcePort = b.getShort();
			msg.destinationPort = b.getShort();
			
//			int dimensions = b.getShort();
//			int digitsCount = b.getShort();
//			if (dimensions <= 0 || dimensions > HyCubeNodeId.MAX_NODE_ID_DIMENSIONS) {
//				throw new IllegalArgumentException("Error while converting a byte array to message object. Not allowed dimensions count.");
//			}
//			if (digitsCount <= 0 || digitsCount > HyCubeNodeId.MAX_NODE_ID_LEVELS) {
//				throw new IllegalArgumentException("Error while converting a byte array to message object. Not allowed levels count.");
//			}
			
			int idByteLength = HyCubeNodeId.getByteLength(dimensions, digitsCount);
			byte[] senderIdB = new byte[idByteLength];
			byte[] recipientIdB = new byte[idByteLength];
			byte[] steinhausPointIdB = new byte[idByteLength];
			b.get(senderIdB);
			b.get(recipientIdB);
			b.get(steinhausPointIdB);
			try {
				msg.senderId = HyCubeNodeId.fromBytes(senderIdB, dimensions, digitsCount, MESSAGE_BYTE_ORDER);
			} catch (NodeIdByteConversionException e) {
				throw new MessageByteConversionException("Error while converting a byte array to message object. Could not convert the byte representation of senderId.", e);
			}
			try {
				msg.recipientId = HyCubeNodeId.fromBytes(recipientIdB, dimensions, digitsCount, MESSAGE_BYTE_ORDER);
			} catch (NodeIdByteConversionException e) {
				throw new MessageByteConversionException("Error while converting a byte array to message object. Could not convert the byte representation of recipientId.", e);
			}
			try {
				msg.steinhausPointId = HyCubeNodeId.fromBytes(steinhausPointIdB, dimensions, digitsCount, MESSAGE_BYTE_ORDER);
			} catch (NodeIdByteConversionException e) {
				throw new MessageByteConversionException("Error while converting a byte array to message object. Could not convert the byte representation of steinhausPointId.", e);
			}
			
			
			
			//sender network address:
			byte[] senderNetworkAddress = new byte[msg.networkAddressByteLength];
			b.get(senderNetworkAddress);
			msg.senderNetworkAddress = senderNetworkAddress;
			
			
			
			
			//route id:
			msg.routeId = b.getInt();
			
			

			
			//options
			byte[] options = new byte[MSG_HEADER_OPTIONS_LENGTH];
			b.get(options);
			
			//set prefix mismatch heuristic enabled option
			int pmhByte = MSG_OPTION_NUM_PMH / 8;
			int pmhBit = MSG_OPTION_NUM_PMH % 8;
			if ((options[pmhByte] & (1 << (7-pmhBit))) != 0) msg.pmhApplied = true;
			else msg.pmhApplied = false;
			
			//set steinhaus transform enabled option
			int stByte = MSG_OPTION_NUM_STEINHAUS_TRANSFORM / 8;
			int stBit = MSG_OPTION_NUM_STEINHAUS_TRANSFORM % 8;
			if ((options[stByte] & (1 << (7-stBit))) != 0) msg.steinhausTransformApplied = true;
			else msg.steinhausTransformApplied = false;

			//set secure routing option
			int srByte = MSG_OPTION_NUM_SECURE_ROUTING / 8;
			int srBit = MSG_OPTION_NUM_SECURE_ROUTING % 8;
			if ((options[srByte] & (1 << (7-srBit))) != 0) msg.secureRoutingApplied = true;
			else msg.secureRoutingApplied = false;
			
			//set skipRandomNumOfNodes option
			int skipRandomNumOfNodesByte = MSG_OPTION_NUM_SKIP_RANDOM_NUM_OF_NODES / 8;
			int skipRandomNumOfNodesBit = MSG_OPTION_NUM_SKIP_RANDOM_NUM_OF_NODES % 8;
			if ((options[skipRandomNumOfNodesByte] & (1 << (7-skipRandomNumOfNodesBit))) != 0) msg.skipRandomNumOfNodesApplied = true;
			else msg.skipRandomNumOfNodesApplied = false;
			
			//set record route option
			int registerRouteByte = MSG_OPTION_NUM_REGISTER_ROUTE / 8;
			int registerRouteBit = MSG_OPTION_NUM_REGISTER_ROUTE % 8;
			if ((options[registerRouteByte] & (1 << (7-registerRouteBit))) != 0) msg.registerRoute = true;
			else msg.registerRoute = false;
			
			//set route back option
			int routeBackByte = MSG_OPTION_NUM_ROUTE_BACK / 8;
			int routeBackBit = MSG_OPTION_NUM_ROUTE_BACK % 8;
			if ((options[routeBackByte] & (1 << (7-routeBackBit))) != 0) msg.routeBack = true;
			else msg.routeBack = false;
			
			//set anonymous route option
			int anonymousRouteByte = MSG_OPTION_NUM_ANONYMOUS_ROUTE / 8;
			int anonymousRouteBit = MSG_OPTION_NUM_ANONYMOUS_ROUTE % 8;
			if ((options[anonymousRouteByte] & (1 << (7-anonymousRouteBit))) != 0) msg.anonymousRoute = true;
			else msg.anonymousRoute = false;
			
			
			
			
			//header extensions
			msg.headerExtensions = new byte[msg.headerExtensionsCount][];
			for (int i = 0; i < msg.headerExtensionsCount; i++) {
				int headerExtensionLength = msg.headerExtensionLengths[i];

				if (headerExtensionLength < 0) {
					throw new MessageByteConversionException("Could not convert the byte array to the message object. The header extensions count is negative.");
				}
				
				byte[] headerExtension = new byte[headerExtensionLength];
				b.get(headerExtension);
				
				msg.headerExtensions[i] = headerExtension;
				
			}

			
			
			//data:
			int headerLength =  msg.getHeaderLength();
			byte[] data = Arrays.copyOfRange(byteArray, headerLength, length);
			msg.data = data;
			
			
		}
		catch (BufferUnderflowException e) {
			throw new MessageByteConversionException("Error while converting a byte array to message object. Message corrupted. Unexpected end of message found.");
		}
		
		return msg;
		
	}
	
	
	public boolean checkMessageFieldsSet() {
		if (senderId == null) return false;
		if (recipientId == null) return false;
		if (steinhausPointId == null) return false;
		if (type == null) return false;
		if (senderNetworkAddress == null) return false;
		
		return true;
	}
	

	




	@Override
	public void validate() throws MessageErrorException {
		
    	if (! checkMessageFieldsSet()) {
    		//message corrupted
    		throw new MessageErrorException("Message corrupted - not all fields were set. Message serial no: " + getSerialNoAndSenderString());
    	}
    	if (! validateCRC32(getCRC32())) {
    		//error - crc validation
    		throw new MessageErrorException("CRC32 validation error. Message #" + getSerialNoAndSenderString());
    	}

    	if (nodeIdDimensions != getSenderId().getDimensions()) {
    		//sender id dimensions count invalid
    		throw new MessageErrorException("Sender id dimensions count invalid. Message #" + getSerialNoAndSenderString());
    	}
    	if (nodeIdDigitsCount != getSenderId().getDigitsCount()) {
    		//sender id levels count invalid
    		throw new MessageErrorException("Sender id levels count invalid. Message #" + getSerialNoAndSenderString());
    	}
    	if (nodeIdDimensions != getRecipientId().getDimensions()) {
    		//recipient id dimensions count invalid
    		throw new MessageErrorException("Recipient id dimensions count invalid. Message #" + getSerialNoAndSenderString());
    	}
    	if (nodeIdDigitsCount != getRecipientId().getDigitsCount()) {
    		//recipient id levels count invalid
    		throw new MessageErrorException("Recipient id levels count invalid. Message #" + getSerialNoAndSenderString());
    	}
    	if (getSteinhausPointId() != null) {
	    	if (nodeIdDimensions != getSteinhausPointId().getDimensions()) {
	    		//steinhaus point id dimensions count invalid
	    		throw new MessageErrorException("Steinhaus point id dimensions count invalid. Message #" + getSerialNoAndSenderString());
	    	}
	    	if (nodeIdDigitsCount != getSteinhausPointId().getDigitsCount()) {
	    		//steinhaus point id levels count invalid
	    		throw new MessageErrorException("Steinthaus point id levels count invalid. Message #" + getSerialNoAndSenderString());
	    	}
    	}

    	
	}


	@Override
	public boolean isSystemMessage() {
		return type.isSystemMessage();
	}
	
	
	@Override
	public HyCubeMessage clone() {

		HyCubeMessage cloned = new HyCubeMessage();
		
		
		cloned.serialNo = this.serialNo;
		
		cloned.nodeIdDimensions = this.nodeIdDimensions;
		cloned.nodeIdDigitsCount = this.nodeIdDigitsCount;
		
		cloned.networkAddressByteLength = this.networkAddressByteLength;
		
		cloned.senderId = this.senderId;
		cloned.recipientId = this.recipientId;
		cloned.steinhausPointId = this.steinhausPointId;
		cloned.senderNetworkAddress = this.senderNetworkAddress;
		
		cloned.registerRoute = this.registerRoute;
		cloned.routeBack = this.routeBack;
		cloned.routeId = this.routeId;
		cloned.anonymousRoute = this.anonymousRoute;
		
		cloned.type = this.type;
		cloned.extType = this.extType;
		cloned.ttl = this.ttl;
		cloned.hopCount = this.hopCount;
		cloned.pmhApplied = this.pmhApplied;
		cloned.steinhausTransformApplied = this.steinhausTransformApplied;
		cloned.secureRoutingApplied = this.secureRoutingApplied;
		cloned.sourcePort = this.sourcePort;
		cloned.destinationPort = this.destinationPort;
	    
		cloned.skipRandomNumOfNodesApplied = this.skipRandomNumOfNodesApplied;

		cloned.headerExtensionLengths = this.headerExtensionLengths;
		cloned.headerExtensionsCount = this.headerExtensionsCount;
		cloned.headerExtensions = new byte[cloned.headerExtensionsCount][];
		for (int i = 0; i < cloned.headerExtensionsCount; i++) {
			cloned.headerExtensions[i] = this.headerExtensions[i];
		}

	    
	    
		cloned.data = this.data;
		
		cloned.crc32 = this.crc32; 
		
		return cloned;
		
		
	}
	
}
