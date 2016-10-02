/**
 * 
 */
package net.hycube.transport;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.InitializationException;
import net.hycube.core.Node;
import net.hycube.core.NodeAccessor;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.environment.NodeProperties;
import net.hycube.logging.LogHelper;
import net.hycube.messaging.fragmentation.MessageFragmentationException;
import net.hycube.messaging.fragmentation.MessageFragmentationRuntimeException;
import net.hycube.messaging.fragmentation.MessageFragmenter;
import net.hycube.messaging.messages.Message;
import net.hycube.utils.ClassInstanceLoadException;
import net.hycube.utils.ClassInstanceLoader;
import net.hycube.utils.ObjectToStringConverter.MappedType;

/**
 * @author Artur Olszak
 *
 */
public class UDPNetworkAdapter implements NetworkAdapter {

	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(UDPNetworkAdapter.class); 
	
	protected static final String PROP_KEY_OS_SEND_BUFFER_SIZE = "OSSendBufferSize";
	protected static final String PROP_KEY_OS_RECEIVE_BUFFER_SIZE = "OSReceiveBufferSize";
	protected static final String PROP_KEY_RECEIVE_TIMEOUT = "ReceiveTimeout";
	
	protected static final String PROP_KEY_MAX_MESSAGE_LENGTH = "MaxMessageLength";
	protected static final String PROP_KEY_THROW_WHEN_MAX_MESSAGE_LENGTH_EXCEEDED = "ThrowWhenMaxMessageLengthExceeded";
	protected static final String PROP_KEY_FRAGMENT_MESSAGES = "FragmentMessages";
	protected static final String PROP_KEY_MESSAGE_FRAGMENTER = "MessageFragmenter";
	

	
	protected boolean initialized = false;
	
	protected int osSendBufferSize;
	protected int osReceiveBufferSize;
	protected int receiveTimeout;
	
	protected String addressString;
	protected byte[] addressBytes;
	protected UDPNodePointer networkNodePointer;
	
	protected String interfaceAddressString;
	protected byte[] interfaceAddressBytes;
	protected UDPNodePointer interfaceNetworkNodePointer;
	
	protected InetSocketAddress socketAddress;
	protected DatagramSocket socket;
	protected Node node;
	protected NodeProperties properties;
	protected NodeAccessor nodeAccessor;
	protected ReceivedMessageProcessProxy receivedMessageProcessProxy;
	
	protected int maxMessageLength;
	protected boolean throwWhenMaxMessageLengthExceeded;
	protected boolean fragmentMessages;
	protected MessageFragmenter messageFragmenter;
	
	
	public boolean isInitialized() {
		return initialized;
	}
	
	
	
	public String getInterfaceAddressString() {
		return interfaceAddressString;
	}
	
	public byte[] getInterfaceAddressBytes() {
		return interfaceAddressBytes;
	}
	
	public NetworkNodePointer getInterfaceNetworkNodePointer() {
		return interfaceNetworkNodePointer;
	}
	
	
	
	public String getPublicAddressString() {
		return addressString;
	}
	
	public byte[] getPublicAddressBytes() {
		return addressBytes;
	}
	
	public NetworkNodePointer getPublicNetworkNodePointer() {
		return networkNodePointer;
	}
	
	
	
	
	public Node getNode() {
		return node;
	}
	
	public DatagramSocket getSocket() {
		return socket;
	}
	
	
	
	
	
	@Override
	public void initialize(String networkAddress, ReceivedMessageProcessProxy receivedMessageProcessProxy, NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		InetSocketAddress isa;
		isa = validateNetworkAddress(networkAddress);
		if (isa == null) {
			throw new IllegalArgumentException("An exception was thrown while initializing the network adapter. Incorrect address specified.");
		}
		initialize(isa, receivedMessageProcessProxy, nodeAccessor, properties);
	}
	
	public void initialize(String address, int port, ReceivedMessageProcessProxy receivedMessageProcessProxy, NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		InetSocketAddress isa;
		isa = validateNetworkAddress(address, port);
		if (isa == null) {
			throw new IllegalArgumentException("An exception was thrown while initializing the network adapter. Incorrect address specified.");
		}
		initialize(isa, receivedMessageProcessProxy, nodeAccessor, properties);
	}
	
	
	public void initialize(InetSocketAddress socketAddress, ReceivedMessageProcessProxy receivedMessageProcessProxy, NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		if (devLog.isInfoEnabled()) {
			devLog.info("Initializing network adapter.");
		}
		
		try {
			this.properties = properties;
			this.nodeAccessor = nodeAccessor;
			this.receivedMessageProcessProxy = receivedMessageProcessProxy;
			
			
			StringBuilder sb = new StringBuilder();
			this.interfaceAddressString = sb.append(socketAddress.getAddress().getHostAddress()).append(":").append(socketAddress.getPort()).toString();
			
			this.interfaceNetworkNodePointer = this.createNetworkNodePointer(this.interfaceAddressString);
			
			this.interfaceAddressBytes = this.interfaceNetworkNodePointer.getAddressBytes();
			
			
			this.addressString = this.interfaceAddressString;
			
			this.addressBytes = this.interfaceAddressBytes;
			
			this.networkNodePointer = this.interfaceNetworkNodePointer;


			this.socketAddress = socketAddress;
			
			socket = new DatagramSocket(socketAddress);
			
			
			try {
				
				this.osSendBufferSize = (Integer) properties.getProperty(PROP_KEY_OS_SEND_BUFFER_SIZE, MappedType.INT);
				this.osReceiveBufferSize = (Integer) properties.getProperty(PROP_KEY_OS_RECEIVE_BUFFER_SIZE, MappedType.INT);
				this.receiveTimeout = (Integer) properties.getProperty(PROP_KEY_RECEIVE_TIMEOUT, MappedType.INT);
				
				this.maxMessageLength = (Integer) properties.getProperty(PROP_KEY_MAX_MESSAGE_LENGTH, MappedType.INT);
				
				this.throwWhenMaxMessageLengthExceeded = (Boolean) properties.getProperty(PROP_KEY_THROW_WHEN_MAX_MESSAGE_LENGTH_EXCEEDED, MappedType.BOOLEAN);
				
				this.fragmentMessages = (Boolean) properties.getProperty(PROP_KEY_FRAGMENT_MESSAGES, MappedType.BOOLEAN);
				
				
				
				if (this.fragmentMessages) {
					try {
						String messageFragmenterKey = properties.getProperty(PROP_KEY_MESSAGE_FRAGMENTER);
						if (messageFragmenterKey == null || messageFragmenterKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_MESSAGE_FRAGMENTER), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_MESSAGE_FRAGMENTER));
						NodeProperties messageFragmenterProperties = properties.getNestedProperty(PROP_KEY_MESSAGE_FRAGMENTER, messageFragmenterKey);
						String messageFragmenterClass = messageFragmenterProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);
						
						messageFragmenter = (MessageFragmenter) ClassInstanceLoader.newInstance(messageFragmenterClass, MessageFragmenter.class);
						messageFragmenter.initialize(nodeAccessor, messageFragmenterProperties);
					} catch (ClassInstanceLoadException e) {
						throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create message fragmenter class instance.", e);
					}
				}
				
				
				
				
				
			} catch (NodePropertiesConversionException e) {
				throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, e.getKey(), "An exception was thrown while initializing the network adapter. Invalid parameter value: " + e.getKey());
			}
			
			
			socket.setSendBufferSize(osSendBufferSize);
			socket.setReceiveBufferSize(osReceiveBufferSize);
			socket.setSoTimeout(receiveTimeout);
			
			
			
			
			this.initialized = true;
		}
		catch (SocketException e) {
			throw new InitializationException(InitializationException.Error.NETWORK_ADAPTER_INITIALIZATION_ERROR, null, "An exception was thrown while initializing the network adapter.", e);
		}
		
		if (userLog.isInfoEnabled()) {
			userLog.info("Initialized network adapter.");
		}
		if (devLog.isInfoEnabled()) {
			devLog.info("Initialized network adapter.");
		}
		
	}
	
	
	
	
	public void setPublicAddress(String addressString) {
		InetSocketAddress isa;
		isa = validateNetworkAddress(addressString);
		if (isa == null) {
			throw new IllegalArgumentException("Invalid network address specified.");
		}
		
		
		this.networkNodePointer = this.createNetworkNodePointer(addressString);
		
		this.addressString = addressString;
				
		this.addressBytes = this.networkNodePointer.getAddressBytes();
		
		this.nodeAccessor.getNodePointer().setNetworkNodePointer(this.networkNodePointer);

	}
	
	public void setPublicAddress(byte[] addressBytes) {
		InetSocketAddress isa;
		isa = validateNetworkAddress(addressBytes);
		if (isa == null) {
			throw new IllegalArgumentException("Invalid network address specified.");
		}
		
		
		this.networkNodePointer = this.createNetworkNodePointer(addressBytes);
		
		this.addressString = this.networkNodePointer.getAddressString();
				
		this.addressBytes = addressBytes;
		
		this.nodeAccessor.getNodePointer().setNetworkNodePointer(this.networkNodePointer);
		
	}
	
	public void setPublicAddress(NetworkNodePointer networkNodePointer) {
		InetSocketAddress isa;
		isa = validateNetworkAddress(addressString);
		if (isa == null) {
			throw new IllegalArgumentException("Invalid network address specified.");
		}
		
		if ( ! (networkNodePointer instanceof UDPNodePointer)) {
			throw new IllegalArgumentException("Invalid network address specified. The network node pointer is expected to be an instance of: " + UDPNodePointer.class.getName());
		}
		
		
		this.networkNodePointer = (UDPNodePointer) networkNodePointer;
		
		this.addressString = this.networkNodePointer.getAddressString();
				
		this.addressBytes = this.networkNodePointer.getAddressBytes();
		
		this.nodeAccessor.getNodePointer().setNetworkNodePointer(this.networkNodePointer);
		
	}
	
	
	
	
		
	@Override
	public void sendMessage(Message msg, NetworkNodePointer np) throws NetworkAdapterException {
		if (!(np instanceof UDPNodePointer)) throw new IllegalArgumentException("The parameter nodePointer specified should be an instance of UDPNodePointer.");
		
		if (!initialized) throw new NetworkAdapterException("The network adapter is not initialized.");
		
		
		
		if (maxMessageLength > 0 && msg.getByteLength() > maxMessageLength) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("The message #" + msg.getSerialNoAndSenderString() + " to " + np.getAddressString() + " exceeds the maximal allowed length and will be dropepd.");
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("The message #" + msg.getSerialNoAndSenderString() + " to " + np.getAddressString() + " exceeds the maximal allowed length and will be dropepd.");
			}
			
			if (throwWhenMaxMessageLengthExceeded) {
				throw new NetworkAdapterException("The message #" + msg.getSerialNoAndSenderString() + " to " + np.getAddressString() + " exceeds the maximal allowed length and will be dropepd.");
			}
			else {
				return;
			}
			
		}
		
		
		if (fragmentMessages) {
			//send fragmented message:
			Message[] fragments;
			try {
				fragments = fragmentMessage(msg);
			} catch (MessageFragmentationException e) {
				throw new NetworkAdapterException("An exception was thrown while fragmenting the message.", e);
			}
			for (Message fragment : fragments) {
				doSendMessage(fragment, np);
			}
		}
		else {
			doSendMessage(msg, np);
			
		}
		
		
	}

	
	

	protected void doSendMessage(Message msg, NetworkNodePointer np) throws NetworkAdapterException {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Sending message #" + msg.getSerialNoAndSenderString() + " to " + np.getAddressString());
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Sending message #" + msg.getSerialNoAndSenderString() + " to " + np.getAddressString());
		}
		
		UDPNodePointer udpNodePointer = (UDPNodePointer) np;
		
		byte[] byteArray = msg.getBytes();
		InetSocketAddress inetAddr = udpNodePointer.getInetSocketAddress();	//new InetSocketAddress(udpNodePointer.getIP(), udpNodePointer.getPort()); 
		
		DatagramPacket packetToSend = null;
		packetToSend = new DatagramPacket(byteArray, byteArray.length, inetAddr);
		
		try {
			socket.send(packetToSend);
		} catch (IOException e) {
			throw new NetworkAdapterException("An exception thrown while sending the packet.", e);
		}
		
	}
	
	
	

	@Override
	public void messageReceived(Message msg, NetworkNodePointer directSender) {
		
		if (fragmentMessages) {
			Message reassembled = null;
			try {
				reassembled = reassembleMessage(msg);
			}
			catch (MessageFragmentationException e) {
				if (devLog.isDebugEnabled()) {
					devLog.debug("An exception has been thrown while reassembling the received message #" + msg.getSerialNoAndSenderString() + ". The message will be dropped.", e);
				}
				if (msgLog.isInfoEnabled()) {
					msgLog.info("An exception has been thrown while reassembling the received message #" + msg.getSerialNoAndSenderString() + ". The message will be dropped.", e);
				}
				return;
			}
			catch (MessageFragmentationRuntimeException e) {
				if (devLog.isDebugEnabled()) {
					devLog.debug("An exception has been thrown while reassembling the received message #" + msg.getSerialNoAndSenderString() + ". The message will be dropped.", e);
				}
				if (msgLog.isInfoEnabled()) {
					msgLog.info("An exception has been thrown while reassembling the received message #" + msg.getSerialNoAndSenderString() + ". The message will be dropped.", e);
				}
				return;
			}
			
			if (reassembled != null) {
				processReceivedMessage(reassembled, directSender);
			}
		}
		else {
			processReceivedMessage(msg, directSender);
		}
		
	}
	
	
	
	public void processReceivedMessage(Message msg, NetworkNodePointer directSender) {
		
		if (maxMessageLength > 0 && msg.getByteLength() > maxMessageLength) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("The received message #" + msg.getSerialNoAndSenderString() + " exceeds the maximal allowed length and will be dropepd.");
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("The received message #" + msg.getSerialNoAndSenderString() + " exceeds the maximal allowed length and will be dropepd.");
			}

			return;
			
		}
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Passing the received message to the node.");
		}
		receivedMessageProcessProxy.messageReceived(msg, directSender);
	}
	
	
	
	
	protected Message[] fragmentMessage(Message msg) throws MessageFragmentationException {
		if (messageFragmenter != null) {
			return messageFragmenter.fragmentMessage(msg);
		}
		else {
			throw new UnsupportedOperationException("The message fragmenter is not defined.");
		}
	}
	
	protected Message reassembleMessage(Message msg) throws MessageFragmentationException {
		if (messageFragmenter != null) {
			return messageFragmenter.reassemblyMessage(msg);
		}
		else {
			throw new UnsupportedOperationException("The message fragmenter is not defined.");
		}
	}
	
	
	
	
	
	@Override
	public int getMaxMessageLength() {
		return maxMessageLength;
	}
	
	@Override
	public boolean isFragmentMessages() {
		return fragmentMessages;
	}
	
	@Override
	public int getMessageFragmentLength() {
		if (fragmentMessages) return messageFragmenter.getFragmentLength();
		else return 0;
	}
	
	@Override
	public int getMaxMassageFragmentsCount() {
		if (fragmentMessages) return messageFragmenter.getMaxFragmentsCount();
		else return 0;
	}
	
	
	
	
	@Override
	public UDPNodePointer createNetworkNodePointer(String addressString) {
		return new UDPNodePointer(addressString);
	}

	@Override
	public UDPNodePointer createNetworkNodePointer(byte[] addressBytes) {
		return new UDPNodePointer(addressBytes);
	}
	
	@Override
	public InetSocketAddress validateNetworkAddress(String networkAddress) {
		return UDPNodePointer.validateNetworkAddress(networkAddress);
	}
	
	public InetSocketAddress validateNetworkAddress(String address, int port) {
		return UDPNodePointer.validateNetworkAddress(address, port);
	}
	
	@Override
	public InetSocketAddress validateNetworkAddress(byte[] networkAddressBytes) {
		return UDPNodePointer.validateNetworkAddress(networkAddressBytes);
	}
	
	public InetSocketAddress validateNetworkAddress(byte[] address, int port) {
		return UDPNodePointer.validateNetworkAddress(address, port);
	}

	
	@Override
	public int getAddressByteLength() {
		return UDPNodePointer.getAddressByteLength();
	}
	
	
	
	@Override
	public long getProximity(NetworkNodePointer np) {
		if (!(np instanceof UDPNodePointer)) throw new IllegalArgumentException("The parameter specified should be an instance of UDPNodePointer.");
		
		//!!!
		//ping the node...
		
		return 0;
	}

	
	
	
	@Override
	public void discard() throws NetworkAdapterException {
		if (devLog.isInfoEnabled()) {
			devLog.info("Discarding the network adapter.");
		}
		
		this.initialized = false;
		
		properties = null;
		addressString = null;
		socketAddress = null;
		socket.close();
		socket = null;
		node = null;

		if (userLog.isInfoEnabled()) {
			userLog.info("Discarded the network adapter.");
		}
		if (devLog.isInfoEnabled()) {
			devLog.info("Discarded the network adapter.");
		}
		
	}




}
