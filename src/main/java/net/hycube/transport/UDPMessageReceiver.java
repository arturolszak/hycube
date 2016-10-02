package net.hycube.transport;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeParameterSet;
import net.hycube.environment.Environment;
import net.hycube.environment.NodeProperties;
import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventCategory;
import net.hycube.logging.LogHelper;
import net.hycube.messaging.messages.Message;
import net.hycube.messaging.messages.MessageByteConversionException;
import net.hycube.messaging.messages.MessageFactory;
import net.hycube.utils.ClassInstanceLoadException;
import net.hycube.utils.ClassInstanceLoader;

public class UDPMessageReceiver implements MessageReceiver {

	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(UDPMessageReceiver.class); 
	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
	
	public static final int RECEIVE_BUFFER_SIZE = 65535;
	
	protected NodeProperties properties;
	protected byte[] buf;
	protected DatagramPacket packet;
	protected HashMap<String, NetworkAdapter> networkAdapters;
	protected List<DatagramSocket> sockets;
	protected List<String> addresses;
	protected BlockingQueue<Event> receiveEventQueue;
	protected int currentSocketIndex = 0;
	protected boolean initialized = false;
	protected Environment environment;
	protected MessageFactory messageFactory;
	
	protected MessageReceiverProcessEventProxy messageReceiverProcessEventProxy;
	
	
	public boolean isInitialized() {
		return initialized;
	}
	
	@Override
	public synchronized void initialize(Environment environment, BlockingQueue<Event> receiveEventQueue, NodeProperties properties) throws InitializationException {
		
		if (devLog.isInfoEnabled()) {
			devLog.info("Initializing message receiver.");
		}
		
		if (receiveEventQueue == null) {
			throw new IllegalArgumentException("receiveEventQueue is null.");
		}
		
		if (environment == null) {
			throw new IllegalArgumentException("environment is null.");
		}
		
		this.properties = properties;
		
		this.networkAdapters = new HashMap<String, NetworkAdapter>();
		this.addresses = new ArrayList<String>();
		this.sockets = new ArrayList<DatagramSocket>();
		
		this.buf = new byte[RECEIVE_BUFFER_SIZE];
		this.packet = new DatagramPacket(buf, RECEIVE_BUFFER_SIZE);
		
		this.receiveEventQueue = receiveEventQueue;
		
		this.environment = environment;
		
		
		//Message factory:
		try {
			String messageFactoryKey = properties.getProperty(NodeParameterSet.PROP_KEY_MESSAGE_FACTORY);
			if (messageFactoryKey == null || messageFactoryKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_MESSAGE_FACTORY), "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_MESSAGE_FACTORY));
			NodeProperties messageFactoryProperties = properties.getNestedProperty(NodeParameterSet.PROP_KEY_MESSAGE_FACTORY, messageFactoryKey);
			String messageFactoryClass = messageFactoryProperties.getProperty(GlobalConstants.PROP_KEY_CLASS);

			messageFactory = (MessageFactory) ClassInstanceLoader.newInstance(messageFactoryClass, MessageFactory.class);
			messageFactory.initialize(messageFactoryProperties);
		} catch (ClassInstanceLoadException e) {
			throw new InitializationException(InitializationException.Error.CLASS_INSTANTIATION_ERROR, e.getLoadedClassName(), "Unable to create message factory instance.", e);
		}
		
		
		this.messageReceiverProcessEventProxy = new MessageReceiverProcessEventProxy(this);
		
		
		this.initialized = true;
		
		if (userLog.isInfoEnabled()) {
			userLog.info("Initialized message receiver.");
		}
		if (devLog.isInfoEnabled()) {
			devLog.info("Initialized message receiver.");
		}
		
	}
	
	
	@Override
	public synchronized void registerNetworkAdapter(NetworkAdapter networkAdapter) throws MessageReceiverException {
		if (networkAdapter instanceof UDPNetworkAdapter) {
			registerNetworkAdapter((UDPNetworkAdapter)networkAdapter);
		}
		else {
			throw new IllegalArgumentException("The network adapter should be an instance of UDPNetworkAdapter class.");
		}
	}
	
	public synchronized void registerNetworkAdapter(UDPNetworkAdapter networkAdapter) {
		
		if (devLog.isInfoEnabled()) {
			devLog.info("Registering new network adapter.");
		}
		
		if (!initialized) throw new MessageReceiverRuntimeException("The message receiver is not initialized.");
			
		if (networkAdapters.containsKey(networkAdapter.getPublicAddressString())) {
			throw new MessageReceiverRuntimeException("The message receiver already registered a network adapter with the same network address.");
		}
	
		this.networkAdapters.put(networkAdapter.getPublicAddressString(), networkAdapter);
		this.sockets.add(networkAdapter.getSocket());
		this.addresses.add(networkAdapter.getPublicAddressString());

		
		if (userLog.isInfoEnabled()) {
			userLog.info("Registered new network adapter. Network address: " + networkAdapter.getPublicAddressString());
		}
		if (devLog.isInfoEnabled()) {
			devLog.info("Registered new network adapter. Network address: " + networkAdapter.getPublicAddressString());
		}
		
	}

	
	@Override
	public synchronized void unregisterNetworkAdapter(NetworkAdapter networkAdapter) {
		if (networkAdapter instanceof UDPNetworkAdapter) {
			unregisterNetworkAdapter((UDPNetworkAdapter)networkAdapter);
		}
		else {
			throw new IllegalArgumentException("The network adapter should be an instance of UDPNetworkAdapter class.");
		}
	}
	
	public synchronized void unregisterNetworkAdapter(UDPNetworkAdapter networkAdapter) {
		if (devLog.isInfoEnabled()) {
			devLog.info("Unregistering new network adapter.");
		}
		
		
		if (!initialized) throw new MessageReceiverRuntimeException("The message receiver is not initialized.");
			
		if (! this.networkAdapters.containsKey(networkAdapter.getPublicAddressString())) {
			//do nothing - this network adapter is not registered for this instance of network receiver
			return;
		}
			
		this.networkAdapters.remove(networkAdapter.getPublicAddressString());
		this.sockets.remove(networkAdapter.getSocket());
		this.addresses.remove(networkAdapter.getPublicAddressString());
		
		
		if (userLog.isInfoEnabled()) {
			userLog.info("Unregistered new network adapter. Network address: " + networkAdapter.getPublicAddressString());
		}
		if (devLog.isInfoEnabled()) {
			devLog.info("Unregistered new network adapter. Network address: " + networkAdapter.getPublicAddressString());
		}
		
	}
	

	@Override
	//receives one message at a time
	public synchronized void receiveMessage() throws MessageReceiverException {
		
		receiveOneMessage();
		
		enqueueMessageReceiverEvent();
		
	}
	
	
	protected synchronized void receiveOneMessage() throws MessageReceiverException {
		if (devLog.isTraceEnabled()) {
			devLog.trace("receiveMessage() called.");
		}
		
		if (!initialized) throw new MessageReceiverRuntimeException("The message receiver is not initialized.");
			
		int index;
		DatagramSocket socket;
		String address;
		String senderAddress;
			
		index = currentSocketIndex;
		currentSocketIndex ++;
		if (currentSocketIndex >= sockets.size()) currentSocketIndex = 0;
	
		socket = sockets.get(index);		//listen on next socket and increase the next socket index;
		address = addresses.get(index);
	
		if (devLog.isDebugEnabled()) {
			devLog.debug("Checking for message - calling receive().");
		}
			
		try {
			socket.receive(packet);
			
			InetAddress socketAddress = packet.getAddress();
        	int socketPort = packet.getPort();
        	
        	if (socketAddress == null || socketPort == -1) {
        		//do nothing - return
    			return;
        	}
        	
        	senderAddress =  new StringBuilder().append(socketAddress.getHostAddress()).append(":").append(socketPort).toString();
			
		}
		catch (SocketTimeoutException e) {	//this clause first - SocketTimeoutException is a subclass of IOException
			if (devLog.isDebugEnabled()) {
				devLog.debug("Timeout of the socket.receive() call elapsed. Returning.");
			}
			//do nothing - return
			return;
		}
		catch (IOException e) {
			throw new MessageReceiverException("An exception thrown while receiving a packet", e);
		}
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Packet was received from the socket...");
		}	
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Converting received packet to a message object...");
		}
		
		byte[] byteArray = new byte[packet.getLength()];
		Arrays.copyOf(packet.getData(), packet.getLength());
		Message msg = null;
		try {
			msg = messageFactory.fromBytes(byteArray);
		} catch (MessageByteConversionException e) {
			if (msgLog.isDebugEnabled()) {
				msgLog.debug("Invalid message - could not convert to the Message object. Message discarded.", e);
			}
			if (devLog.isDebugEnabled()) {
				devLog.debug("Invalid message - could not convert to the Message object. Message discarded.", e);
			}
			//the message is invalid, discard -> do nothing
			return;
		}
		
		NetworkAdapter networkAdapter;
		//enqueue the message:
		networkAdapter = networkAdapters.get(address);

		if (networkAdapter == null) {
			//queue not defined for this ip/port
			devLog.debug("Message receiver received a message for the network address for which the networkAdapter was not registered.");
			//throw new MessageReceiverException("Message receiver received a message for the network address for which the networkAdapter was not registered.");
			return;
		}
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Received message: " + msg.getSerialNoAndSenderString());
		}
		if (msgLog.isDebugEnabled()) {
			msgLog.debug("Received message: " + msg.getSerialNoAndSenderString());
		}
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Passing the received message to the network adapter.");
		}
		
		NetworkNodePointer senderNodePointer = networkAdapter.createNetworkNodePointer(senderAddress);
		
		networkAdapters.get(address).messageReceived(msg, senderNodePointer);
		
	}
	

	public void startMessageReceiver() {
		enqueueMessageReceiverEvent();
	}
	
	public void startMessageReceiver(int numEventsToEnqueue) {
		if (numEventsToEnqueue <= 0) {
			throw new IllegalArgumentException("Illegal number of events to be enqueued.");
		}
		for (int i = 0; i < numEventsToEnqueue; i++) {
			enqueueMessageReceiverEvent();
		}
	}
	
	protected void enqueueMessageReceiverEvent() {
		
		//add the event to the queue
		boolean enqueued = false;
		while (!enqueued) {
			try {
				this.receiveEventQueue.put(new Event(environment.getTimeProvider().getCurrentTime(), EventCategory.receiveMessageEvent, messageReceiverProcessEventProxy, null));
				enqueued = true;
			} catch (InterruptedException e) {
				//do nothing, the put will be retried (enqueued is still false) 
			}
		}

	}
	
	@Override
	public synchronized void discard() {
		
		if (devLog.isInfoEnabled()) {
			devLog.info("Discarding the message receiver.");
		}
		
		this.initialized = false;
			
		buf = null;
		packet = null;
		networkAdapters = null;
		sockets = null;
		addresses = null;
		receiveEventQueue = null;
		environment = null;
		
		messageReceiverProcessEventProxy = null;
		
		properties = null;

		if (userLog.isInfoEnabled()) {
			userLog.info("Discarded the message receiver.");
		}
		if (devLog.isInfoEnabled()) {
			devLog.info("Discarded the message receiver.");
		}
		
	}

}
