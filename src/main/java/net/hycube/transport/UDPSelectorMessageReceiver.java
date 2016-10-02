package net.hycube.transport;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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

public class UDPSelectorMessageReceiver implements MessageReceiver {

	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(UDPMessageReceiver.class); 
	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
	
	public static final int RECEIVE_BUFFER_SIZE = 65535;
	public static final int SELECTOR_SELECT_TIMEOUT = 1000;
	
	protected NodeProperties properties;
	protected Selector selector;
	protected HashMap<String, NetworkAdapter> networkAdapters;
	protected List<DatagramChannel> channels;
	protected List<String> addresses;
	protected HashMap<String, SelectionKey> selectionKeys;
	protected BlockingQueue<Event> receiveEventQueue;
	protected int currentSocketIndex = 0;
	protected Object selectLock = new Object();
	protected boolean initialized = false;
	protected Environment environment;
	protected MessageFactory messageFactory;
	byte[] byteArray = new byte[RECEIVE_BUFFER_SIZE];
    protected ByteBuffer buff = ByteBuffer.wrap(byteArray);
    
    protected boolean hold = false;
    protected boolean wasHeld = false;
    protected int wasHeldNum = 0;
    protected Object holdLock = new Object();
    
    protected boolean wakeable;
    protected Object wakeableLock = new Object();
    
    protected MessageReceiverProcessEventProxy messageReceiverProcessEventProxy;
    
    
	public boolean isInitialized() {
		return initialized;
	}
	

    @Override
	public synchronized void initialize(Environment environment, BlockingQueue<Event> receiveEventQueue, NodeProperties properties) throws MessageReceiverException, InitializationException {
		if (devLog.isInfoEnabled()) {
			devLog.info("Initializing message receiver.");
		}
		
		if (receiveEventQueue == null) {
			throw new IllegalArgumentException("receiveEventQueue is null.");
		}
		
		if (environment == null) {
			throw new IllegalArgumentException("environment is null.");
		}
		
		try {
			selector = Selector.open();
		} catch (IOException e) {
			throw new MessageReceiverException("An exception thrown while opening the Selector.", e);
		}
		
		this.properties = properties;
		
		this.networkAdapters = new HashMap<String, NetworkAdapter>();
		this.addresses = new ArrayList<String>();
		this.channels = new ArrayList<DatagramChannel>();
		this.selectionKeys = new HashMap<String, SelectionKey>();
		
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
		
		
		this.hold = false;
		this.wasHeld = false;
		
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
		if (networkAdapter instanceof UDPSelectorNetworkAdapter) {
			registerNetworkAdapter((UDPSelectorNetworkAdapter)networkAdapter);
		}
		else {
			throw new IllegalArgumentException("The network adapter should be an instance of UDPSelectorNetworkAdapter class.");
		}
	}
	
	public synchronized void registerNetworkAdapter(UDPSelectorNetworkAdapter networkAdapter) throws MessageReceiverException {
		
		if (devLog.isInfoEnabled()) {
			devLog.info("Registering new network adapter.");
		}
		
		if (!initialized) throw new MessageReceiverRuntimeException("The message receiver is not initialized.");
		
		hold();	//after hold() call, no new selections will be made
		wakeup();	//wake up the current selection
		synchronized(selectLock) {	//waits for the current receive to finish and does not allow select meanwhile
			unhold();	//hold is no longer needed, selectLock is acquired

			if (networkAdapters.containsKey(networkAdapter.getPublicAddressString())) {
				throw new MessageReceiverRuntimeException("The message receiver already registered a network adapter with the same network address.");
			}
			
			this.networkAdapters.put(networkAdapter.getPublicAddressString(), networkAdapter);
			DatagramChannel channel = networkAdapter.getChannel();
			this.channels.add(channel);
			this.addresses.add(networkAdapter.getPublicAddressString());
			
			try {
				SelectionKey selKey = channel.register(selector, SelectionKey.OP_READ);
				this.selectionKeys.put(networkAdapter.getPublicAddressString(), selKey);
			} catch (ClosedChannelException e) {
				throw new MessageReceiverException("An exception thrown while registering the channel with the selector.", e);
			} finally {
				
			}
		}
			
		if (userLog.isInfoEnabled()) {
			userLog.info("Registered new network adapter. Network address: " + networkAdapter.getPublicAddressString());
		}
		if (devLog.isInfoEnabled()) {
			devLog.info("Registered new network adapter. Network address: " + networkAdapter.getPublicAddressString());
		}
		
	}

	@Override
	public synchronized void unregisterNetworkAdapter(NetworkAdapter networkAdapter) {
		if (networkAdapter instanceof UDPSelectorNetworkAdapter) {
			unregisterNetworkAdapter((UDPSelectorNetworkAdapter)networkAdapter);
		}
		else {
			throw new IllegalArgumentException("The network adapter should be an instance of UDPSelectorNetworkAdapter class.");
		}
	}
	
	public synchronized void unregisterNetworkAdapter(UDPSelectorNetworkAdapter networkAdapter) {
		if (devLog.isInfoEnabled()) {
			devLog.info("Unregistering new network adapter.");
		}
		
		if (!initialized) throw new MessageReceiverRuntimeException("The message receiver is not initialized.");
		
		hold();	//after hold() call, no new selections will be made
		wakeup();	//wake up the current selection
		synchronized(selectLock) {	//waits for the current receive to finish and does not allow select meanwhile
			unhold();	//hold is no longer needed, selectLock is acquired
			
			if (! this.networkAdapters.containsKey(networkAdapter.getPublicAddressString())) {
				//do nothing - this network adapter is not registered for this instance of network receiver
				return;
			}
			
			this.networkAdapters.remove(networkAdapter.getPublicAddressString());
			this.channels.remove(networkAdapter.getChannel());
			this.addresses.remove(networkAdapter.getPublicAddressString());
			
			try {
				this.selectionKeys.get(networkAdapter.getPublicAddressString()).cancel();
				//this.selector.keys().remove(this.selectionKeys.get(networkAdapter.getPublicAddressString()));
			} finally {
				
			}
		}
		
		
		if (userLog.isInfoEnabled()) {
			userLog.info("Unregistered new network adapter. Network address: " + networkAdapter.getPublicAddressString());
		}
		if (devLog.isInfoEnabled()) {
			devLog.info("Unregistered new network adapter. Network address: " + networkAdapter.getPublicAddressString());
		}
		
	}
	
	@Override
	//waits for messages on a selector and then retrieves all the messages that are immediately available from the socket
	public void receiveMessage() throws MessageReceiverException {
		
		if (devLog.isTraceEnabled()) {
			devLog.trace("receiveMessage() called.");
		}
		
		if (!initialized) throw new MessageReceiverRuntimeException("The message receiver is not initialized.");
		
		synchronized(selectLock) {
			
			//check again if initialized (initialization synchronizes on selectLock)
			if (!isInitialized()) return;
			
			if (checkHoldAndSetHeld()) {
				//return, new events will not be enqueued, the following unhold() call will enqueue the messagereceiver again 
				return;
			}
			
			if (devLog.isDebugEnabled()) {
				devLog.debug("Checking for message - calling select().");
			}
			
			try {
				selector.select(SELECTOR_SELECT_TIMEOUT);
			} catch (IOException e) {
				throw new MessageReceiverException("An exception thrown during the select() call.", e);
			} finally {
				synchronized (wakeableLock) {
					this.wakeable = false;
				}
			}
			
			//will also get all immediately available messages
			int selectedAfter;
			do {
				Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
	            while (selectedKeys.hasNext()) {
	            	SocketAddress sa = null;
	            	Message msg = null;
	            	DatagramSocket socket = null;
	            	String address = null;
	            	String senderAddress = null;
	
	                SelectionKey key = selectedKeys.next();
	                selectedKeys.remove();
	 
	                if (!key.isValid()) {
	                	continue;
	                }
	
	                if (key.isReadable()) {
	                	
	                	if (devLog.isDebugEnabled()) {
	    					devLog.debug("Receiving packet from the socket...");
	    				}
	                	
	                	DatagramChannel chan = (DatagramChannel)key.channel();
	                	socket = chan.socket();
	                	
	                	InetAddress socketLocalAddress = socket.getLocalAddress();
	                	int socketLocalPort = socket.getLocalPort();
	                		                	
	                	if (socketLocalAddress == null || socketLocalPort == -1) {
	                		continue;
	                	}
	                	
	                	address = new StringBuilder().append(socketLocalAddress.getHostAddress()).append(":").append(socketLocalPort).toString();
	                	
	                	
	                	buff.rewind();
	                    try {
							sa = chan.receive(buff);
	                    } catch (ClosedByInterruptException e) {
	                    	//do nothing
	                    	continue;
						} catch (IOException e) {
							throw new MessageReceiverException("An exception thrown during channel.receive() call.", e);
						}
	                    if (sa == null) {
	                    	continue;
	                    }
	                    
	                    InetSocketAddress isa = (InetSocketAddress) sa;
	                    
	                    senderAddress =  new StringBuilder().append(isa.getAddress().getHostAddress()).append(":").append(isa.getPort()).toString();
	                    
	                    
	            		if (devLog.isDebugEnabled()) {
	            			devLog.debug("Packet was received from the socket...");
	            		}	
	                    
	                    if (devLog.isDebugEnabled()) {
	                    	devLog.debug("Converting received packet to a message object...");
	    				}
	                    
	                    try {
							msg = messageFactory.fromBytes(buff.array());
						} catch (MessageByteConversionException e) {
							if (msgLog.isDebugEnabled()) {
								msgLog.debug("Invalid message - could not convert to the Message object. Message discarded.", e);
							}
							if (devLog.isDebugEnabled()) {
								devLog.debug("Invalid message - could not convert to the Message object. Message discarded.", e);
							}
							//the message is invalid, discard -> do nothing
							continue;
						}
	                }
	                
	                if (msg != null) {
	                	
		                NetworkAdapter networkAdapter;
		       			
		       			//enqueue the message:
		       			networkAdapter = networkAdapters.get(address);

		       			if (networkAdapter == null) {
		       				devLog.debug("Message receiver received a message for the network address for which the networkAdapter was not registered.");
		       				//throw new MessageReceiverException("Message receiver received a message for the network address for which the networkAdapter was not registered.");
		       				continue;
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
	                
	            }
	            
	            try {
	            	//the selectNow() call will also clear the wakeup flag of the selector, which would affect the next select call if wakeup() was called after the previous select() call
					selectedAfter = selector.selectNow();
					//!!!clear the selected keys if we want only to get one message at a time
					//Iterator<SelectionKey> selectedKeysAfter = selector.selectedKeys().iterator();
		            //while (selectedKeysAfter.hasNext()) {
		            //	selectedKeysAfter.remove();
		            //}
					//selectedAfter = 0;
				} catch (IOException e) {
					throw new MessageReceiverException("An exception thrown during the selectNow() call.", e);
				} 
			} while (selectedAfter > 0);
		
			
			enqueueMessageReceiverEvent();
			
		}
		
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
		
		synchronized (wakeableLock) {
			this.wakeable = true;
		}
		
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
	public synchronized void discard() throws MessageReceiverException {
		
		if (devLog.isInfoEnabled()) {
			devLog.info("Discarding the message receiver.");
		}
		
		hold();	//after hold() call, no new selections will be made
		wakeup();	//wake up the current selection
		synchronized(selectLock) {	//waits for the current receive to finish and does not allow select meanwhile
			//unhold();	//hold is no longer needed, selectLock is acquired

			this.initialized = false;
			
			networkAdapters = null;
			channels = null;
			addresses = null;
			selectionKeys = null;
			receiveEventQueue = null;
			currentSocketIndex = 0;
			environment = null;
	
			try {
				selector.close();
			} catch (IOException e) {
				throw new MessageReceiverException("An exception thrown while closing the selector.", e);
			}
			selector = null;
			
			hold = false;
			wasHeld = false;
			wakeable = false;
			
			messageReceiverProcessEventProxy = null;
			
			properties = null;
			
		}
		
		if (userLog.isInfoEnabled()) {
			userLog.info("Discarded the message receiver.");
		}
		if (devLog.isInfoEnabled()) {
			devLog.info("Discarded the message receiver.");
		}
		
	}
	
	
	protected void wakeup() {
		synchronized (wakeableLock) {
			if (wakeable) selector.wakeup();
		}
	}
	
	
    protected void hold() {
    	synchronized(holdLock) {
    		hold = true;
    	}
    }
	
    protected void unhold() {
    	synchronized(holdLock) {
    		hold = false;
    		if (wasHeld) {
    			wasHeld = false;	
    			while (wasHeldNum > 0) {
    				enqueueMessageReceiverEvent();
    				wasHeldNum--;
    			}
    		}
    		
    	}
    }
    
    protected boolean checkHoldAndSetHeld() {
    	synchronized(holdLock) {
    		if (hold) {
    			wasHeld = true;
    			wasHeldNum++;
    			synchronized (wakeableLock) {
    				this.wakeable = false;	//will not block on select()
    			}
    			return true;
    		}
    		else return false;
    	}
    }
    

}
