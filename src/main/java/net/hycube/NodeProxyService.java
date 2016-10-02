package net.hycube;

import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import net.hycube.core.InitializationException;
import net.hycube.core.Node;
import net.hycube.core.NodeId;
import net.hycube.core.NodePointer;
import net.hycube.dht.DeleteCallback;
import net.hycube.dht.GetCallback;
import net.hycube.dht.PutCallback;
import net.hycube.dht.RefreshPutCallback;
import net.hycube.environment.Environment;
import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventScheduler;
import net.hycube.eventprocessing.EventType;
import net.hycube.join.JoinCallback;
import net.hycube.logging.LogHelper;
import net.hycube.lookup.LookupCallback;
import net.hycube.messaging.ack.MessageAckCallback;
import net.hycube.messaging.callback.MessageReceivedCallback;
import net.hycube.messaging.data.DataMessage;
import net.hycube.messaging.data.ReceivedDataMessage;
import net.hycube.messaging.processing.MessageSendInfo;
import net.hycube.messaging.processing.ProcessMessageException;
import net.hycube.search.SearchCallback;
import net.hycube.transport.NetworkAdapterException;
import net.hycube.transport.NetworkNodePointer;


public class NodeProxyService implements NodeService {
	
	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(NodeProxyService.class); 

	
	public static final String PROP_KEY_NODE_PROXY_SERVICE = "NodeProxyService";
	
	
	protected Node node;
	protected boolean initialized = false;
	protected boolean discarded = false;
	
	
	
	
	
	
	public static NodeProxyService initialize(String networkAddress, Environment environment, Map<EventType, LinkedBlockingQueue<Event>> eventQueues, EventScheduler eventScheduler) throws InitializationException {
		return initialize(null, null, null, networkAddress, environment, eventQueues, eventScheduler);
	}
	
	public static NodeProxyService initialize(NodeId nodeId, String networkAddress, Environment environment, Map<EventType, LinkedBlockingQueue<Event>> eventQueues, EventScheduler eventScheduler) throws InitializationException {
		return initialize(null, nodeId, null, networkAddress, environment, eventQueues, eventScheduler);
	}
	
	public static NodeProxyService initialize(String nodeIdString, String networkAddress, Environment environment, Map<EventType, LinkedBlockingQueue<Event>> eventQueues, EventScheduler eventScheduler) throws InitializationException {
		return initialize(null, null, nodeIdString, networkAddress, environment, eventQueues, eventScheduler);
	}
	
	protected static NodeProxyService initialize(NodeProxyService nodeProxy, NodeId nodeId, String nodeIdString, String networkAddress, Environment environment, Map<EventType, LinkedBlockingQueue<Event>> eventQueues, EventScheduler eventScheduler) throws InitializationException {
		
		userLog.info("Initializing Node Service...");
		devLog.info("Initializing Node Service...");
		
		if (networkAddress == null) {
			throw new IllegalArgumentException("Could not initialize the network layer adapter. Address not specified.");
		}
		
		userLog.info("Network address: " + networkAddress);
		devLog.info("Network address: " + networkAddress);
		
		if (nodeProxy == null) {
			nodeProxy = new NodeProxyService();
		}
		
		
		//create and prepare the Node object:
		if (nodeId != null) {
			nodeProxy.node = Node.initializeNode(environment, nodeId, networkAddress, eventQueues, eventScheduler);
		}
		else if (nodeIdString != null && (!nodeIdString.isEmpty())) {
			nodeProxy.node = Node.initializeNode(environment, nodeIdString, networkAddress, eventQueues, eventScheduler);
		}
		else {
			nodeProxy.node = Node.initializeNode(environment, networkAddress, eventQueues, eventScheduler);
		}

		
		

		
		//service initialized:
		
		nodeProxy.initialized = true;
		userLog.info("Node Service initialized.");
		devLog.info("Node Service initialized.");

		
		return nodeProxy;
	}
	
	
	
	
	
	
	@Override
	public Node getNode() {
		return node;
	}
	
	
	@Override
	public NetworkNodePointer createNetworkNodePointer(String address) {
		return node.getNetworkAdapter().createNetworkNodePointer(address);
	}
	
	
	
	@Override
	public void setPublicAddress(String addressString) {
		node.getNetworkAdapter().setPublicAddress(addressString);
	}
	
	
	@Override
	public void setPublicAddress(NetworkNodePointer networkNodePointer) {
		node.getNetworkAdapter().setPublicAddress(networkNodePointer);
	}
	
	
	
	
	@Override
	public MessageSendInfo send(NodeId recipientId, short sourcePort, short destinationPort, String directRecipientNetworkAddress, byte[] data) throws NodeServiceException {
		try {
			return node.sendDataMessage(recipientId, sourcePort, destinationPort, directRecipientNetworkAddress, data, null, null, false, null);
		} catch (NetworkAdapterException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		} catch (ProcessMessageException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		}
	}
	
	
	@Override
	public MessageSendInfo send(NodeId recipientId, short sourcePort, short destinationPort, NetworkNodePointer directRecipient, byte[] data) throws NodeServiceException {
		try {
			return node.sendDataMessage(recipientId, sourcePort, destinationPort, directRecipient, data, null, null, false, null);
		} catch (NetworkAdapterException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		} catch (ProcessMessageException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		}
	}
	
	
	@Override
	public MessageSendInfo send(DataMessage message) throws NodeServiceException {
		try {
			return node.sendDataMessage(message.getRecipientId(), message.getSourcePort(), message.getDestinationPort(), message.getRecipientNetworkAddress(), message.getData(), null, null, false, null);
		} catch (NetworkAdapterException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		} catch (ProcessMessageException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		}
	}
	

	
	
	@Override
	public MessageSendInfo send(NodeId recipientId, short sourcePort, short destinationPort, String directRecipientNetworkAddress, byte[] data, MessageAckCallback ackCallback, Object callbackArg) throws NodeServiceException {
		try {
			return node.sendDataMessage(recipientId, sourcePort, destinationPort, directRecipientNetworkAddress, data, ackCallback, callbackArg, false, null);
		} catch (NetworkAdapterException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		} catch (ProcessMessageException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		}
	}
	
	
	@Override
	public MessageSendInfo send(NodeId recipientId, short sourcePort, short destinationPort, NetworkNodePointer directRecipient, byte[] data, MessageAckCallback ackCallback, Object callbackArg) throws NodeServiceException {
		try {
			return node.sendDataMessage(recipientId, sourcePort, destinationPort, directRecipient, data, ackCallback, callbackArg, false, null);
		} catch (NetworkAdapterException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		} catch (ProcessMessageException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		}
	}
	
	
	@Override
	public MessageSendInfo send(DataMessage message, MessageAckCallback ackCallback, Object callbackArg) throws NodeServiceException {
		try {
			return node.sendDataMessage(message.getRecipientId(), message.getSourcePort(), message.getDestinationPort(), message.getRecipientNetworkAddress(), message.getData(), ackCallback, callbackArg, false, null);
		} catch (NetworkAdapterException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		} catch (ProcessMessageException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		}
	}
	
	
	@Override
	public MessageSendInfo send(NodeId recipientId, short sourcePort, short destinationPort, String directRecipientNetworkAddress, byte[] data, MessageAckCallback ackCallback, Object callbackArg, Object[] routingParameters) throws NodeServiceException {
		try {
			return node.sendDataMessage(recipientId, sourcePort, destinationPort, directRecipientNetworkAddress, data, ackCallback, callbackArg, false, routingParameters);
		} catch (NetworkAdapterException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		} catch (ProcessMessageException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		}
	}
	
	
	@Override
	public MessageSendInfo send(NodeId recipientId, short sourcePort, short destinationPort, NetworkNodePointer directRecipient, byte[] data, MessageAckCallback ackCallback, Object callbackArg, Object[] routingParameters) throws NodeServiceException {
		try {
			return node.sendDataMessage(recipientId, sourcePort, destinationPort, directRecipient, data, ackCallback, callbackArg, false, routingParameters);
		} catch (NetworkAdapterException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		} catch (ProcessMessageException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		}
	}
	
	
	@Override
	public MessageSendInfo send(DataMessage message, MessageAckCallback ackCallback, Object callbackArg, Object[] routingParameters) throws NodeServiceException {
		try {
			return node.sendDataMessage(message.getRecipientId(), message.getSourcePort(), message.getDestinationPort(), message.getRecipientNetworkAddress(), message.getData(), ackCallback, callbackArg, false, routingParameters);
		} catch (NetworkAdapterException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		} catch (ProcessMessageException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		}
	}

	
	@Override
	public MessageSendInfo send(NodeId recipientId, short sourcePort, short destinationPort, String directRecipientNetworkAddress, byte[] data, MessageAckCallback ackCallback, Object callbackArg, boolean wait) throws NodeServiceException {
		try {
			return node.sendDataMessage(recipientId, sourcePort, destinationPort, directRecipientNetworkAddress, data, ackCallback, callbackArg, wait, null);
		} catch (NetworkAdapterException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		} catch (ProcessMessageException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		}
	}
	
	
	@Override
	public MessageSendInfo send(NodeId recipientId, short sourcePort, short destinationPort, NetworkNodePointer directRecipient, byte[] data, MessageAckCallback ackCallback, Object callbackArg, boolean wait) throws NodeServiceException {
		try {
			return node.sendDataMessage(recipientId, sourcePort, destinationPort, directRecipient, data, ackCallback, callbackArg, wait, null);
		} catch (NetworkAdapterException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		} catch (ProcessMessageException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		}
	}
	
	
	@Override
	public MessageSendInfo send(DataMessage message, MessageAckCallback ackCallback, Object callbackArg, boolean wait) throws NodeServiceException {
		try {
			return node.sendDataMessage(message.getRecipientId(), message.getSourcePort(), message.getDestinationPort(), message.getRecipientNetworkAddress(), message.getData(), ackCallback, callbackArg, wait, null);
		} catch (NetworkAdapterException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		} catch (ProcessMessageException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		}
	}

	
	@Override
	public MessageSendInfo send(NodeId recipientId, short sourcePort, short destinationPort, String directRecipientNetworkAddress, byte[] data, MessageAckCallback ackCallback, Object callbackArg, boolean wait, Object[] routingParameters) throws NodeServiceException {
		try {
			return node.sendDataMessage(recipientId, sourcePort, destinationPort, directRecipientNetworkAddress, data, ackCallback, callbackArg, wait, routingParameters);
		} catch (NetworkAdapterException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		} catch (ProcessMessageException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		}
	}
	
	
	@Override
	public MessageSendInfo send(NodeId recipientId, short sourcePort, short destinationPort, NetworkNodePointer directRecipient, byte[] data, MessageAckCallback ackCallback, Object callbackArg, boolean wait, Object[] routingParameters) throws NodeServiceException {
		try {
			return node.sendDataMessage(recipientId, sourcePort, destinationPort, directRecipient, data, ackCallback, callbackArg, wait, routingParameters);
		} catch (NetworkAdapterException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		} catch (ProcessMessageException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		}
	}
	
	
	@Override
	public MessageSendInfo send(DataMessage message, MessageAckCallback ackCallback, Object callbackArg, boolean wait, Object[] routingParameters) throws NodeServiceException {
		try {
			return node.sendDataMessage(message.getRecipientId(), message.getSourcePort(), message.getDestinationPort(), message.getRecipientNetworkAddress(), message.getData(), ackCallback, callbackArg, wait, routingParameters);
		} catch (NetworkAdapterException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		} catch (ProcessMessageException e) {
			throw new NodeServiceException("An error occured while sending the message", e);
		}
	}
	

	
	
	@Override
	public void join(String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg) {
		this.node.join(bootstrapNodeAddress, joinCallback, callbackArg);
	}
	
	@Override
	public void join(String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, Object[] joinParameters) {
		this.node.join(bootstrapNodeAddress, joinCallback, callbackArg, joinParameters);
	}
	

	
	@Override
	public void leave() {
		this.node.leave();
	}
	
	
	
	
	
	//ports:

	@Override
	public LinkedBlockingQueue<ReceivedDataMessage> registerPort(short port) {
		return node.registerPort(port);
	}
	
	@Override
	public LinkedBlockingQueue<ReceivedDataMessage> registerPort(short port, MessageReceivedCallback callback) {
		return node.registerPort(port, callback);
	}
	
	@Override
	public void registerMessageReceivedCallbackForPort(short port, MessageReceivedCallback callback) {
		node.registerMessageReceivedCallbackForPort(port, callback);
	}
	
	@Override
	public void unregisterMessageReceivedCallbackForPort(short port) {
		node.unregisterMessageReceivedCallbackForPort(port);
	}
	
	@Override
	public void unregisterPort(short port) {
		node.unregisterPort(port);
	}
	
	
	//ports diabled:
	
	@Override
	public void registerMessageReceivedCallback(MessageReceivedCallback callback) {
		node.registerMessageReceivedCallback(callback);
	}
	
	@Override
	public void unregisterMessageReceivedCallback() {
		node.unregisterMessageReceivedCallback();
	}
	
	
	
	@Override
	public void discard() {
		
		userLog.info("Discarding the node...");
		devLog.info("Discarding the node...");
		
		//make all necessary clearing
		this.node.discard();
		this.node = null;

		discarded = true;
		initialized = false;
		
		userLog.info("Node discarded.");
		devLog.info("Node discarded.");
		
	}
	
	
	
	
	

	
	@Override
	public boolean isInitialized() {
		return initialized;
	}
	
	
	@Override
	public boolean isDiscarded() {
		return discarded;
	}

	
	
	
	@Override
	public LookupCallback lookup(NodeId lookupNodeId, LookupCallback lookupCallback,
			Object callbackArg) {
		return node.lookup(lookupNodeId, lookupCallback, callbackArg);
		
	}
	
	@Override
	public LookupCallback lookup(NodeId lookupNodeId, LookupCallback lookupCallback,
			Object callbackArg, Object[] parameters) {
		return node.lookup(lookupNodeId, lookupCallback, callbackArg, parameters);
		
	}

	@Override
	public SearchCallback search(NodeId seachNodeId, short k,
			SearchCallback searchCallback, Object callbackArg) {
		return node.search(seachNodeId, k, searchCallback, callbackArg);
	}

	@Override
	public SearchCallback search(NodeId seachNodeId,
			NodePointer[] initialNodes, short k, SearchCallback searchCallback,
			Object callbackArg) {
		return node.search(seachNodeId, initialNodes, k, searchCallback, callbackArg);
	}

	@Override
	public SearchCallback search(NodeId seachNodeId, short k,
			boolean ignoreTargetNode, SearchCallback searchCallback,
			Object callbackArg) {
		return node.search(seachNodeId, k, ignoreTargetNode, searchCallback, callbackArg);
	}

	@Override
	public SearchCallback search(NodeId seachNodeId,
			NodePointer[] initialNodes, short k, boolean ignoreTargetNode,
			SearchCallback searchCallback, Object callbackArg) {
		return node.search(seachNodeId, initialNodes, k, ignoreTargetNode, searchCallback, callbackArg);
	}


	
	@Override
	public SearchCallback search(NodeId seachNodeId, short k,
			SearchCallback searchCallback, Object callbackArg,
			Object[] parameters) {
		return node.search(seachNodeId, k, searchCallback, callbackArg, parameters);
	}

	@Override
	public SearchCallback search(NodeId seachNodeId,
			NodePointer[] initialNodes, short k, SearchCallback searchCallback,
			Object callbackArg,
			Object[] parameters) {
		return node.search(seachNodeId, initialNodes, k, searchCallback, callbackArg, parameters);
	}

	@Override
	public SearchCallback search(NodeId seachNodeId, short k,
			boolean ignoreTargetNode, SearchCallback searchCallback,
			Object callbackArg,
			Object[] parameters) {
		return node.search(seachNodeId, k, ignoreTargetNode, searchCallback, callbackArg, parameters);
	}

	@Override
	public SearchCallback search(NodeId seachNodeId,
			NodePointer[] initialNodes, short k, boolean ignoreTargetNode,
			SearchCallback searchCallback, Object callbackArg,
			Object[] parameters) {
		return node.search(seachNodeId, initialNodes, k, ignoreTargetNode, searchCallback, callbackArg, parameters);
	}

	
	
	
	
	@Override
	public PutCallback put(NodePointer np, BigInteger key, Object value, PutCallback putCallback, Object putCallbackArg) {
		return node.put(np, key, value, putCallback, putCallbackArg);
	}
	
	@Override
	public RefreshPutCallback refreshPut(NodePointer np, BigInteger key, Object value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg) {
		return node.refreshPut(np, key, value, refreshPutCallback, refreshPutCallbackArg);
	}
	
	@Override
	public GetCallback get(NodePointer np, BigInteger key, Object detail, GetCallback getCallback, Object getCallbackArg) {
		return node.get(np, key, detail, getCallback, getCallbackArg);
	}
	
	@Override
	public PutCallback put(BigInteger key, Object value, PutCallback putCallback, Object putCallbackArg) {
		return node.put(key, value, putCallback, putCallbackArg);
	}
	
	@Override
	public RefreshPutCallback refreshPut(BigInteger key, Object value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg) {
		return node.refreshPut(key, value, refreshPutCallback, refreshPutCallbackArg);
	}
	
	@Override
	public GetCallback get(BigInteger key, Object detail, GetCallback getCallback, Object getCallbackArg) {
		return node.get(key, detail, getCallback, getCallbackArg);
	}
	
	@Override
	public DeleteCallback delete(NodePointer np, BigInteger key, Object detail, DeleteCallback deleteCallback, Object deleteCallbackArg) {
		return node.delete(np, key, detail, deleteCallback, deleteCallbackArg);
	}
	
	@Override
	public PutCallback put(NodePointer np, BigInteger key, Object value, PutCallback putCallback, Object putCallbackArg, Object[] parameters) {
		return node.put(np, key, value, putCallback, putCallbackArg, parameters);
	}
	
	@Override
	public RefreshPutCallback refreshPut(NodePointer np, BigInteger key, Object value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg, Object[] parameters) {
		return node.refreshPut(np, key, value, refreshPutCallback, refreshPutCallbackArg, parameters);
	}
	
	@Override
	public GetCallback get(NodePointer np, BigInteger key, Object detail, GetCallback getCallback, Object getCallbackArg, Object[] parameters) {
		return node.get(np, key, detail, getCallback, getCallbackArg, parameters);
	}
	
	@Override
	public PutCallback put(BigInteger key, Object value, PutCallback putCallback, Object putCallbackArg, Object[] parameters) {
		return node.put(key, value, putCallback, putCallbackArg, parameters);
	}
	
	@Override
	public RefreshPutCallback refreshPut(BigInteger key, Object value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg, Object[] parameters) {
		return node.refreshPut(key, value, refreshPutCallback, refreshPutCallbackArg, parameters);
	}
	
	@Override
	public GetCallback get(BigInteger key, Object detail, GetCallback getCallback, Object getCallbackArg, Object[] parameters) {
		return node.get(key, detail, getCallback, getCallbackArg, parameters);
	}
	
	@Override
	public DeleteCallback delete(NodePointer np, BigInteger key, Object detail, DeleteCallback deleteCallback, Object deleteCallbackArg, Object[] parameters) {
		return node.delete(np, key, detail, deleteCallback, deleteCallbackArg, parameters);
	}
	
	
	
	
	@Override
	public int getMaxMessageLength() {
		return node.getMaxMessageLength();
	}
	
	@Override
	public int getMaxMessageDataLength() {
		return node.getMaxMessageDataLength();
	}
	
	
	
	
}
