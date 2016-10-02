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
import net.hycube.dht.HyCubeResource;
import net.hycube.dht.HyCubeResourceDescriptor;
import net.hycube.dht.PutCallback;
import net.hycube.dht.RefreshPutCallback;
import net.hycube.environment.Environment;
import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventProcessingErrorCallback;
import net.hycube.eventprocessing.EventScheduler;
import net.hycube.eventprocessing.EventType;
import net.hycube.eventprocessing.ThreadPoolInfo;
import net.hycube.join.JoinCallback;
import net.hycube.lookup.LookupCallback;
import net.hycube.messaging.ack.MessageAckCallback;
import net.hycube.messaging.callback.MessageReceivedCallback;
import net.hycube.messaging.data.DataMessage;
import net.hycube.messaging.data.ReceivedDataMessage;
import net.hycube.messaging.processing.MessageSendInfo;
import net.hycube.search.SearchCallback;
import net.hycube.transport.NetworkNodePointer;

public class HyCubeSingleQueueNodeServiceNonWakeable extends SingleQueueNodeServiceNonWakeable implements HyCubeNodeService {

//	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
//	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeSingleQueueNodeServiceNonWakeable.class); 
	
	
		
	public static HyCubeSingleQueueNodeServiceNonWakeable initializeFromConf(Environment environment, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(null, null, environment, null, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, errorCallback, errorCallbackArg);
	}
	
	public static HyCubeSingleQueueNodeServiceNonWakeable initializeFromConf(Environment environment, NodeId nodeId, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(null, null, environment, nodeId, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, errorCallback, errorCallbackArg);
	}
	
	public static HyCubeSingleQueueNodeServiceNonWakeable initializeFromConf(Environment environment, String nodeIdString, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(null, null, environment, null, nodeIdString, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, errorCallback, errorCallbackArg);
	}
	
	
	public static HyCubeSingleQueueNodeServiceNonWakeable initializeFromConf(String nodeServiceConfKey, Environment environment, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(nodeServiceConfKey, null, environment, null, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, errorCallback, errorCallbackArg);
	}
	
	public static HyCubeSingleQueueNodeServiceNonWakeable initializeFromConf(String nodeServiceConfKey, Environment environment, NodeId nodeId, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(nodeServiceConfKey, null, environment, nodeId, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, errorCallback, errorCallbackArg);
	}
	
	public static HyCubeSingleQueueNodeServiceNonWakeable initializeFromConf(String nodeServiceConfKey, Environment environment, String nodeIdString, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initializeFromConf(nodeServiceConfKey, null, environment, null, nodeIdString, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, errorCallback, errorCallbackArg);
	}
	
	protected static HyCubeSingleQueueNodeServiceNonWakeable initializeFromConf(String nodeServiceConfKey, HyCubeSingleQueueNodeServiceNonWakeable nodeService, Environment environment, NodeId nodeId, String nodeIdString, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		if (nodeService == null) {
			nodeService = new HyCubeSingleQueueNodeServiceNonWakeable();
		}
		return (HyCubeSingleQueueNodeServiceNonWakeable) SingleQueueNodeServiceNonWakeable.initializeFromConf(nodeServiceConfKey, nodeService, environment, nodeId, nodeIdString, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, errorCallback, errorCallbackArg);
	}

	
	
	
	
	public static HyCubeSingleQueueNodeServiceNonWakeable initialize(Environment environment, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, ThreadPoolInfo threadPoolInfo, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initialize(null, environment, null, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, threadPoolInfo, errorCallback, errorCallbackArg);
	}
	
	public static HyCubeSingleQueueNodeServiceNonWakeable initialize(Environment environment, NodeId nodeId, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, ThreadPoolInfo threadPoolInfo, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initialize(null, environment, nodeId, null, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, threadPoolInfo, errorCallback, errorCallbackArg);
	}
	
	public static HyCubeSingleQueueNodeServiceNonWakeable initialize(Environment environment, String nodeIdString, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, ThreadPoolInfo threadPoolInfo, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		return initialize(null, environment, null, nodeIdString, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, threadPoolInfo, errorCallback, errorCallbackArg);
	}
	
	protected static HyCubeSingleQueueNodeServiceNonWakeable initialize(HyCubeSingleQueueNodeServiceNonWakeable nodeService, Environment environment, NodeId nodeId, String nodeIdString, String networkAddress, String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, ThreadPoolInfo threadPoolInfo, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) throws InitializationException {
		
		if (nodeService == null) {
			nodeService = new HyCubeSingleQueueNodeServiceNonWakeable();
		}
		return (HyCubeSingleQueueNodeServiceNonWakeable) SingleQueueNodeServiceNonWakeable.initialize(nodeService, environment, nodeId, nodeIdString, networkAddress, bootstrapNodeAddress, joinCallback, callbackArg, threadPoolInfo, errorCallback, errorCallbackArg);		
	}
	
	
	@Override
	protected NodeProxyService initializeNodeProxyService(NodeId nodeId,
			String nodeIdString, String networkAddress,
			Environment environment,
			Map<EventType, LinkedBlockingQueue<Event>> eventQueues,
			EventScheduler eventScheduler)
			throws InitializationException {
		
		if (nodeId != null) return HyCubeNodeProxyService.initialize(nodeId, networkAddress, environment, eventQueues, eventScheduler);
		else if (nodeIdString != null && (!nodeIdString.isEmpty())) return HyCubeNodeProxyService.initialize(nodeIdString, networkAddress, environment, eventQueues, eventScheduler);
		else return HyCubeNodeProxyService.initialize(networkAddress, environment, eventQueues, eventScheduler);
		
	}
	
	
	
	
	
	//methods of HyCubeNodeService (extends NodeService) - call methods of the proxy object HyCubeNodeProxyService: 
	
	@Override
	public Node getNode() {
		if (nodeProxyService != null) return nodeProxyService.getNode();
		else return null;
	}

	@Override
	public NetworkNodePointer createNetworkNodePointer(String address) {
		if (nodeProxyService != null) return nodeProxyService.createNetworkNodePointer(address);
		else return null;
	}
	
	
	
	
	@Override
	public void setPublicAddress(String addressString) {
		if (nodeProxyService != null) nodeProxyService.setPublicAddress(addressString);
	}
	
	@Override
	public void setPublicAddress(NetworkNodePointer networkNodePointer) {
		if (nodeProxyService != null) nodeProxyService.setPublicAddress(networkNodePointer);
	}
	
	
	
	
	@Override
	public MessageSendInfo send(NodeId recipientId, short sourcePort,
			short destinationPort, String directRecipientNetworkAddress,
			byte[] data) throws NodeServiceException {
		if (nodeProxyService != null) return nodeProxyService.send(recipientId, sourcePort, destinationPort, directRecipientNetworkAddress, data);
		else return null;
	}
	
	@Override
	public MessageSendInfo send(NodeId recipientId, short sourcePort,
			short destinationPort, NetworkNodePointer directRecipient,
			byte[] data) throws NodeServiceException {
		if (nodeProxyService != null) return nodeProxyService.send(recipientId, sourcePort, destinationPort, directRecipient, data);
		else return null;
	}

	@Override
	public MessageSendInfo send(DataMessage message)
			throws NodeServiceException {
		if (nodeProxyService != null) return nodeProxyService.send(message);
		else return null;
	}


	@Override
	public MessageSendInfo send(NodeId recipientId, short sourcePort,
			short destinationPort, String directRecipientNetworkAddress,
			byte[] data, MessageAckCallback ackCallback, Object callbackArg)
			throws NodeServiceException {
		if (nodeProxyService != null) return nodeProxyService.send(recipientId, sourcePort, destinationPort, directRecipientNetworkAddress, data, ackCallback, callbackArg);
		else return null;
	}
	
	@Override
	public MessageSendInfo send(NodeId recipientId, short sourcePort,
			short destinationPort, NetworkNodePointer directRecipient,
			byte[] data, MessageAckCallback ackCallback, Object callbackArg)
			throws NodeServiceException {
		if (nodeProxyService != null) return nodeProxyService.send(recipientId, sourcePort, destinationPort, directRecipient, data, ackCallback, callbackArg);
		else return null;
	}
	

	@Override
	public MessageSendInfo send(DataMessage message,
			MessageAckCallback ackCallback, Object callbackArg)
			throws NodeServiceException {
		if (nodeProxyService != null) return nodeProxyService.send(message, ackCallback, callbackArg);
		else return null;
	}

	@Override
	public MessageSendInfo send(NodeId recipientId, short sourcePort,
			short destinationPort, String directRecipientNetworkAddress,
			byte[] data, MessageAckCallback ackCallback,
			Object callbackArg, Object[] routingParameters) throws NodeServiceException {
		if (nodeProxyService != null) return nodeProxyService.send(recipientId, sourcePort, destinationPort, directRecipientNetworkAddress, data, ackCallback, callbackArg, routingParameters);
		else return null;
	}
	
	@Override
	public MessageSendInfo send(NodeId recipientId, short sourcePort,
			short destinationPort, NetworkNodePointer directRecipient,
			byte[] data, MessageAckCallback ackCallback,
			Object callbackArg, Object[] routingParameters) throws NodeServiceException {
		if (nodeProxyService != null) return nodeProxyService.send(recipientId, sourcePort, destinationPort, directRecipient, data, ackCallback, callbackArg, routingParameters);
		else return null;
	}

	@Override
	public MessageSendInfo send(DataMessage message, 
			MessageAckCallback ackCallback, Object callbackArg, Object[] routingParameters)
			throws NodeServiceException {
		if (nodeProxyService != null) return nodeProxyService.send(message, ackCallback, callbackArg, routingParameters);
		else return null;
	}

	@Override
	public MessageSendInfo send(NodeId recipientId, short sourcePort,
			short destinationPort, String directRecipientNetworkAddress,
			byte[] data, MessageAckCallback ackCallback, Object callbackArg,
			boolean wait) throws NodeServiceException {
		if (nodeProxyService != null) return nodeProxyService.send(recipientId, sourcePort, destinationPort, directRecipientNetworkAddress, data, ackCallback, callbackArg, wait);
		else return null;
	}
	
	@Override
	public MessageSendInfo send(NodeId recipientId, short sourcePort,
			short destinationPort, NetworkNodePointer directRecipient,
			byte[] data, MessageAckCallback ackCallback, Object callbackArg,
			boolean wait) throws NodeServiceException {
		if (nodeProxyService != null) return nodeProxyService.send(recipientId, sourcePort, destinationPort, directRecipient, data, ackCallback, callbackArg, wait);
		else return null;
	}

	@Override
	public MessageSendInfo send(DataMessage message,
			MessageAckCallback ackCallback, Object callbackArg, boolean wait)
			throws NodeServiceException {
		if (nodeProxyService != null) return nodeProxyService.send(message, ackCallback, callbackArg, wait);
		else return null;
	}

	@Override
	public MessageSendInfo send(NodeId recipientId, short sourcePort,
			short destinationPort, String directRecipientNetworkAddress,
			byte[] data, MessageAckCallback ackCallback,
			Object callbackArg, boolean wait, Object[] routingParameters) throws NodeServiceException {
		if (nodeProxyService != null) return nodeProxyService.send(recipientId, sourcePort, destinationPort, directRecipientNetworkAddress, data, ackCallback, callbackArg, wait, routingParameters);
		else return null;
	}
	
	@Override
	public MessageSendInfo send(NodeId recipientId, short sourcePort,
			short destinationPort, NetworkNodePointer directRecipient,
			byte[] data, MessageAckCallback ackCallback,
			Object callbackArg, boolean wait, Object[] routingParameters) throws NodeServiceException {
		if (nodeProxyService != null) return nodeProxyService.send(recipientId, sourcePort, destinationPort, directRecipient, data, ackCallback, callbackArg, wait, routingParameters);
		else return null;
	}

	@Override
	public MessageSendInfo send(DataMessage message,
			MessageAckCallback ackCallback, Object callbackArg, boolean wait, Object[] routingParameters)
			throws NodeServiceException {
		if (nodeProxyService != null) return nodeProxyService.send(message, ackCallback, callbackArg, wait, routingParameters);
		else return null;
	}
	
	

	@Override
	public void join(String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg) {
		if (nodeProxyService != null) nodeProxyService.join(bootstrapNodeAddress, joinCallback, callbackArg);
		else return;
	}
	
	@Override
	public void join(String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, Object[] joinParameters) {
		if (nodeProxyService != null) nodeProxyService.join(bootstrapNodeAddress, joinCallback, callbackArg, joinParameters);
		else return;
	}
	

	@Override
	public void leave() {
		if (nodeProxyService != null) nodeProxyService.leave();
		else return;
	}

	@Override
	public LinkedBlockingQueue<ReceivedDataMessage> registerPort(short port) {
		if (nodeProxyService != null) return nodeProxyService.registerPort(port);
		else return null;
	}

	@Override
	public LinkedBlockingQueue<ReceivedDataMessage> registerPort(short port,
			MessageReceivedCallback callback) {
		if (nodeProxyService != null) return nodeProxyService.registerPort(port, callback);
		else return null;
	}

	@Override
	public void registerMessageReceivedCallbackForPort(short port,
			MessageReceivedCallback callback) {
		if (nodeProxyService != null) nodeProxyService.registerMessageReceivedCallbackForPort(port, callback);
		else return;
		
	}

	@Override
	public void unregisterMessageReceivedCallbackForPort(short port) {
		if (nodeProxyService != null) nodeProxyService.unregisterMessageReceivedCallbackForPort(port);
		else return;
		
	}

	@Override
	public void unregisterPort(short port) {
		if (nodeProxyService != null) nodeProxyService.unregisterPort(port);
		else return;
		
	}

	@Override
	public void registerMessageReceivedCallback(MessageReceivedCallback callback) {
		if (nodeProxyService != null) nodeProxyService.registerMessageReceivedCallback(callback);
		else return;
		
	}

	@Override
	public void unregisterMessageReceivedCallback() {
		if (nodeProxyService != null) nodeProxyService.unregisterMessageReceivedCallback();
		else return;
		
	}

	@Override
	public boolean isInitialized() {
		return this.initialized;
	}

	@Override
	public boolean isDiscarded() {
		return this.discarded;
	}


	
	@Override
	public LookupCallback lookup(NodeId lookupNodeId, LookupCallback lookupCallback, Object callbackArg) {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).lookup(lookupNodeId, lookupCallback, callbackArg);
		else return null;
	}
	
	@Override
	public LookupCallback lookup(NodeId lookupNodeId, LookupCallback lookupCallback, Object callbackArg, Object[] parameters) {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).lookup(lookupNodeId, lookupCallback, callbackArg, parameters);
		else return null;
	}

	@Override
	public SearchCallback search(NodeId seachNodeId, short k, SearchCallback searchCallback, Object callbackArg) {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).search(seachNodeId, k, searchCallback, callbackArg);
		else return null;
	}
	
	@Override
	public SearchCallback search(NodeId seachNodeId, NodePointer[] initialNodes, short k, SearchCallback searchCallback, Object callbackArg) {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).search(seachNodeId, initialNodes, k, searchCallback, callbackArg);
		else return null;
	}
	
	@Override
	public SearchCallback search(NodeId seachNodeId, short k, boolean ignoreTargetNode, SearchCallback searchCallback, Object callbackArg) {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).search(seachNodeId, k, ignoreTargetNode, searchCallback, callbackArg);
		else return null;
	}

	@Override
	public SearchCallback search(NodeId seachNodeId, NodePointer[] initialNodes, short k, boolean ignoreTargetNode, SearchCallback searchCallback, Object callbackArg) {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).search(seachNodeId, initialNodes, k, ignoreTargetNode, searchCallback, callbackArg);
		else return null;
	}
	
	@Override
	public SearchCallback search(NodeId seachNodeId, short k, SearchCallback searchCallback, Object callbackArg, Object[] parameters) {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).search(seachNodeId, k, searchCallback, callbackArg, parameters);
		else return null;
	}
	
	@Override
	public SearchCallback search(NodeId seachNodeId, NodePointer[] initialNodes, short k, SearchCallback searchCallback, Object callbackArg, Object[] parameters) {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).search(seachNodeId, initialNodes, k, searchCallback, callbackArg, parameters);
		else return null;
	}
	
	@Override
	public SearchCallback search(NodeId seachNodeId, short k, boolean ignoreTargetNode, SearchCallback searchCallback, Object callbackArg, Object[] parameters) {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).search(seachNodeId, k, ignoreTargetNode, searchCallback, callbackArg, parameters);
		else return null;
	}

	@Override
	public SearchCallback search(NodeId seachNodeId, NodePointer[] initialNodes, short k, boolean ignoreTargetNode, SearchCallback searchCallback, Object callbackArg, Object[] parameters) {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).search(seachNodeId, initialNodes, k, ignoreTargetNode, searchCallback, callbackArg, parameters);
		else return null;
	}
	


	
	@Override
	public PutCallback put(NodePointer np, BigInteger key, Object value, PutCallback putCallback, Object putCallbackArg) {
		if (nodeProxyService != null) return ((NodeProxyService)nodeProxyService).put(np, key, value, putCallback, putCallbackArg);
		else return null;
	}
	
	@Override
	public RefreshPutCallback refreshPut(NodePointer np, BigInteger key, Object value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg) {
		if (nodeProxyService != null) return ((NodeProxyService)nodeProxyService).refreshPut(np, key, value, refreshPutCallback, refreshPutCallbackArg);
		else return null;
	}
	
	@Override
	public GetCallback get(NodePointer np, BigInteger key, Object detail, GetCallback getCallback, Object getCallbackArg) {
		if (nodeProxyService != null) return ((NodeProxyService)nodeProxyService).get(np, key, detail, getCallback, getCallbackArg);
		else return null;
	}
	
	@Override
	public PutCallback put(BigInteger key, Object value, PutCallback putCallback, Object putCallbackArg) {
		if (nodeProxyService != null) return ((NodeProxyService)nodeProxyService).put(key, value, putCallback, putCallbackArg);
		else return null;
	}
	
	@Override
	public RefreshPutCallback refreshPut(BigInteger key, Object value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg) {
		if (nodeProxyService != null) return ((NodeProxyService)nodeProxyService).refreshPut(key, value, refreshPutCallback, refreshPutCallbackArg);
		else return null;
	}
	
	@Override
	public GetCallback get(BigInteger key, Object detail, GetCallback getCallback, Object getCallbackArg) {
		if (nodeProxyService != null) return ((NodeProxyService)nodeProxyService).get(key, detail, getCallback, getCallbackArg);
		else return null;
	}
	
	@Override
	public DeleteCallback delete(NodePointer np, BigInteger key, Object detail, DeleteCallback deleteCallback, Object deleteCallbackArg) {
		if (nodeProxyService != null) return ((NodeProxyService)nodeProxyService).delete(np, key, detail, deleteCallback, deleteCallbackArg);
		else return null;
	}
	
	
	
	
	
	@Override
	public PutCallback put(NodePointer np, BigInteger key, Object value, PutCallback putCallback, Object putCallbackArg, Object[] parameters) {
		if (nodeProxyService != null) return ((NodeProxyService)nodeProxyService).put(np, key, value, putCallback, putCallbackArg, parameters);
		else return null;
	}
	
	@Override
	public RefreshPutCallback refreshPut(NodePointer np, BigInteger key, Object value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg, Object[] parameters) {
		if (nodeProxyService != null) return ((NodeProxyService)nodeProxyService).refreshPut(np, key, value, refreshPutCallback, refreshPutCallbackArg, parameters);
		else return null;
	}
	
	@Override
	public GetCallback get(NodePointer np, BigInteger key, Object detail, GetCallback getCallback, Object getCallbackArg, Object[] parameters) {
		if (nodeProxyService != null) return ((NodeProxyService)nodeProxyService).get(np, key, detail, getCallback, getCallbackArg, parameters);
		else return null;
	}
	
	@Override
	public PutCallback put(BigInteger key, Object value, PutCallback putCallback, Object putCallbackArg, Object[] parameters) {
		if (nodeProxyService != null) return ((NodeProxyService)nodeProxyService).put(key, value, putCallback, putCallbackArg, parameters);
		else return null;
	}
	
	@Override
	public RefreshPutCallback refreshPut(BigInteger key, Object value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg, Object[] parameters) {
		if (nodeProxyService != null) return ((NodeProxyService)nodeProxyService).refreshPut(key, value, refreshPutCallback, refreshPutCallbackArg, parameters);
		else return null;
	}
	
	@Override
	public GetCallback get(BigInteger key, Object detail, GetCallback getCallback, Object getCallbackArg, Object[] parameters) {
		if (nodeProxyService != null) return ((NodeProxyService)nodeProxyService).get(key, detail, getCallback, getCallbackArg, parameters);
		else return null;
	}
	
	@Override
	public DeleteCallback delete(NodePointer np, BigInteger key, Object detail, DeleteCallback deleteCallback, Object deleteCallbackArg, Object[] parameters) {
		if (nodeProxyService != null) return ((NodeProxyService)nodeProxyService).delete(np, key, detail, deleteCallback, deleteCallbackArg, parameters);
		else return null;
	}
	
	
	
	
	
	
	@Override
	public PutCallback put(NodePointer np, BigInteger key, HyCubeResource value, PutCallback putCallback, Object putCallbackArg) {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).put(np, key, value, putCallback, putCallbackArg);
		else return null;
	}
	
	@Override
	public RefreshPutCallback refreshPut(NodePointer np, BigInteger key, HyCubeResourceDescriptor value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg) {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).refreshPut(np, key, value, refreshPutCallback, refreshPutCallbackArg);
		else return null;
	}
	
	@Override
	public GetCallback get(NodePointer np, BigInteger key, HyCubeResourceDescriptor criteria, GetCallback getCallback, Object getCallbackArg) {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).get(np, key, criteria, getCallback, getCallbackArg);
		else return null;
	}
	
	@Override
	public PutCallback put(BigInteger key, HyCubeResource value, PutCallback putCallback, Object putCallbackArg) {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).put(key, value, putCallback, putCallbackArg);
		else return null;
	}
	
	@Override
	public RefreshPutCallback refreshPut(BigInteger key, HyCubeResourceDescriptor value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg) {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).refreshPut(key, value, refreshPutCallback, refreshPutCallbackArg);
		else return null;
	}
	
	@Override
	public GetCallback get(BigInteger key, HyCubeResourceDescriptor criteria, GetCallback getCallback, Object getCallbackArg) {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).get(key, criteria, getCallback, getCallbackArg);
		else return null;
	}
	
	@Override
	public DeleteCallback delete(NodePointer np, BigInteger key, HyCubeResourceDescriptor criteria, DeleteCallback deleteCallback, Object deleteCallbackArg) {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).delete(np, key, criteria, deleteCallback, deleteCallbackArg);
		else return null;
	}
	
	
	
	
	
	@Override
	public PutCallback put(NodePointer np, BigInteger key, HyCubeResource value, PutCallback putCallback, Object putCallbackArg, Object[] parameters) {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).put(np, key, value, putCallback, putCallbackArg, parameters);
		else return null;
	}
	
	@Override
	public RefreshPutCallback refreshPut(NodePointer np, BigInteger key, HyCubeResourceDescriptor value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg, Object[] parameters) {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).refreshPut(np, key, value, refreshPutCallback, refreshPutCallbackArg, parameters);
		else return null;
	}
	
	@Override
	public GetCallback get(NodePointer np, BigInteger key, HyCubeResourceDescriptor criteria, GetCallback getCallback, Object getCallbackArg, Object[] parameters) {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).get(np, key, criteria, getCallback, getCallbackArg, parameters);
		else return null;
	}
	
	@Override
	public PutCallback put(BigInteger key, HyCubeResource value, PutCallback putCallback, Object putCallbackArg, Object[] parameters) {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).put(key, value, putCallback, putCallbackArg, parameters);
		else return null;
	}
	
	@Override
	public RefreshPutCallback refreshPut(BigInteger key, HyCubeResourceDescriptor value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg, Object[] parameters) {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).refreshPut(key, value, refreshPutCallback, refreshPutCallbackArg, parameters);
		else return null;
	}
	
	@Override
	public GetCallback get(BigInteger key, HyCubeResourceDescriptor criteria, GetCallback getCallback, Object getCallbackArg, Object[] parameters) {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).get(key, criteria, getCallback, getCallbackArg, parameters);
		else return null;
	}
	
	@Override
	public DeleteCallback delete(NodePointer np, BigInteger key, HyCubeResourceDescriptor criteria, DeleteCallback deleteCallback, Object deleteCallbackArg, Object[] parameters) {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).delete(np, key, criteria, deleteCallback, deleteCallbackArg, parameters);
		else return null;
	}
	
	
	
	
	
	
	@Override
	public int getMaxMessageLength() {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).getMaxMessageLength();
		else return 0;
	}
	
	@Override
	public int getMaxMessageDataLength() {
		if (nodeProxyService != null) return ((HyCubeNodeProxyService)nodeProxyService).getMaxMessageDataLength();
		else return 0;
	}
	
	
	
	

	@Override
	public void recover() {
		if (nodeProxyService != null) ((HyCubeNodeProxyService)nodeProxyService).recover();
		else return;
	}

	@Override
	public void recoverNS() {
		if (nodeProxyService != null) ((HyCubeNodeProxyService)nodeProxyService).recoverNS();
		else return;
	}

	
	
	
}
