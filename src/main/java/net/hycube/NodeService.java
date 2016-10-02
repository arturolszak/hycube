package net.hycube;

import java.math.BigInteger;
import java.util.concurrent.LinkedBlockingQueue;

import net.hycube.core.Node;
import net.hycube.core.NodeId;
import net.hycube.core.NodePointer;
import net.hycube.dht.DeleteCallback;
import net.hycube.dht.GetCallback;
import net.hycube.dht.PutCallback;
import net.hycube.dht.RefreshPutCallback;
import net.hycube.join.JoinCallback;
import net.hycube.lookup.LookupCallback;
import net.hycube.messaging.ack.MessageAckCallback;
import net.hycube.messaging.callback.MessageReceivedCallback;
import net.hycube.messaging.data.DataMessage;
import net.hycube.messaging.data.ReceivedDataMessage;
import net.hycube.messaging.processing.MessageSendInfo;
import net.hycube.search.SearchCallback;
import net.hycube.transport.NetworkNodePointer;

public interface NodeService {

	public static final String PROP_KEY_MESSAGE_RECEIVER = "MessageReceiver";
	public static final String PROP_KEY_NODE_SERVICE = "NodeService";
	
	
	public Node getNode();
	
	public NetworkNodePointer createNetworkNodePointer(String address);
	

	
	public void setPublicAddress(String addressString);
	
	public void setPublicAddress(NetworkNodePointer networkNodePointer);
	
	
	
	public MessageSendInfo send(NodeId recipientId, short sourcePort,
			short destinationPort, String directRecipientNetworkAddress,
			byte[] data) throws NodeServiceException;
	
	public MessageSendInfo send(NodeId recipientId, short sourcePort,
			short destinationPort, NetworkNodePointer directRecipient,
			byte[] data) throws NodeServiceException;

	public MessageSendInfo send(DataMessage message)
			throws NodeServiceException;

	public MessageSendInfo send(NodeId recipientId, short sourcePort,
			short destinationPort, String directRecipientNetworkAddress,
			byte[] data, MessageAckCallback ackCallback, Object callbackArg)
			throws NodeServiceException;
	
	public MessageSendInfo send(NodeId recipientId, short sourcePort,
			short destinationPort, NetworkNodePointer directRecipient,
			byte[] data, MessageAckCallback ackCallback, Object callbackArg)
			throws NodeServiceException;

	public MessageSendInfo send(DataMessage message,
			MessageAckCallback ackCallback, Object callbackArg)
			throws NodeServiceException;

	public MessageSendInfo send(NodeId recipientId, short sourcePort,
			short destinationPort, String directRecipientNetworkAddress,
			byte[] data, MessageAckCallback ackCallback, Object callbackArg, Object[] routingParameters)
			throws NodeServiceException;
	
	public MessageSendInfo send(NodeId recipientId, short sourcePort,
			short destinationPort, NetworkNodePointer directRecipient,
			byte[] data, MessageAckCallback ackCallback, Object callbackArg, Object[] routingParameters)
			throws NodeServiceException;

	public MessageSendInfo send(DataMessage message,
			MessageAckCallback ackCallback, Object callbackArg, Object[] routingParameters)
			throws NodeServiceException;

	public MessageSendInfo send(NodeId recipientId, short sourcePort,
			short destinationPort, String directRecipientNetworkAddress,
			byte[] data, MessageAckCallback ackCallback, Object callbackArg,
			boolean wait) throws NodeServiceException;
	
	public MessageSendInfo send(NodeId recipientId, short sourcePort,
			short destinationPort, NetworkNodePointer directRecipient,
			byte[] data, MessageAckCallback ackCallback, Object callbackArg,
			boolean wait) throws NodeServiceException;

	public MessageSendInfo send(DataMessage message,
			MessageAckCallback ackCallback, Object callbackArg, boolean wait)
			throws NodeServiceException;

	public MessageSendInfo send(NodeId recipientId, short sourcePort,
			short destinationPort, String directRecipientNetworkAddress,
			byte[] data, MessageAckCallback ackCallback, Object callbackArg,
			boolean wait, Object[] routingParameters) throws NodeServiceException;

	public MessageSendInfo send(NodeId recipientId, short sourcePort,
			short destinationPort, NetworkNodePointer directRecipient,
			byte[] data, MessageAckCallback ackCallback,	Object callbackArg,
			boolean wait, Object[] routingParameters) throws NodeServiceException;
	
	public MessageSendInfo send(DataMessage message,
			MessageAckCallback ackCallback, Object callbackArg, boolean wait, Object[] routingParameters)
			throws NodeServiceException;

	
	
	
	
	public LookupCallback lookup(NodeId lookupNodeId, LookupCallback lookupCallback, Object callbackArg);

	public LookupCallback lookup(NodeId lookupNodeId, LookupCallback lookupCallback, Object callbackArg, Object[] parameters);
	
	
	
	public SearchCallback search(NodeId seachNodeId, short k, SearchCallback searchCallback, Object callbackArg);
	
	
	public SearchCallback search(NodeId seachNodeId, NodePointer[] initialNodes, short k, SearchCallback searchCallback, Object callbackArg);
	

	public SearchCallback search(NodeId seachNodeId, short k, boolean ignoreTargetNode, SearchCallback searchCallback, Object callbackArg);

	public SearchCallback search(NodeId seachNodeId, NodePointer[] initialNodes, short k, boolean ignoreTargetNode, SearchCallback searchCallback, Object callbackArg);
	
	
	
	public SearchCallback search(NodeId seachNodeId, short k, SearchCallback searchCallback, Object callbackArg, Object[] parameters);
	
	
	public SearchCallback search(NodeId seachNodeId, NodePointer[] initialNodes, short k, SearchCallback searchCallback, Object callbackArg, Object[] parameters);
	

	public SearchCallback search(NodeId seachNodeId, short k, boolean ignoreTargetNode, SearchCallback searchCallback, Object callbackArg, Object[] parameters);

	public SearchCallback search(NodeId seachNodeId, NodePointer[] initialNodes, short k, boolean ignoreTargetNode, SearchCallback searchCallback, Object callbackArg, Object[] parameters);
	
	
	
	
	public PutCallback put(NodePointer np, BigInteger key, Object value, PutCallback putCallback, Object putCallbackArg);
	
	public RefreshPutCallback refreshPut(NodePointer np, BigInteger key, Object value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg);
	
	public GetCallback get(NodePointer np, BigInteger key, Object detail, GetCallback getCallback, Object getCallbackArg);
	
	public PutCallback put(BigInteger key, Object value, PutCallback putCallback, Object putCallbackArg);
	
	public RefreshPutCallback refreshPut(BigInteger key, Object value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg);
	
	public GetCallback get(BigInteger key, Object detail, GetCallback getCallback, Object getCallbackArg);
	
	public DeleteCallback delete(NodePointer np, BigInteger key, Object detail, DeleteCallback deleteCallback, Object deleteCallbackArg);
	
	public PutCallback put(NodePointer np, BigInteger key, Object value, PutCallback putCallback, Object putCallbackArg, Object parameters[]);
	
	public RefreshPutCallback refreshPut(NodePointer np, BigInteger key, Object value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg, Object parameters[]);
	
	public GetCallback get(NodePointer np, BigInteger key, Object detail, GetCallback getCallback, Object getCallbackArg, Object parameters[]);
	
	public PutCallback put(BigInteger key, Object value, PutCallback putCallback, Object putCallbackArg, Object parameters[]);
	
	public RefreshPutCallback refreshPut(BigInteger key, Object value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg, Object parameters[]);
	
	public GetCallback get(BigInteger key, Object detail, GetCallback getCallback, Object getCallbackArg, Object parameters[]);
	
	public DeleteCallback delete(NodePointer np, BigInteger key, Object detail, DeleteCallback deleteCallback, Object deleteCallbackArg, Object parameters[]);
	
	
	
	
	
	
	public void join(String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg);
	
	public void join(String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, Object[] joinParameters);

	public void leave();

	
	
	public LinkedBlockingQueue<ReceivedDataMessage> registerPort(short port);

	public LinkedBlockingQueue<ReceivedDataMessage> registerPort(short port,
			MessageReceivedCallback callback);

	public void registerMessageReceivedCallbackForPort(short port,
			MessageReceivedCallback callback);

	public void unregisterMessageReceivedCallbackForPort(short port);

	public void unregisterPort(short port);

	public void registerMessageReceivedCallback(MessageReceivedCallback callback);

	public void unregisterMessageReceivedCallback();

	
	
	public int getMaxMessageLength();
	
	public int getMaxMessageDataLength();
	
	
	
	public boolean isInitialized();

	public boolean isDiscarded();

	public void discard();
	

	
	
	
	
	
}