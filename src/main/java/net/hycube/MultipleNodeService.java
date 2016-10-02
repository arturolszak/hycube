package net.hycube;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import net.hycube.core.InitializationException;
import net.hycube.core.NodeId;
import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventType;
import net.hycube.join.JoinCallback;
import net.hycube.transport.MessageReceiver;
import net.hycube.transport.MessageReceiverException;

public interface MultipleNodeService {

	public Map<EventType, LinkedBlockingQueue<Event>> getEventQueues();

	public MessageReceiver initializeMessageReceiver()
			throws InitializationException;

	public NodeService initializeNode(String networkAddress,
			String bootstrapNodeAddress, JoinCallback joinCallback,
			Object callbackArg, MessageReceiver messageReceiver)
			throws InitializationException;

	public NodeService initializeNode(NodeId nodeId, String networkAddress,
			String bootstrapNodeAddress, JoinCallback joinCallback,
			Object callbackArg, MessageReceiver messageReceiver)
			throws InitializationException;

	public NodeService initializeNode(String nodeIdString,
			String networkAddress, String bootstrapNodeAddress,
			JoinCallback joinCallback, Object callbackArg,
			MessageReceiver messageReceiver) throws InitializationException;

	public NodeService initializeNode(NodeId nodeId, String nodeIdString,
			String networkAddress, String bootstrapNodeAddress,
			JoinCallback joinCallback, Object callbackArg,
			MessageReceiver messageReceiver) throws InitializationException;

	public void discardMessageReceiver(MessageReceiver messageReceiver)
			throws MessageReceiverException;

	public void discardNode(NodeService nodeService);

	public void discard();

}