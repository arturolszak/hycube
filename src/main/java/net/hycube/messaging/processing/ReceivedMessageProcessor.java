package net.hycube.messaging.processing;

import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.environment.NodeProperties;
import net.hycube.messaging.messages.Message;
import net.hycube.transport.NetworkNodePointer;

public interface ReceivedMessageProcessor {

	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException;
	
	public boolean processMessage(Message msg, NetworkNodePointer directSender) throws ProcessMessageException;

	public void discard();
	
	
}
