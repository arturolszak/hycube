package net.hycube.messaging.processing;

import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.environment.NodeProperties;

public interface MessageSendProcessor {

	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException;
	
	public boolean processSendMessage(MessageSendProcessInfo mspi) throws ProcessMessageException;

	public void discard();
	
	
}
