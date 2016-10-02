package net.hycube.routing;

import net.hycube.common.EntryPoint;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodePointer;
import net.hycube.environment.NodeProperties;
import net.hycube.messaging.messages.Message;
import net.hycube.messaging.processing.MessageSendProcessInfo;
import net.hycube.messaging.processing.ProcessMessageException;
import net.hycube.transport.NetworkAdapterException;

public interface RoutingManager {


	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException;

	public abstract NodePointer findNextHop(Message msg);
	
	public boolean routeMessage(MessageSendProcessInfo info, boolean wait) throws NetworkAdapterException, ProcessMessageException;
	
	
	
	public EntryPoint getEntryPoint();

	public void discard();
	
	
}
