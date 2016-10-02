package net.hycube.transport;

import net.hycube.messaging.messages.Message;

public interface ReceivedMessageProcessProxy {
	
	public void messageReceived(Message msg, NetworkNodePointer directSender);

	
}
