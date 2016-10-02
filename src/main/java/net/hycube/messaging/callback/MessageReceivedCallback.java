package net.hycube.messaging.callback;

import net.hycube.messaging.data.ReceivedDataMessage;

public interface MessageReceivedCallback {
	
	public void notifyReceived(ReceivedDataMessage message);
	
	public void notifyReceived(ReceivedDataMessage message, short port);
	
	
}
