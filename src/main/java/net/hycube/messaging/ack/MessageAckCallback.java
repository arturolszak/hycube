package net.hycube.messaging.ack;

public interface MessageAckCallback {

	public void notifyDelivered(Object callbackArg);
	public void notifyUndelivered(Object callbackArg);
	
	
}
