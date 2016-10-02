package net.hycube.transport;

import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventProcessException;
import net.hycube.eventprocessing.ProcessEventProxy;

public class MessageReceiverProcessEventProxy implements ProcessEventProxy {
	
	protected MessageReceiver messageReceiver;
	
	public MessageReceiverProcessEventProxy(MessageReceiver messageReceiver) {
		this.messageReceiver = messageReceiver;
	}
	
	@Override
	public void processEvent(Event event) throws EventProcessException {
		try {
			messageReceiver.receiveMessage();
		} catch (MessageReceiverException e) {
			throw new EventProcessException("An exception thrown while receiving message from the MessageReceiver.", e);
		}
	
		
    }
}
