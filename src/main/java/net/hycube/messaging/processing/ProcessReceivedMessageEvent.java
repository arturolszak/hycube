package net.hycube.messaging.processing;

import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventCategory;
import net.hycube.eventprocessing.ProcessEventProxy;
import net.hycube.messaging.messages.Message;
import net.hycube.transport.NetworkNodePointer;

public class ProcessReceivedMessageEvent extends Event {

	public ProcessReceivedMessageEvent(long timestamp, ProcessEventProxy processEventProxy,
			Message message, NetworkNodePointer directSender) {
		super(timestamp, EventCategory.processReceivedMessageEvent, processEventProxy, createProcessReceivedMessageEventArg(message, directSender));
	}
	
	
	public static Object[] createProcessReceivedMessageEventArg(Message message, NetworkNodePointer directSender) {
		Object[] arg = new Object[] {message, directSender};
		return arg;
	}
	
	public Message getMessage() {
		return (Message) this.eventArgs[0];
	}
	
	public NetworkNodePointer getDirectSender() {
		return (NetworkNodePointer) this.eventArgs[1];
	}
	
}
