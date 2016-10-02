package net.hycube.messaging.ack;

import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventCategory;
import net.hycube.eventprocessing.ProcessEventProxy;

public class MessageAckCallbackEvent extends Event {

	public MessageAckCallbackEvent(long timestamp, ProcessEventProxy processEventProxy,
			MessageAckCallbackType callbackType, MessageAckCallback callback, Object callbackArg) {
		super(timestamp, EventCategory.processAckCallbackEvent, processEventProxy, createAckCallbackEventArg(callbackType, callback, callbackArg));
	}
	
	public static Object[] createAckCallbackEventArg(MessageAckCallbackType callbackType, MessageAckCallback callback, Object callbackArg) {
		Object[] arg = new Object[] {callbackType, callback, callbackArg};
		return arg;
	}
	
	
	public MessageAckCallbackType getMessageAckCallbackType() {
		return (MessageAckCallbackType) this.eventArgs[0];
	}
	
	public MessageAckCallback getMessageAckCallback() {
		return (MessageAckCallback) this.eventArgs[1];
	}
	
	public Object getCallbackArg() {
		return this.eventArgs[2];
	}
	
	
}
