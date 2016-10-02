package net.hycube.messaging.data;

import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventCategory;
import net.hycube.eventprocessing.ProcessEventProxy;
import net.hycube.messaging.callback.MessageReceivedCallback;

public class MessageReceivedCallbackEvent extends Event {

	public MessageReceivedCallbackEvent(long timestamp, ProcessEventProxy processEventProxy,
			MessageReceivedCallback callback, ReceivedDataMessage message, Short port) {
		super(timestamp, EventCategory.processMsgReceivedCallbackEvent, processEventProxy, createMessageReceivedCallbackEventArg(callback, message, port));
	}

	public MessageReceivedCallbackEvent(long timestamp, ProcessEventProxy processEventProxy,
			MessageReceivedCallback callback, ReceivedDataMessage message) {
		super(timestamp, EventCategory.processMsgReceivedCallbackEvent, processEventProxy, createMessageReceivedCallbackEventArg(callback, message));
	}
	
	
	
	public static Object[] createMessageReceivedCallbackEventArg(MessageReceivedCallback callback, ReceivedDataMessage message, Short port) {
		Object[] arg = new Object[] {callback, message, port};
		return arg;
	}
	
	public static Object[] createMessageReceivedCallbackEventArg(MessageReceivedCallback callback, ReceivedDataMessage message) {
		Object[] arg = new Object[] {callback, message};
		return arg;
	}
	
	public MessageReceivedCallback getMessageReceivedCallback() {
		return (MessageReceivedCallback) this.eventArgs[0];
	}
	
	public ReceivedDataMessage getMessage() {
		return (ReceivedDataMessage) this.eventArgs[1];
	}
	
	public Short getPort() {
		if (this.eventArgs.length >= 3) return (Short) this.eventArgs[2];
		else return null;
	}

	
	
}
