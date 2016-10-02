package net.hycube.messaging.processing;

import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventCategory;
import net.hycube.eventprocessing.ProcessEventProxy;

public class PushMessageEvent extends Event {
	
	public PushMessageEvent(long timestamp, ProcessEventProxy processEventProxy,
			MessageSendProcessInfo messageSendProcessInfo, boolean systemMessage) {
		super(timestamp, (systemMessage ? EventCategory.pushMessageEvent : EventCategory.pushMessageEvent), processEventProxy, createPushMessageEventArg(messageSendProcessInfo));
	}
	
	
	public static Object[] createPushMessageEventArg(MessageSendProcessInfo messageSendProcessInfo) {
		Object[] arg = new Object[] {messageSendProcessInfo};
		return arg;
	}
	
	
	public MessageSendProcessInfo getMessageSendProcessInfo() {
		return (MessageSendProcessInfo) this.eventArgs[0];
	}
	
	
	
}
