package net.hycube.messaging.processing;

import net.hycube.messaging.messages.Message;
import net.hycube.transport.NetworkNodePointer;

public class MessageSendProcessInfo {
	
	protected Message msg;
	protected NetworkNodePointer directRecipient;
	
	protected Object[] routingParameters;
	
	protected boolean processBeforeSend;
	
	
	public MessageSendProcessInfo(Message msg, NetworkNodePointer directRecipient, boolean processBeforeSend) {
		this.msg = msg;
		this.directRecipient = directRecipient;
		this.processBeforeSend = processBeforeSend;
	}
	
	public MessageSendProcessInfo(Message msg, NetworkNodePointer directRecipient) {
		this.msg = msg;
		this.directRecipient = directRecipient;
		this.processBeforeSend = false;
	}
	
	public MessageSendProcessInfo(Message msg) {
		this.msg = msg;
		this.directRecipient = null;
		this.processBeforeSend = false;
	}
	
	public MessageSendProcessInfo(Message msg, NetworkNodePointer directRecipient, boolean processBeforeSend, Object[] routingParameters) {
		this.msg = msg;
		this.directRecipient = directRecipient;
		this.processBeforeSend = processBeforeSend;
		this.routingParameters = routingParameters;
	}
	
	public MessageSendProcessInfo(Message msg, NetworkNodePointer directRecipient, Object[] routingParameters) {
		this.msg = msg;
		this.directRecipient = directRecipient;
		this.processBeforeSend = false;
		this.routingParameters = routingParameters;
	}
	
	public MessageSendProcessInfo(Message msg, Object[] routingParameters) {
		this.msg = msg;
		this.directRecipient = null;
		this.processBeforeSend = false;
		this.routingParameters = routingParameters;
	}
	



	public Message getMsg() {
		return msg;
	}

	public void setMsg(Message msg) {
		this.msg = msg;
	}

	public NetworkNodePointer getDirectRecipient() {
		return directRecipient;
	}

	public void setDirectRecipient(NetworkNodePointer directRecipient) {
		this.directRecipient = directRecipient;
	}

	public boolean getProcessBeforeSend() {
		return processBeforeSend;
	}

	public void setProcessBeforeSend(boolean processSend) {
		this.processBeforeSend = processSend;
	}

	public Object[] getRoutingParameters() {
		return routingParameters;
	}
	
	public void setRoutingParameters(Object[] routingParameters) {
		this.routingParameters = routingParameters;
	}
	
	
	
}
