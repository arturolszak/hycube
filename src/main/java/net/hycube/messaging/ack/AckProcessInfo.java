package net.hycube.messaging.ack;

import net.hycube.logging.LogHelper;
import net.hycube.messaging.messages.Message;
import net.hycube.transport.NetworkNodePointer;

public class AckProcessInfo {

	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(AckProcessInfo.class);
	
	protected int msgSerialNo;
	protected long discardTimestamp;
	protected Message message;
	
	protected Object[] routingParameters;
	
	protected int sendAttempts;
	protected NetworkNodePointer directRecipient;
	
	protected MessageAckCallback ackCallback;
	protected Object callbackArg;
	
	protected int ackTimeout;
	
	protected boolean processed;
	
	protected int sendCounter;
	
	
	public long getDiscardTimestamp() {
		return discardTimestamp;
	}
	
	public void setDiscardTimestamp(long discardTimestamp) {
		this.discardTimestamp = discardTimestamp;
	}
	
	public int getMessageSerialNo() {
		return msgSerialNo;
	}
	
	public void setMessageSerialNo(int msgSerialNo) {
		this.msgSerialNo = msgSerialNo;
	}
	
	public NetworkNodePointer getDirectRecipient() {
		return directRecipient;
	}
	
	public MessageAckCallback getAckCallback() {
		return ackCallback;
	}
	
	public Object getAckCallbackArg() {
		return callbackArg;
	}
	
	
	public int getAckTimeout() {
		return ackTimeout;
	}
	
	public void setAckTimeout(int ackTimeout) {
		this.ackTimeout = ackTimeout;
	}
	
	public Message getMessage() {
		return message;
	}
	
	public void setMessage(Message message) {
		this.message = message;
	}
	
	public Object[] getRoutingParameters() {
		return routingParameters;
	}
	
	public void setRoutingParameters(Object[] routingParameters) {
		this.routingParameters = routingParameters;
	}
	
	public int getSendAttempts() {
		return sendAttempts;
	}
	
	public void setSendAttempts(int sendAttempts) {
		this.sendAttempts = sendAttempts;
	}
	
	public int getSendCounter() {
		return sendCounter;
	}
	
	public void setSendCounter(int sendCounter) {
		this.sendCounter = sendCounter;
	}
	

	public boolean isProcessed() {
		return processed;
	}

	
	
	public AckProcessInfo(Message message, NetworkNodePointer directRecipient, MessageAckCallback ackCallback, Object callbackArg, Object[] routingParameters) {
		this.message = message;
		
		this.msgSerialNo = message.getSerialNo();
		this.ackCallback = ackCallback;
		this.callbackArg = callbackArg;
		
		this.sendAttempts = 0;
		this.directRecipient = directRecipient;
		
		this.ackTimeout = 0;
		
		processed = false;
	}
	
	public AckProcessInfo(Message message, NetworkNodePointer directRecipient, MessageAckCallback ackCallback, Object callbackArg, int sendAttempts, Object[] routingParameters) {
		this.message = message;
		
		this.msgSerialNo = message.getSerialNo();
		this.ackCallback = ackCallback;
		this.callbackArg = callbackArg;
		
		this.sendAttempts = sendAttempts;
		this.directRecipient = directRecipient;
		
		this.ackTimeout = 0;
		
		processed = false;
	}

	public AckProcessInfo(Message message, NetworkNodePointer directRecipient, MessageAckCallback ackCallback, Object callbackArg, int sendAttempts, int ackTimeout, Object[] routingParameters) {
		this.message = message;
		
		this.msgSerialNo = message.getSerialNo();
		this.ackCallback = ackCallback;
		this.callbackArg = callbackArg;
		
		this.sendAttempts = sendAttempts;
		this.directRecipient = directRecipient;
		
		this.ackTimeout = ackTimeout;
		
		processed = false;
	}
	


	
		
	public synchronized void process(Message ackMsg) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("ACK message processing: #" + ackMsg.getSerialNoAndSenderString() + " for message #" + this.message.getSerialNoAndSenderString());
		}
		
		
		processed = true;
		
	}
	
	public synchronized void setProcessed() {
		
		processed = true;
		
	}

	public synchronized void discard() {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("ACK not received for message #" + this.message.getSerialNoAndSenderString() + " within the specified time limit.");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("ACK not received for message #" + this.message.getSerialNoAndSenderString() + ". within the specified time limit.");
		}
		
		this.sendAttempts--;
		
		if (sendAttempts == 0) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("ACK not received for message #" + this.message.getSerialNoAndSenderString() + ". Discarding the appAckProcessInfo.");
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.debug("ACK not received for message #" + this.message.getSerialNoAndSenderString() + ". Discarding the appAckProcessInfo.");
			}
			
//			if (ackCallback != null) {
//				ackCallback.notifyUndelivered(callbackArg);
//			}

		}
		
		
		
		
	}



	

	



}
