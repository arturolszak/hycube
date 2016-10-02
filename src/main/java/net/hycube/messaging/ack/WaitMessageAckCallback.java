package net.hycube.messaging.ack;


public class WaitMessageAckCallback implements MessageAckCallback {

	protected boolean callbackProcessed;
	protected boolean delivered;
			
	public WaitMessageAckCallback() {
		this.callbackProcessed = false;
		this.delivered = false;
	}
	
	public synchronized boolean isProcessed() {
		return callbackProcessed;
	}
	
	public synchronized void notifyDelivered(Object callbackArg) {
		this.delivered = true;
		this.callbackProcessed = true;
		notify();
	}
	
	public synchronized void notifyUndelivered(Object callbackArg) {
		this.delivered = false;
		this.callbackProcessed = true;
		notify();
	}
	
	public synchronized void waitForAck() throws InterruptedException {
		waitForAck(0);
	}
	
	public synchronized void waitForAck(long timeout) throws InterruptedException {
		
		if (callbackProcessed) return;
		
		try {
			wait(timeout);
		} catch (InterruptedException e) {
			throw e;
		}
		
	}
	
	
	public synchronized boolean isDelivered() {
		return delivered;
	}
	
	
}
