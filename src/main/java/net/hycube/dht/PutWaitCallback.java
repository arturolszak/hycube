package net.hycube.dht;

public class PutWaitCallback implements PutCallback {

	protected Object result;
	protected boolean returned;
	
	public PutWaitCallback() {
		returned = false;

	}
	
	
		
	@Override
	public synchronized void putReturned(Object callbackArg, Object putResult) {
		this.result = putResult;
		this.returned = true;
		notify();
		
	}

	
	public synchronized boolean hasReturned() {
		return returned;
	}

	
	public synchronized Object waitPut() throws InterruptedException {
		return waitPut(0);
	}
	
	public synchronized Object waitPut(long timeout) throws InterruptedException {
		
		if (returned) return result;
		
		try {
			wait(timeout);
		} catch (InterruptedException e) {
			throw e;
		}
		
		return result;
		
	}

	
}
