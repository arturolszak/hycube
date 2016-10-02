package net.hycube.dht;

public class HyCubePutWaitCallback extends HyCubePutCallback {

	protected Boolean result;
	protected boolean returned;
	
	public HyCubePutWaitCallback() {
		returned = false;

	}
	
	
		
	@Override
	public void putReturned(Object callbackArg, Boolean putResult) {
		this.result = putResult;
		this.returned = true;
		notify();
		
	}

	
	public synchronized boolean hasReturned() {
		return returned;
	}

	
	public synchronized Boolean waitPut() throws InterruptedException {
		return waitPut(0);
	}
	
	public synchronized Boolean waitPut(long timeout) throws InterruptedException {
		
		if (returned) return result;
		
		try {
			wait(timeout);
		} catch (InterruptedException e) {
			throw e;
		}
		
		return result;
		
	}

	
}
