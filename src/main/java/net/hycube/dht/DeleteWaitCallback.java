package net.hycube.dht;

public class DeleteWaitCallback implements DeleteCallback {

	protected Object result;
	protected boolean returned;
	
	public DeleteWaitCallback() {
		returned = false;

	}
	
	
		
	@Override
	public synchronized void deleteReturned(Object callbackArg, Object deleteResult) {
		this.result = deleteResult;
		this.returned = true;
		notify();
		
	}

	
	public synchronized boolean hasReturned() {
		return returned;
	}

	
	public synchronized Object waitDelete() throws InterruptedException {
		return waitDelete(0);
	}
	
	public synchronized Object waitDelete(long timeout) throws InterruptedException {
		
		if (returned) return result;
		
		try {
			wait(timeout);
		} catch (InterruptedException e) {
			throw e;
		}
		
		return result;
		
	}
	
	
}
