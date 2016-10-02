package net.hycube.dht;

public class RefreshPutWaitCallback implements RefreshPutCallback {

	protected Object result;
	protected boolean returned;
	
	public RefreshPutWaitCallback() {
		returned = false;

	}
	
	
		
	@Override
	public synchronized void refreshPutReturned(Object callbackArg, Object refreshPutResult) {
		this.result = refreshPutResult;
		this.returned = true;
		notify();
		
	}

	
	public synchronized boolean hasReturned() {
		return returned;
	}

	
	public synchronized Object waitRefreshPut() throws InterruptedException {
		return waitRefreshPut(0);
	}
	
	public synchronized Object waitRefreshPut(long timeout) throws InterruptedException {
		
		if (returned) return result;
		
		try {
			wait(timeout);
		} catch (InterruptedException e) {
			throw e;
		}
		
		return result;
		
	}

	
}
