package net.hycube.dht;

public class HyCubeRefreshPutWaitCallback extends HyCubeRefreshPutCallback {

	protected Boolean result;
	protected boolean returned;
	
	public HyCubeRefreshPutWaitCallback() {
		returned = false;

	}
	
	
		
	@Override
	public void refreshPutReturned(Object callbackArg, Boolean refreshPutResult) {
		this.result = refreshPutResult;
		this.returned = true;
		notify();
		
	}

	
	public synchronized boolean hasReturned() {
		return returned;
	}

	
	public synchronized Boolean waitRefreshPut() throws InterruptedException {
		return waitRefreshPut(0);
	}
	
	public synchronized Boolean waitRefreshPut(long timeout) throws InterruptedException {
		
		if (returned) return result;
		
		try {
			wait(timeout);
		} catch (InterruptedException e) {
			throw e;
		}
		
		return result;
		
	}

	
}
