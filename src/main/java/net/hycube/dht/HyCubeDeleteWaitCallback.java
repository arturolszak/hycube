package net.hycube.dht;

public class HyCubeDeleteWaitCallback extends HyCubeDeleteCallback {

	protected Boolean result;
	protected boolean returned;
	
	public HyCubeDeleteWaitCallback() {
		returned = false;

	}
	
	
		
	@Override
	public void deleteReturned(Object callbackArg, Boolean deleteResult) {
		this.result = deleteResult;
		this.returned = true;
		notify();
		
	}

	
	public synchronized boolean hasReturned() {
		return returned;
	}

	
	public synchronized Boolean waitDelete() throws InterruptedException {
		return waitDelete(0);
	}
	
	public synchronized Boolean waitDelete(long timeout) throws InterruptedException {
		
		if (returned) return result;
		
		try {
			wait(timeout);
		} catch (InterruptedException e) {
			throw e;
		}
		
		return result;
		
	}
	
	
}
