package net.hycube.dht;

public class HyCubeGetWaitCallback extends HyCubeGetCallback {

	protected HyCubeResource[] result;
	protected boolean returned;
	
	public HyCubeGetWaitCallback() {
		returned = false;

	}
	
	
		
	@Override
	public void getReturned(Object callbackArg, HyCubeResource[] getResult) {
		this.result = getResult;
		this.returned = true;
		notify();
		
	}
	

	
	public synchronized boolean hasReturned() {
		return returned;
	}

	
	public synchronized HyCubeResource[] waitGet() throws InterruptedException {
		return waitGet(0);
	}
	
	public synchronized HyCubeResource[] waitGet(long timeout) throws InterruptedException {
		
		if (returned) return result;
		
		try {
			wait(timeout);
		} catch (InterruptedException e) {
			throw e;
		}
		
		return result;
		
	}





	
}
