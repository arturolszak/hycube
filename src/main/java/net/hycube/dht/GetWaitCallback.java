package net.hycube.dht;

public class GetWaitCallback implements GetCallback {

	protected Object result;
	protected boolean returned;
	
	public GetWaitCallback() {
		returned = false;

	}
	
	
		
	@Override
	public synchronized void getReturned(Object callbackArg, Object getResult) {
		this.result = getResult;
		this.returned = true;
		notify();
		
	}
	

	
	public synchronized boolean hasReturned() {
		return returned;
	}

	
	public synchronized Object waitGet() throws InterruptedException {
		return waitGet(0);
	}
	
	public synchronized Object waitGet(long timeout) throws InterruptedException {
		
		if (returned) return result;
		
		try {
			wait(timeout);
		} catch (InterruptedException e) {
			throw e;
		}
		
		return result;
		
	}





	
}
