package net.hycube.join;


public class JoinWaitCallback implements JoinCallback {
	
	protected boolean returned;
	
	public JoinWaitCallback() {
		returned = false;

	}
	
	
		
	@Override
	public synchronized void joinReturned(Object callbackArg) {
		returned = true;
		notify();
		
	}

	
	public synchronized boolean hasReturned() {
		return returned;
	}

	
	public synchronized boolean waitJoin() throws InterruptedException {
		return waitJoin(0);
	}
	
	public synchronized boolean waitJoin(long timeout) throws InterruptedException {
		
		if (returned) return returned;
		
		try {
			wait(timeout);
		} catch (InterruptedException e) {
			throw e;
		}
		
		return returned;
		
	}
	
	
}

