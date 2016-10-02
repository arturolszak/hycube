package net.hycube.lookup;

import net.hycube.core.NodePointer;

public class LookupWaitCallback implements LookupCallback {

	protected int lookupId;
	protected NodePointer result;
	
	protected boolean returned;
	
	public LookupWaitCallback() {
		returned = false;
		result = null;
	}
	
	
	public int getLookupId() {
		return lookupId;
	}
	
		
	@Override
	public synchronized void lookupReturned(int lookupId, Object callbackArg, NodePointer result) {
		this.lookupId = lookupId;
		this.result = result;
		returned = true;
		notify();
		
	}

	
	public synchronized boolean hasReturned() {
		return returned;
	}

	
	public synchronized NodePointer waitForResult() throws InterruptedException {
		return waitForResult(0);
	}
	
	public synchronized NodePointer waitForResult(long timeout) throws InterruptedException {
		
		if (returned) return result;
		
		try {
			wait(timeout);
		} catch (InterruptedException e) {
			throw e;
		}
		
		return result;
		
	}
	
	
}

