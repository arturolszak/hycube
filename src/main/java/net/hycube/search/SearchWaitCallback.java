package net.hycube.search;

import net.hycube.core.NodePointer;

public class SearchWaitCallback implements SearchCallback {

	protected int searchId;
	protected NodePointer[] result;
	
	protected boolean returned;
	
	public SearchWaitCallback() {
		returned = false;
		result = null;
	}
	
	
		
	@Override
	public synchronized void searchReturned(int searchId, Object callbackArg, NodePointer[] result) {
		this.searchId = searchId;
		this.result = result;
		returned = true;
		notify();
		
	}

	
	public synchronized boolean hasReturned() {
		return returned;
	}

	
	public synchronized NodePointer[] waitForResult() throws InterruptedException {
		return waitForResult(0);
	}
	
	public synchronized NodePointer[] waitForResult(long timeout) throws InterruptedException {
		
		if (returned) return result;
		
		try {
			wait(timeout);
		} catch (InterruptedException e) {
			throw e;
		}
		
		return result;
		
	}
	
	
}

