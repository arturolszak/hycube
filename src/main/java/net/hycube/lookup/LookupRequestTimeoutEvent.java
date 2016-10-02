package net.hycube.lookup;

import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.ProcessEventProxy;

public class LookupRequestTimeoutEvent extends Event {
	
	protected LookupManager lookupManager;
	
	protected int lookupId;
	protected long nodeIdHash;
	protected boolean finalLookup;
	
	
	
	public LookupRequestTimeoutEvent(LookupManager lookupManager, ProcessEventProxy eventProxy, int lookupId, long nodeIdHash, boolean finalLookup) {
		super(0, lookupManager.getLookupCallbackEventType(), eventProxy, null);
		
		this.lookupManager = lookupManager;
		
		this.lookupId = lookupId;
		this.nodeIdHash = nodeIdHash;
		
		this.finalLookup = finalLookup;
		
	}
	
	
	public int getLookupId() {
		return lookupId;
	}
	
	public void setLookupId(int lookupId) {
		this.lookupId = lookupId;
	}
	
	
	public long getNodeIdHash() {
		return this.nodeIdHash;
	}
	
	public void setNodeIdHash(long nodeIdHash) {
		this.nodeIdHash = nodeIdHash;
	}


	public boolean isFinalLookup() {
		return finalLookup;
	}


	public void setFinalLookup(boolean finalLookup) {
		this.finalLookup = finalLookup;
	}
	
	
}
