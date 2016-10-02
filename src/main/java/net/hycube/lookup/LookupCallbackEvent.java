package net.hycube.lookup;

import net.hycube.core.NodePointer;
import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventProcessException;

public class LookupCallbackEvent extends Event {
	
	protected LookupManager lookupManager;
	
	protected LookupCallback lookupCallback;
	protected Object callbackArg;
	protected int lookupId;
	protected NodePointer lookupResult;
	
	
	
	public LookupCallbackEvent(LookupManager lookupManager, LookupCallback lookupCallback, Object callbackArg, int lookupId, NodePointer lookupResult) {
		super(0, lookupManager.getLookupCallbackEventType(), null, null);
		
		this.lookupManager = lookupManager;
		
		this.lookupCallback = lookupCallback;
		this.callbackArg = callbackArg;
		
		this.lookupId = lookupId;
		this.lookupResult = lookupResult;
		
		
	}
	
	
	public int getLookupId() {
		return lookupId;
	}
	
	public void setLookupId(int lookupId) {
		this.lookupId = lookupId;
	}
	
	public Object getCallbackArg() {
		return callbackArg;
	}
	
	public void setCallbackArg(Object callbackArg) {
		this.callbackArg = callbackArg;
	}
	
	
	public NodePointer getLookupResult() {
		return this.lookupResult;
	}
	
	public void setLookupResult(NodePointer lookupResult) {
		this.lookupResult = lookupResult;
	}
	
	@Override
	public void process() throws EventProcessException {
		try {
			this.lookupCallback.lookupReturned(this.getLookupId(), this.getCallbackArg(), this.getLookupResult());
		}
		catch (Exception e) {
			throw new EventProcessException("An exception was thrown while processing a lookup callback event.", e);
		}
	}
	
}
