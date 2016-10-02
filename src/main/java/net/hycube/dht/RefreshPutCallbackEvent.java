package net.hycube.dht;

import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventProcessException;

public class RefreshPutCallbackEvent extends Event {
	
	protected DHTManager dhtManager;
	
	protected RefreshPutCallback refreshPutCallback;
	protected Object callbackArg;
	protected int commandId;
	protected boolean refreshPutStatus;
	
	



	public RefreshPutCallback getRefreshPutCallback() {
		return refreshPutCallback;
	}

	public void setRefreshPutCallback(RefreshPutCallback refreshPutCallback) {
		this.refreshPutCallback = refreshPutCallback;
	}

	public Object getCallbackArg() {
		return callbackArg;
	}

	public void setCallbackArg(Object callbackArg) {
		this.callbackArg = callbackArg;
	}

	public int getCommandId() {
		return commandId;
	}

	public void setCommandId(int commandId) {
		this.commandId = commandId;
	}

	public boolean getRefreshPutStatus() {
		return refreshPutStatus;
	}

	public void setRefreshPutStatus(boolean refreshPutStatus) {
		this.refreshPutStatus = refreshPutStatus;
	}
	

	
	
	public RefreshPutCallbackEvent(DHTManager dhtManager, RefreshPutCallback refreshPutCallback, Object callbackArg, int commandId, boolean refreshPutStatus) {
		super(0, dhtManager.getRefreshPutCallbackEventType(), null, null);
		
		this.dhtManager = dhtManager;
		
		this.refreshPutCallback = refreshPutCallback;
		this.callbackArg = callbackArg;
		
		this.commandId = commandId;
		this.refreshPutStatus = refreshPutStatus;
		
		
	}
	
	

	
	@Override
	public void process() throws EventProcessException {
		try {
			this.refreshPutCallback.refreshPutReturned(this.getCallbackArg(), this.getRefreshPutStatus());
		}
		catch (Exception e) {
			throw new EventProcessException("An exception was thrown while processing a refresh put callback event.", e);
		}
	}


	
}
