package net.hycube.dht;

import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventProcessException;

public class DeleteCallbackEvent extends Event {
	
	protected DHTManager dhtManager;
	
	protected DeleteCallback deleteCallback;
	protected Object callbackArg;
	protected int commandId;
	protected boolean deleteStatus;
	
	



	public DeleteCallback getDeleteCallback() {
		return deleteCallback;
	}

	public void setDeleteCallback(DeleteCallback deleteCallback) {
		this.deleteCallback = deleteCallback;
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

	public boolean getDeleteStatus() {
		return deleteStatus;
	}

	public void setDeleteStatus(boolean deleteStatus) {
		this.deleteStatus = deleteStatus;
	}
	

	
	
	public DeleteCallbackEvent(DHTManager dhtManager, DeleteCallback deleteCallback, Object callbackArg, int commandId, boolean deleteStatus) {
		super(0, dhtManager.getDeleteCallbackEventType(), null, null);
		
		this.dhtManager = dhtManager;
		
		this.deleteCallback = deleteCallback;
		this.callbackArg = callbackArg;
		
		this.commandId = commandId;
		this.deleteStatus = deleteStatus;
		
		
	}
	
	

	
	@Override
	public void process() throws EventProcessException {
		try {
			this.deleteCallback.deleteReturned(this.getCallbackArg(), this.getDeleteStatus());
		}
		catch (Exception e) {
			throw new EventProcessException("An exception was thrown while processing a delete callback event.", e);
		}
	}


	
}
