package net.hycube.dht;

import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventProcessException;

public class PutCallbackEvent extends Event {
	
	protected DHTManager dhtManager;
	
	protected PutCallback putCallback;
	protected Object callbackArg;
	protected int commandId;
	protected boolean putStatus;
	
	



	public PutCallback getPutCallback() {
		return putCallback;
	}

	public void setPutCallback(PutCallback putCallback) {
		this.putCallback = putCallback;
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

	public boolean getPutStatus() {
		return putStatus;
	}

	public void setPutStatus(boolean putStatus) {
		this.putStatus = putStatus;
	}
	

	
	
	public PutCallbackEvent(DHTManager dhtManager, PutCallback putCallback, Object callbackArg, int commandId, boolean putStatus) {
		super(0, dhtManager.getPutCallbackEventType(), null, null);
		
		this.dhtManager = dhtManager;
		
		this.putCallback = putCallback;
		this.callbackArg = callbackArg;
		
		this.commandId = commandId;
		this.putStatus = putStatus;
		
		
	}
	
	

	
	@Override
	public void process() throws EventProcessException {
		try {
			this.putCallback.putReturned(this.getCallbackArg(), this.getPutStatus());
		}
		catch (Exception e) {
			throw new EventProcessException("An exception was thrown while processing a put callback event.", e);
		}
	}


	
}
