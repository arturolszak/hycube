package net.hycube.dht;

import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventProcessException;

public class GetCallbackEvent extends Event {
	
	protected DHTManager dhtManager;
	
	protected GetCallback getCallback;
	protected Object callbackArg;
	protected int commandId;
	protected Object getResult;
	
	



	public GetCallback getGetCallback() {
		return getCallback;
	}

	public void setGetCallback(GetCallback getCallback) {
		this.getCallback = getCallback;
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

	public Object getGetResult() {
		return getResult;
	}

	public void setGetResult(Object getResult) {
		this.getResult = getResult;
	}
	

	
	
	public GetCallbackEvent(DHTManager dhtManager, GetCallback getCallback, Object callbackArg, int commandId, Object getResult) {
		super(0, dhtManager.getPutCallbackEventType(), null, null);
		
		this.dhtManager = dhtManager;
		
		this.getCallback = getCallback;
		this.callbackArg = callbackArg;
		
		this.commandId = commandId;
		this.getResult = getResult;
		
		
	}
	
	

	
	@Override
	public void process() throws EventProcessException {
		try {
			this.getCallback.getReturned(this.getCallbackArg(), this.getGetResult());
		}
		catch (Exception e) {
			throw new EventProcessException("An exception was thrown while processing a put callback event.", e);
		}
	}


	
}
