package net.hycube.join;

import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventProcessException;

public class JoinCallbackEvent extends Event {
	
	protected JoinManager joinManager;
	
	protected JoinCallback joinCallback;
	
	protected Object callbackArg;
	
	
	public JoinCallbackEvent(JoinManager joinManager, JoinCallback joinCallback, Object callbackArg) {
		super(0, joinManager.getJoinCallbackEventType(), null, null);
		
		this.joinManager = joinManager;
		
		this.joinCallback = joinCallback;
		
		this.callbackArg = callbackArg;
		
		
	}
	

	public Object getCallbackArg() {
		return callbackArg;
	}
	
	public void setCallbackArg(Object callbackArg) {
		this.callbackArg = callbackArg;
	}
	
	
	
	@Override
	public void process() throws EventProcessException {
		try {
			this.joinCallback.joinReturned(this.getCallbackArg());
		}
		catch (Exception e) {
			throw new EventProcessException("An exception was thrown while processing a join callback event.", e);
		}
	}
	
}
