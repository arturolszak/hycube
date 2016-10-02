package net.hycube.pastry.join.routejoin;

import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.ProcessEventProxy;
import net.hycube.join.JoinManager;

public class PastryRouteJoinTimeoutEvent extends Event {
	
	protected JoinManager joinManager;
	
	protected int joinId;
	
	public PastryRouteJoinTimeoutEvent(PastryRouteJoinManager joinManager, ProcessEventProxy eventProxy, int joinId) {
		super(0, joinManager.getJoinTimeoutEventType(), eventProxy, null);
		
		this.joinManager = joinManager;
		
		this.joinId = joinId;
		
		
	}
	
}
