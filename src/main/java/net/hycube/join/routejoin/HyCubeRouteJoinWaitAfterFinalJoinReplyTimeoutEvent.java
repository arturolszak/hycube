package net.hycube.join.routejoin;

import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.ProcessEventProxy;
import net.hycube.join.JoinManager;

public class HyCubeRouteJoinWaitAfterFinalJoinReplyTimeoutEvent extends Event {
	
	protected JoinManager joinManager;
	
	protected int joinId;
	
	public HyCubeRouteJoinWaitAfterFinalJoinReplyTimeoutEvent(HyCubeRouteJoinManager joinManager, ProcessEventProxy eventProxy, int joinId) {
		super(0, joinManager.getWaitAfterFinalJoinReplyTimeoutEventType(), eventProxy, null);
		
		this.joinManager = joinManager;
		
		this.joinId = joinId;
		
		
	}
	
}
