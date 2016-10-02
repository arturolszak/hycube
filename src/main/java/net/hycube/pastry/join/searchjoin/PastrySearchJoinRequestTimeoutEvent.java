package net.hycube.pastry.join.searchjoin;

import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.ProcessEventProxy;
import net.hycube.join.JoinManager;

public class PastrySearchJoinRequestTimeoutEvent extends Event {
	
	protected JoinManager joinManager;
	
	protected int joinId;
	protected String nodeAddressString;
	protected long nodeIdHash;
	protected boolean initialRequest;
	protected boolean finalSearch;
	
	
	
	public PastrySearchJoinRequestTimeoutEvent(PastrySearchJoinManager joinManager, ProcessEventProxy eventProxy, int joinId, String nodeAddressString, long nodeIdHash, boolean initialRequest, boolean finalSearch) {
		super(0, joinManager.getJoinRequestTimeoutEventType(), eventProxy, null);
		
		this.joinManager = joinManager;
		
		this.joinId = joinId;
		this.nodeAddressString = nodeAddressString;
		this.nodeIdHash = nodeIdHash;
		
		this.initialRequest = initialRequest;
		this.finalSearch = finalSearch;
		
		
	}
	
	
	public int getJoinId() {
		return joinId;
	}
	
	public void setJoinId(int joinId) {
		this.joinId = joinId;
	}
	
	
	public long getNodeIdHash() {
		return this.nodeIdHash;
	}
	
	public void setNodeIdHash(long nodeIdHash) {
		this.nodeIdHash = nodeIdHash;
	}


	public boolean isInitialRequest() {
		return initialRequest;
	}


	public void setInitialRequest(boolean initialRequest) {
		this.initialRequest = initialRequest;
	}
	
	
	
	public boolean isFinalSearch() {
		return finalSearch;
	}


	public void setFinalSearch(boolean finalSearch) {
		this.finalSearch = finalSearch;
	}
	
	
}
