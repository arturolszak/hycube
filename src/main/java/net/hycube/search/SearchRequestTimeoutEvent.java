package net.hycube.search;

import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.ProcessEventProxy;

public class SearchRequestTimeoutEvent extends Event {
	
	protected SearchManager searchManager;
	
	protected int searchId;
	protected long nodeIdHash;
	protected boolean finalSearch;
	
	
	
	public SearchRequestTimeoutEvent(SearchManager searchManager, ProcessEventProxy eventProxy, int searchId, long nodeIdHash, boolean finalSearch) {
		super(0, searchManager.getSearchRequestTimeoutEventType(), eventProxy, null);
		
		this.searchManager = searchManager;
		
		this.searchId = searchId;
		this.nodeIdHash = nodeIdHash;
		
		this.finalSearch = finalSearch;
		
		
	}
	
	
	public int getSearchId() {
		return searchId;
	}
	
	public void setSearchId(int searchId) {
		this.searchId = searchId;
	}
	
	
	public long getNodeIdHash() {
		return this.nodeIdHash;
	}
	
	public void setNodeIdHash(long nodeIdHash) {
		this.nodeIdHash = nodeIdHash;
	}


	public boolean isFinalSearch() {
		return finalSearch;
	}


	public void setFinalSearch(boolean finalSearch) {
		this.finalSearch = finalSearch;
	}
	
	
}
