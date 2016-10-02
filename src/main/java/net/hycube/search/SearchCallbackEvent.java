package net.hycube.search;

import net.hycube.core.NodePointer;
import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventProcessException;

public class SearchCallbackEvent extends Event {
	
	protected SearchManager searchManager;
	
	protected SearchCallback searchCallback;
	protected Object callbackArg;
	protected int searchId;
	protected NodePointer[] searchResult;
	
	
	
	public SearchCallbackEvent(SearchManager searchManager, SearchCallback searchCallback, Object callbackArg, int searchId, NodePointer[] searchResult) {
		super(0, searchManager.getSearchCallbackEventType(), null, null);
		
		this.searchManager = searchManager;
		
		this.searchCallback = searchCallback;
		this.callbackArg = callbackArg;
		
		this.searchId = searchId;
		this.searchResult = searchResult;
		
		
	}
	
	
	public int getSearchId() {
		return searchId;
	}
	
	public void setSearchId(int searchId) {
		this.searchId = searchId;
	}
	
	
	public Object getCallbackArg() {
		return callbackArg;
	}
	
	public void setCallbackArg(Object callbackArg) {
		this.callbackArg = callbackArg;
	}
	
	
	public NodePointer[] getSearchResult() {
		return this.searchResult;
	}
	
	public void setSearchResult(NodePointer[] searchResult) {
		this.searchResult = searchResult;
	}
	
	@Override
	public void process() throws EventProcessException {
		try {
			this.searchCallback.searchReturned(this.getSearchId(), this.getCallbackArg(), this.getSearchResult());
		}
		catch (Exception e) {
			throw new EventProcessException("An exception was thrown while processing a search callback event.", e);
		}
	}
	
}
