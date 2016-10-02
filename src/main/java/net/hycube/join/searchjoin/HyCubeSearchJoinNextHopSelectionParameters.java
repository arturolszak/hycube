package net.hycube.join.searchjoin;

import net.hycube.nexthopselection.HyCubeNextHopSelectionParameters;

public class HyCubeSearchJoinNextHopSelectionParameters extends HyCubeNextHopSelectionParameters {

	protected short beta;
	
	protected boolean initialRequest;
	protected boolean finalSearch;
	
		
	public short getBeta() {
		return beta;
	}
	public void setBeta(short beta) {
		this.beta = beta;
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
