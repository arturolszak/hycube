package net.hycube.search;

import net.hycube.nexthopselection.HyCubeNextHopSelectionParameters;

public class HyCubeSearchNextHopSelectionParameters extends HyCubeNextHopSelectionParameters {

	protected short beta;
	
	protected boolean finalSearch;
	
		
	public short getBeta() {
		return beta;
	}
	public void setBeta(short beta) {
		this.beta = beta;
	}
	

	
	public boolean isFinalSearch() {
		return finalSearch;
	}
	public void setFinalSearch(boolean finalSearch) {
		this.finalSearch = finalSearch;
	}
	
	
	
}
