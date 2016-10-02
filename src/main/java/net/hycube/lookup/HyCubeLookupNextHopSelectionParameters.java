package net.hycube.lookup;

import net.hycube.nexthopselection.HyCubeNextHopSelectionParameters;

public class HyCubeLookupNextHopSelectionParameters extends HyCubeNextHopSelectionParameters {

	protected short beta;
	
	protected boolean finalLookup;
	
	
	
	public short getBeta() {
		return beta;
	}
	public void setBeta(short beta) {
		this.beta = beta;
	}
	

	
	public boolean isFinalLookup() {
		return finalLookup;
	}
	public void setFinalLookup(boolean finalLookup) {
		this.finalLookup = finalLookup;
	}
	
	
	
}
