package net.hycube.join.routejoin;

import net.hycube.join.JoinCallback;

public class HyCubeRouteJoinData {

	
	protected int joinId;
	

	protected String bootstrapNodeAddress;
	
	protected boolean secureRouting;
	protected boolean skipRandomNextHops;
	
	protected JoinCallback joinCallback;
	protected Object callbackArg;
	
	
	protected boolean publicNetworkAddressDiscovered;
	
	
	
	
	
	public int getJoinId() {
		return joinId;
	}


	public void setJoinId(int joinId) {
		this.joinId = joinId;
	}


	public String getBootstrapNodeAddress() {
		return bootstrapNodeAddress;
	}


	public void setBootstrapNodeAddress(String bootstrapNodeAddress) {
		this.bootstrapNodeAddress = bootstrapNodeAddress;
	}


	public boolean isSecureRouting() {
		return secureRouting;
	}


	public void setSecureRouting(boolean secureRouting) {
		this.secureRouting = secureRouting;
	}
	
	
	public boolean isSkipRandomNextHops() {
		return skipRandomNextHops;
	}


	public void setSkipRandomNextHops(boolean skipRandomNextHops) {
		this.skipRandomNextHops = skipRandomNextHops;
	}


	public JoinCallback getJoinCallback() {
		return joinCallback;
	}


	public void setJoinCallback(JoinCallback joinCallback) {
		this.joinCallback = joinCallback;
	}


	public Object getCallbackArg() {
		return callbackArg;
	}


	public void setCallbackArg(Object callbackArg) {
		this.callbackArg = callbackArg;
	}


	public boolean isPublicNetworkAddressDiscovered() {
		return publicNetworkAddressDiscovered;
	}


	public void setPublicNetworkAddressDiscovered(
			boolean publicNetworkAddressDiscovered) {
		this.publicNetworkAddressDiscovered = publicNetworkAddressDiscovered;
	}
	
	
	
	
	
	public HyCubeRouteJoinData(int joinId, String bootstrapNodeAddress, boolean secureRouting, boolean skipRandomNextHops) {
		
		this.joinId = joinId;
		
		this.bootstrapNodeAddress = bootstrapNodeAddress;
		
		this.publicNetworkAddressDiscovered = false;
		
		this.secureRouting = secureRouting;
		
		this.skipRandomNextHops = skipRandomNextHops;
		
		
	}
	
	
	
	
	
	public void discard() {
		
	}
	
	
	
}
