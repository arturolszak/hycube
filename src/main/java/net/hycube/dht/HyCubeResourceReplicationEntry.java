package net.hycube.dht;

import java.math.BigInteger;


public class HyCubeResourceReplicationEntry {

	protected BigInteger key;
	protected HyCubeResourceDescriptor resourceDescriptor;
	protected long refreshTime;


	public BigInteger getKey() {
		return key;
	}
	
	
	public void setKey(BigInteger key) {
		this.key = key;
	}
	
	
	public HyCubeResourceDescriptor getResourceDescriptor() {
		return resourceDescriptor;
	}


	public void setResourceDescriptor(HyCubeResourceDescriptor resourceDescriptor) {
		this.resourceDescriptor = resourceDescriptor;
	}
	
	
	public long getRefreshTime() {
		return refreshTime;
	}
	
	
	public void setRefreshTime(long refreshTime) {
		this.refreshTime = refreshTime;
	}
	
	
	
	public HyCubeResourceReplicationEntry(BigInteger key, HyCubeResourceDescriptor resourceDescriptor, long refreshTime) {
		this.key = key;
		this.resourceDescriptor = resourceDescriptor;
		this.refreshTime = refreshTime;
		
	}
	
	
}
