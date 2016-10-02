package net.hycube.dht;

import java.math.BigInteger;

public class HyCubeResourceEntry {

	
	protected HyCubeResource resource;
	protected long refreshTime;
	protected BigInteger key;
	protected boolean deleted;
	


	public HyCubeResource getResource() {
		return resource;
	}


	public void setResource(HyCubeResource resource) {
		this.resource = resource;
	}
	
	
	public long getRefreshTime() {
		return refreshTime;
	}
	
	
	public void setRefreshTime(long refreshTime) {
		this.refreshTime = refreshTime;
	}
	
	
	public BigInteger getKey() {
		return key;
	}
	
	
	public void setKey(BigInteger key) {
		this.key = key;
	}
	
	
	public boolean isDeleted() {
		return deleted;
	}
	
	
	public void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}
	
	
	
	public HyCubeResourceEntry(BigInteger key, HyCubeResource resource, long time) {
		this.key = key;
		this.resource = resource;
		this.refreshTime = time;
		this.deleted = false;
		
	}
	
	
}
