package net.hycube.eventprocessing;

public class ThreadPoolInfo {
	
	protected int poolSize;
	protected long keepAliveTimeSec;
	
	public ThreadPoolInfo(int poolSize) {
		this(poolSize, 0);
	}
	
	public ThreadPoolInfo(int poolSize, long keepAliveTimeSec) {
		this.poolSize = poolSize;
		this.keepAliveTimeSec = keepAliveTimeSec;
	}
	
	public int getPoolSize() {
		return poolSize;
	}
	
	public long getKeepAliveTimeSec() {
		return keepAliveTimeSec;
	}
	
	
}
