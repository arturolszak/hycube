package net.hycube.eventprocessing;


public class EventQueueProcessingInfo {
	protected ThreadPoolInfo threadPoolInfo;
	protected EventType[] eventTypes;
	protected boolean wakeable;

	public EventQueueProcessingInfo(ThreadPoolInfo threadPoolInfo, EventType[] eventTypes, boolean wakeable) {
		this.threadPoolInfo = threadPoolInfo;
		this.eventTypes = eventTypes;
		this.wakeable = wakeable;
	}
	
	public ThreadPoolInfo getThreadPoolInfo() {
		return threadPoolInfo;
	}

	public void setThreadPoolInfo(ThreadPoolInfo threadPoolInfo) {
		this.threadPoolInfo = threadPoolInfo;
	}
	
	public EventType[] getEventTypes() {
		return eventTypes;
	}
	
	public void setEventTypes(EventType[] eventTypes) {
		this.eventTypes = eventTypes;
	}
	
	public boolean getWakeable() {
		return wakeable;
	}
	
	public void setWakeable(boolean wakeable) {
		this.wakeable = wakeable;
	}
	
}
