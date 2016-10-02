package net.hycube.eventprocessing;


public class EventQueueProcessingInfoNonWakeable {
	protected ThreadPoolInfo threadPoolInfo;
	protected EventType[] eventTypes;
	protected boolean wakeable;

	public EventQueueProcessingInfoNonWakeable(ThreadPoolInfo threadPoolInfo, EventType[] eventTypes) {
		this.threadPoolInfo = threadPoolInfo;
		this.eventTypes = eventTypes;
	}
	
	public ThreadPoolInfo getThreadPoolInfo() {
		return threadPoolInfo;
	}

	public void setThreadPoolInfo(
			ThreadPoolInfo threadPoolInfo) {
		this.threadPoolInfo = threadPoolInfo;
	}
	
	public EventType[] getEventTypes() {
		return eventTypes;
	}
	
	public void setEventTypes(EventType[] eventTypes) {
		this.eventTypes = eventTypes;
	}
	
}