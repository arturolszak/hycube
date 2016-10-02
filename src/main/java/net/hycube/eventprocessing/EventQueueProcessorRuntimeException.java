package net.hycube.eventprocessing;

public class EventQueueProcessorRuntimeException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2508154300304905454L;
	
	public EventQueueProcessorRuntimeException() {
		
	}

	public EventQueueProcessorRuntimeException(String msg) {
		super(msg);
		
	}

	public EventQueueProcessorRuntimeException(Throwable e) {
		super(e);
		
	}

	public EventQueueProcessorRuntimeException(String msg, Throwable e) {
		super(msg, e);
		
	}
	
}