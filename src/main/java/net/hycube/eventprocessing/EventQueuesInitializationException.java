package net.hycube.eventprocessing;

public class EventQueuesInitializationException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5792401775812467990L;

	
	public EventQueuesInitializationException() {
		
	}

	public EventQueuesInitializationException(String msg) {
		super(msg);
		
	}

	public EventQueuesInitializationException(Throwable e) {
		super(e);
		
	}

	public EventQueuesInitializationException(String msg, Throwable e) {
		super(msg, e);
		
	}
	
	
}
