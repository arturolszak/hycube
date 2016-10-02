package net.hycube.eventprocessing;

public class EventProcessException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2198445781408552607L;

	public EventProcessException() {

	}

	public EventProcessException(String msg) {
		super(msg);
	}

	public EventProcessException(Throwable e) {
		super(e);
	}

	public EventProcessException(String msg, Throwable e) {
		super(msg, e);
	}

}
