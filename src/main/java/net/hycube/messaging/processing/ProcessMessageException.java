package net.hycube.messaging.processing;

public class ProcessMessageException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -192104986110124479L;

	public ProcessMessageException() {
		
	}

	public ProcessMessageException(String msg) {
		super(msg);
		
	}

	public ProcessMessageException(Throwable e) {
		super(e);
		
	}

	public ProcessMessageException(String msg, Throwable e) {
		super(msg, e);
		
	}

}
