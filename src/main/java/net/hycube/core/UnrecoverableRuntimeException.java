package net.hycube.core;

public class UnrecoverableRuntimeException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1220493970380048381L;


	public UnrecoverableRuntimeException() {
		
	}

	public UnrecoverableRuntimeException(String msg) {
		super(msg);
		
	}

	public UnrecoverableRuntimeException(Throwable e) {
		super(e);
		
	}

	public UnrecoverableRuntimeException(String msg, Throwable e) {
		super(msg, e);
		
	}

}
