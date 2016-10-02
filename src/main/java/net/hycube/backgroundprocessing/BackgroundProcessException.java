package net.hycube.backgroundprocessing;

public class BackgroundProcessException extends RuntimeException {


	/**
	 * 
	 */
	private static final long serialVersionUID = 7098741617627061884L;

	public BackgroundProcessException() {

	}

	public BackgroundProcessException(String msg) {
		super(msg);
	}

	public BackgroundProcessException(Throwable e) {
		super(e);
	}

	public BackgroundProcessException(String msg, Throwable e) {
		super(msg, e);
	}

}
