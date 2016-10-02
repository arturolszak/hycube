package net.hycube.transport;

public class MessageReceiverException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3458857480469456576L;

	public MessageReceiverException() {

	}

	public MessageReceiverException(String msg) {
		super(msg);
	}

	public MessageReceiverException(Throwable e) {
		super(e);
	}

	public MessageReceiverException(String msg, Throwable e) {
		super(msg, e);
	}

}
