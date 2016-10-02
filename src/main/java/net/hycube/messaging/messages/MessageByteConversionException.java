package net.hycube.messaging.messages;

public class MessageByteConversionException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4059578689682629860L;

	public MessageByteConversionException() {
		super();
	}

	public MessageByteConversionException(String msg, Throwable e) {
		super(msg, e);
	}

	public MessageByteConversionException(String msg) {
		super(msg);
	}

	public MessageByteConversionException(Throwable e) {
		super(e);
	}

	
	
}
