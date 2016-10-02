package net.hycube.messaging.messages;

public class MessageErrorException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2481875891231333412L;

	public MessageErrorException() {
		
	}

	public MessageErrorException(String msg) {
		super(msg);
		
	}

	public MessageErrorException(Throwable e) {
		super(e);
		
	}

	public MessageErrorException(String msg, Throwable e) {
		super(msg, e);
		
	}

}
