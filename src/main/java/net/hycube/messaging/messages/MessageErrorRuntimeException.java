package net.hycube.messaging.messages;

public class MessageErrorRuntimeException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1263806482101483433L;

	public MessageErrorRuntimeException() {
		
	}

	public MessageErrorRuntimeException(String msg) {
		super(msg);
		
	}

	public MessageErrorRuntimeException(Throwable e) {
		super(e);
		
	}

	public MessageErrorRuntimeException(String msg, Throwable e) {
		super(msg, e);
		
	}

}
