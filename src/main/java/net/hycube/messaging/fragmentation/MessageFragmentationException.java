package net.hycube.messaging.fragmentation;

public class MessageFragmentationException extends Exception {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = -8889875038086297244L;

	
	
	public MessageFragmentationException() {
		super();
	}

	public MessageFragmentationException(String message) {
		super(message);
	}

	public MessageFragmentationException(Throwable cause) {
		super(cause);
	}

	public MessageFragmentationException(String message, Throwable cause) {
		super(message, cause);
	}

}
