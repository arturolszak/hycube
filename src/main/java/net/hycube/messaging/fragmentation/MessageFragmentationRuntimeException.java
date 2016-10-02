package net.hycube.messaging.fragmentation;

public class MessageFragmentationRuntimeException extends RuntimeException {


	/**
	 * 
	 */
	private static final long serialVersionUID = 8367280401716918503L;

	
	
	public MessageFragmentationRuntimeException() {
		super();
	}

	public MessageFragmentationRuntimeException(String message) {
		super(message);
	}

	public MessageFragmentationRuntimeException(Throwable cause) {
		super(cause);
	}

	public MessageFragmentationRuntimeException(String message, Throwable cause) {
		super(message, cause);
	}

}
