package net.hycube.transport;

public class MessageReceiverRuntimeException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6045153832210870598L;

	public MessageReceiverRuntimeException() {

	}

	public MessageReceiverRuntimeException(String msg) {
		super(msg);
	}

	public MessageReceiverRuntimeException(Throwable e) {
		super(e);
	}

	public MessageReceiverRuntimeException(String msg, Throwable e) {
		super(msg, e);
	}

}
