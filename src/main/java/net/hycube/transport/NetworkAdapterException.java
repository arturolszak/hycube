package net.hycube.transport;

public class NetworkAdapterException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5656889114988608539L;

	public NetworkAdapterException() {

	}

	public NetworkAdapterException(String msg) {
		super(msg);
	}

	public NetworkAdapterException(Throwable e) {
		super(e);
	}

	public NetworkAdapterException(String msg, Throwable e) {
		super(msg, e);
	}

}
