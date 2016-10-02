package net.hycube.transport;

public class NetworkAdapterRuntimeException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2529826495511984072L;


	public NetworkAdapterRuntimeException() {

	}

	public NetworkAdapterRuntimeException(String msg) {
		super(msg);
	}

	public NetworkAdapterRuntimeException(Throwable e) {
		super(e);
	}

	public NetworkAdapterRuntimeException(String msg, Throwable e) {
		super(msg, e);
	}

}
