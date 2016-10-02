package net.hycube.environment;


public class NodePropertiesException extends Exception {

	private static final long serialVersionUID = -951325947704849737L;

	public NodePropertiesException() {
		super();
	}

	public NodePropertiesException(String msg, Throwable e) {
		super(msg, e);
	}
	
	public NodePropertiesException(String msg) {
		super(msg);
	}

	public NodePropertiesException(Throwable e) {
		super(e);
	}
	
}
