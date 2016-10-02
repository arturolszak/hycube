package net.hycube.environment;


public class NodePropertiesRuntimeException extends RuntimeException {
	
	private static final long serialVersionUID = -6290335875651368731L;

	public NodePropertiesRuntimeException() {
		super();
	}

	public NodePropertiesRuntimeException(String msg, Throwable e) {
		super(msg, e);
	}
	
	public NodePropertiesRuntimeException(String msg) {
		super(msg);
	}

	public NodePropertiesRuntimeException(Throwable e) {
		super(e);
	}
	
}
