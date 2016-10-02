package net.hycube.environment;


public class NodePropertiesInitializationException extends Exception {
	private static final long serialVersionUID = -1307896983366812776L;
	
	protected InitializationError error;
	
	public InitializationError getError() { return error; }
	
	public NodePropertiesInitializationException(InitializationError error) {
		super();
		this.error = error;
	}
	public NodePropertiesInitializationException(InitializationError error, String msg, Throwable e) {
		super(msg, e);
		this.error = error;
	}
	public NodePropertiesInitializationException(InitializationError error, String msg) {
		super(msg);
		this.error = error;
	}
	public NodePropertiesInitializationException(InitializationError error, Throwable e) {
		super(e);
		this.error = error;
	}
	
	public static enum InitializationError {
		DEFAULT_PROPERTY_FILE_READ_ERROR,
		APPLICATION_PROPERTY_FILE_READ_ERROR,
	}
}
