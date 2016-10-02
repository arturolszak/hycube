package net.hycube.core;


public class InitializationException extends Exception {
	private static final long serialVersionUID = 3885606886594971633L;
	
	protected Error error;
	protected Object[] details;
	
	public Error getError() { return error; }
	public Object[] getDetails() { return details; }
	
	public InitializationException(Error error, Object[] details) {
		super();
		this.error = error;
		this.details = details;
	}
	public InitializationException(Error error, Object[] details, String msg, Throwable e) {
		super(msg, e);
		this.error = error;
		this.details = details;
	}
	public InitializationException(Error error, Object[] details, String msg) {
		super(msg);
		this.error = error;
		this.details = details;
	}
	public InitializationException(Error error, Object[] details, Throwable e) {
		super(e);
		this.error = error;
		this.details = details;
	}

	public InitializationException(String msg) {
		super(msg);
	}
	public InitializationException(String msg, Throwable e) {
		super(msg, e);
	}
	public InitializationException(String msg, Object[] details) {
		super(msg);
		this.details = details;
	}
	public InitializationException(String msg, Object[] details, Throwable e) {
		super(msg, e);
		this.details = details;
	}
	
	public InitializationException(String msg, Object details) {
		super(msg);
		this.details = new Object[]{details};
	}
	public InitializationException(String msg, Object details, Throwable e) {
		super(msg, e);
		this.details = new Object[]{details};
	}

	
	public InitializationException(Error error, Object detail) {
		this(error, new Object[] {detail});
	}
	public InitializationException(Error error, Object detail, String msg, Throwable e) {
		this(error, new Object[] {detail}, msg, e);
	}
	public InitializationException(Error error, Object detail, String msg) {
		this(error, new Object[] {detail}, msg);
	}
	public InitializationException(Error error, Object detail, Throwable e) {
		this(error, new Object[] {detail}, e);
	}
	
	public static enum Error {
		PARAMETERS_READ_ERROR,
		INVALID_PARAMETER_VALUE,
		MISSING_PARAMETER_VALUE,
		CLASS_INSTANTIATION_ERROR,
		NETWORK_ADAPTER_INITIALIZATION_ERROR,
		MESSAGE_RECEIVER_INITIALIZATION_ERROR,
		EVENT_PROCESSOR_INITIALIZATION_ERROR,
		NODE_INITIALIZATION_ERROR,
		NODE_SERVICE_INITIALIZATION_ERROR,
		MISSING_EXTENSION_ERROR,
	}
	
}
