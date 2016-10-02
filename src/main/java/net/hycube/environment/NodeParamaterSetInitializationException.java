package net.hycube.environment;

public class NodeParamaterSetInitializationException extends Exception {
	private static final long serialVersionUID = 5968206539980037577L;
	
	protected String property;
	
	public String getProperty() { return property; }
	
	public NodeParamaterSetInitializationException(String property) {
		super();
		this.property = property;
	}
	public NodeParamaterSetInitializationException(String property, String msg, Throwable e) {
		super(msg, e);
		this.property = property;
	}
	public NodeParamaterSetInitializationException(String property, String msg) {
		super(msg);
		this.property = property;
	}
	public NodeParamaterSetInitializationException(String property, Throwable e) {
		super(e);
		this.property = property;
	}
}