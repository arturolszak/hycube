package net.hycube.common;


public interface EntryPoint {

	public Object call();
	
	public Object call(Object arg);
	
	public Object call(Object[] args);
	
	public Object call(Object entryPoint, Object[] args);
	
	
}
