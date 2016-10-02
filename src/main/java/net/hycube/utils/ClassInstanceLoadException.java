package net.hycube.utils;

public class ClassInstanceLoadException extends Exception {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1752511491906455924L;

	
	protected String className;

	
	/**
	 * 
	 */
	public ClassInstanceLoadException(String className) {
		super();
		this.className = className;
	}

	/**
	 * @param arg0
	 */
	public ClassInstanceLoadException(String arg0, String className) {
		super(arg0);
		this.className = className;
	}

	/**
	 * @param arg0
	 */
	public ClassInstanceLoadException(Throwable arg0, String className) {
		super(arg0);
		this.className = className;
	}

	/**
	 * @param arg0
	 * @param arg1
	 */
	public ClassInstanceLoadException(String arg0, Throwable arg1, String className) {
		super(arg0, arg1);
		this.className = className;
	}


	
	/**
	 * 
	 */
	public ClassInstanceLoadException(Class<?> clazz) {
		super();
		this.className = clazz.getName();
	}

	/**
	 * @param arg0
	 */
	public ClassInstanceLoadException(String arg0, Class<?> clazz) {
		super(arg0);
		this.className = clazz.getName();
	}

	/**
	 * @param arg0
	 */
	public ClassInstanceLoadException(Throwable arg0, Class<?> clazz) {
		super(arg0);
		this.className = clazz.getName();
	}

	/**
	 * @param arg0
	 * @param arg1
	 */
	public ClassInstanceLoadException(String arg0, Throwable arg1, Class<?> clazz) {
		super(arg0, arg1);
		this.className = clazz.getName();
	}
	
	
	
	public String getLoadedClassName() {
		return className;
	}
	
}
