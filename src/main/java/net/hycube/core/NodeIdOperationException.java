/**
 * 
 */
package net.hycube.core;

/**
 * @author Artur Olszak
 *
 * Exception thrown when an error occurs during perfirmin operations on NodeID objects
 */
public class NodeIdOperationException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2113772164238728313L;

	/**
	 * 
	 */
	public NodeIdOperationException() {
		super();
	}

	/**
	 * @param arg0
	 */
	public NodeIdOperationException(String arg0) {
		super(arg0);
	}

	/**
	 * @param arg0
	 */
	public NodeIdOperationException(Throwable arg0) {
		super(arg0);
	}

	/**
	 * @param arg0
	 * @param arg1
	 */
	public NodeIdOperationException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

}
