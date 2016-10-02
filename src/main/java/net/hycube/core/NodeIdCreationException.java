/**
 * 
 */
package net.hycube.core;

/**
 * @author Artur Olszak
 *
 * Exception thrown when an error occurs during perfirmin operations on NodeID objects
 */
public class NodeIdCreationException extends RuntimeException {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 3411985153856638818L;

	/**
	 * 
	 */
	public NodeIdCreationException() {
		super();
	}

	/**
	 * @param arg0
	 */
	public NodeIdCreationException(String arg0) {
		super(arg0);
	}

	/**
	 * @param arg0
	 */
	public NodeIdCreationException(Throwable arg0) {
		super(arg0);
	}

	/**
	 * @param arg0
	 * @param arg1
	 */
	public NodeIdCreationException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

}
