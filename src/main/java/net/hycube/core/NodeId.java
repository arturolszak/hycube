package net.hycube.core;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteOrder;

public abstract class NodeId implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7139755191118734962L;
	

	public abstract boolean[] getId();
	public abstract void setId(boolean[] id);
	public abstract String toString();
	public abstract String toHexString();
	public abstract boolean get(int index);
	public abstract NodeId copy();
	public abstract long calculateHash();
	public abstract double getNum();
	public abstract byte[] getBytes(ByteOrder byteOrder);
	public abstract int getByteLength();
    public abstract BigInteger getBigInteger();
	
	public abstract int hashCode();
	public abstract boolean equals(Object id);
	public abstract boolean equals(NodeId id);
	
	public abstract boolean compareTo(NodeId nodeId);
	
	public static boolean compareIds(NodeId id1, NodeId id2) {
		return id1.compareTo(id2);
	}

	
}
