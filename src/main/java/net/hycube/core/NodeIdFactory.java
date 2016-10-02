package net.hycube.core;

import java.math.BigInteger;
import java.nio.ByteOrder;

import net.hycube.environment.NodeProperties;

public interface NodeIdFactory {
	
	public void initialize(NodeProperties nodeProperties) throws InitializationException;
	
	public NodeId generateRandomNodeId();
	public NodeId fromBytes(byte[] byteArray, ByteOrder byteOrder) throws NodeIdByteConversionException;
	public NodeId parseNodeId(String input);
	public NodeId fromBigInteger(BigInteger input);
	public Class<?> getNodeIdType();

	public void discard();
	
}
