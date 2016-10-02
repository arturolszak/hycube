package net.hycube.dht;

import java.math.BigInteger;

import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodeId;
import net.hycube.environment.NodeProperties;

public interface DHTStorageManager {

	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException;
	
	
	public Object putToStorage(BigInteger key, NodeId sender, Object value);
	public Object refreshPutToStorage(BigInteger key, NodeId senderNodeId, Object value);	
	public Object[] getFromStorage(BigInteger key, NodeId senderNodeId, Object detail);
	public Object deleteFromStorage(BigInteger key, NodeId senderNodeId, Object detail);
	
	public Object putToStorage(BigInteger key, NodeId sender, Object value, Object[] parameters);
	public Object refreshPutToStorage(BigInteger key, NodeId senderNodeId, Object value, Object[] parameters);	
	public Object[] getFromStorage(BigInteger key, NodeId senderNodeId, Object detail, Object[] parameters);
	public Object deleteFromStorage(BigInteger key, NodeId senderNodeId, Object detail, Object[] parameters);
	
	public void discardOutdatedEntries(long discardTime);
	
	
}
