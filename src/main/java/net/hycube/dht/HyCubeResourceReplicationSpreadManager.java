package net.hycube.dht;

import java.math.BigInteger;

import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodeId;
import net.hycube.environment.NodeProperties;

public interface HyCubeResourceReplicationSpreadManager {

	
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException;
	
	
	public void putToStorageProcessed(BigInteger key, NodeId senderNodeId, HyCubeResource r, long refreshTime, boolean replication, Object[] parameters, boolean result);

	public void refreshPutToStorageProcessed(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor rd, long refreshTime, boolean replication, Object[] parameters, boolean result);
	
	public void getFromStorageProcessed(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor criteria, Object[] parameters, HyCubeResourceEntry[] result);
	
	public void deleteFromProcessed(BigInteger key, NodeId sender, HyCubeResourceDescriptor criteria, Object[] parameters, boolean result);
	
	
	public double getReplicationNodesNumSpreadForResource(BigInteger key, HyCubeResourceDescriptor resourceDescriptor, long refreshTime);
	
	public int getReplicationNodesNumForResource(int deafaultReplicationNodesNum, BigInteger key, HyCubeResourceDescriptor resourceDescriptor, long refreshTime);
	
	
	
}
