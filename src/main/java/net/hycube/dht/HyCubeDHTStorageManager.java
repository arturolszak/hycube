package net.hycube.dht;

import java.math.BigInteger;
import java.util.Map;

import net.hycube.core.NodeId;

public interface HyCubeDHTStorageManager extends DHTStorageManager {

	public boolean putToStorage(BigInteger key, NodeId senderNodeId, HyCubeResource r, long refreshTime);
	public boolean putToStorage(BigInteger key, NodeId senderNodeId, HyCubeResource r, long refreshTime, boolean replication);
	public boolean refreshPutToStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor rd, long refreshTime);
	public boolean refreshPutToStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor rd, long refreshTime, boolean replication);
	public HyCubeResourceEntry[] getFromStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor criteria);
	public boolean deleteFromStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor criteria);
	
	public boolean putToStorage(BigInteger key, NodeId senderNodeId, HyCubeResource r, long refreshTime, Object[] parameters);
	public boolean putToStorage(BigInteger key, NodeId senderNodeId, HyCubeResource r, long refreshTime, boolean replication, Object[] parameters);
	public boolean refreshPutToStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor rd, long refreshTime, Object[] parameters);
	public boolean refreshPutToStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor rd, long refreshTime, boolean replication, Object[] parameters);
	public HyCubeResourceEntry[] getFromStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor criteria, Object[] parameters);
	public boolean deleteFromStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor criteria, Object[] parameters);
	
	public Map<BigInteger, HyCubeResourceReplicationEntry[]> getResourcesInfoForReplication();
	
}
