package net.hycube.dht;

import java.math.BigInteger;

import net.hycube.common.EntryPoint;
import net.hycube.core.NodeId;
import net.hycube.core.NodePointer;

public interface HyCubeDHTManagerEntryPoint extends EntryPoint {

	public boolean putToStorage(BigInteger key, NodeId senderNodeId, HyCubeResource r, long refreshTime, Object[] parameters);
	public boolean putToStorage(BigInteger key, NodeId senderNodeId, HyCubeResource r, long refreshTime, boolean replication, Object[] parameters);
	public boolean refreshPutToStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor rd, long refreshTime, Object[] parameters);
	public boolean refreshPutToStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor rd, long refreshTime, boolean replication, Object[] parameters);
	public HyCubeResourceEntry[] getFromStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor criteria, Object[] parameters);
	public boolean deleteFromStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor criteria, Object[] parameters);
	
	public PutCallback put(NodePointer recipient, BigInteger key, HyCubeResource resource, PutCallback putCallback, Object putCallbackArg, Object[] parameters);
	public RefreshPutCallback refreshPut(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor resourceDescriptor, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg, Object[] parameters);
	public GetCallback get(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor criteria, GetCallback getCallback, Object getCallbackArg, Object[] parameters);
	
	public PutCallback put(BigInteger key, HyCubeResource resource, PutCallback putCallback, Object putCallbackArg, Object[] parameters);
	public RefreshPutCallback refreshPut(BigInteger key, HyCubeResourceDescriptor resourceDescriptor, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg, Object[] parameters);
	public GetCallback get(BigInteger key, HyCubeResourceDescriptor criteria, GetCallback getCallback, Object getCallbackArg, Object[] parameters);
	
	public DeleteCallback delete(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor criteria, DeleteCallback deleteCallback, Object deleteCallbackArg, Object[] parameters);
	
	public boolean isReplica(BigInteger key, NodeId nodeId, int k);
	
	
	
	
	
}
