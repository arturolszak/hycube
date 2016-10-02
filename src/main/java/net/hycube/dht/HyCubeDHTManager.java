package net.hycube.dht;

import java.math.BigInteger;

import net.hycube.core.NodeId;
import net.hycube.core.NodePointer;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.processing.ProcessMessageException;

public interface HyCubeDHTManager extends DHTManager {

	public boolean putToStorage(BigInteger key, NodeId senderNodeId, HyCubeResource r, long refreshTime);
	public boolean putToStorage(BigInteger key, NodeId senderNodeId, HyCubeResource r, long refreshTime, boolean replication);
	public boolean refreshPutToStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor rd, long refreshTime);
	public boolean refreshPutToStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor rd, long refreshTime, boolean replication);
	public HyCubeResourceEntry[] getFromStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor criteria);
	public boolean deleteFromStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor criteria);
	
	public PutCallback put(NodePointer recipient, BigInteger key, HyCubeResource resource, PutCallback putCallback, Object putCallbackArg);
	public RefreshPutCallback refreshPut(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor resourceDescriptor, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg);
	public GetCallback get(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor criteria, GetCallback getCallback, Object getCallbackArg);
	
	public PutCallback put(BigInteger key, HyCubeResource resource, PutCallback putCallback, Object putCallbackArg);
	public RefreshPutCallback refreshPut(BigInteger key, HyCubeResourceDescriptor resourceDescriptor, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg);
	public GetCallback get(BigInteger key, HyCubeResourceDescriptor criteria, GetCallback getCallback, Object getCallbackArg);
	
	public DeleteCallback delete(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor criteria, DeleteCallback deleteCallback, Object deleteCallbackArg);
	
	
	public boolean putToStorage(BigInteger key, NodeId senderNodeId, HyCubeResource r, long refreshTime, Object[] parameters);
	public boolean putToStorage(BigInteger key, NodeId senderNodeId, HyCubeResource r, long refreshTime, boolean replication, Object[] parameters);
	public boolean refreshPutToStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor rd, long refreshTime, Object[] parameters);
	public boolean refreshPutToStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor rd, long refreshTime, boolean replication, Object[] parameters);
	public HyCubeResourceEntry[] getFromStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor criteria, Object[] parameters);
	public boolean deleteFromStorage(BigInteger key, NodeId sender, HyCubeResourceDescriptor criteria, Object[] parameters);
	
	public PutCallback put(NodePointer recipient, BigInteger key, HyCubeResource resource, PutCallback putCallback, Object putCallbackArg, Object[] parameters);
	public RefreshPutCallback refreshPut(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor resourceDescriptor, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg, Object[] parameters);
	public GetCallback get(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor criteria, GetCallback getCallback, Object getCallbackArg, Object[] parameters);
	
	public PutCallback put(BigInteger key, HyCubeResource resource, PutCallback putCallback, Object putCallbackArg, Object[] parameters);
	public RefreshPutCallback refreshPut(BigInteger key, HyCubeResourceDescriptor resourceDescriptor, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg, Object[] parameters);
	public GetCallback get(BigInteger key, HyCubeResourceDescriptor criteria, GetCallback getCallback, Object getCallbackArg, Object[] parameters);
	
	public DeleteCallback delete(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor criteria, DeleteCallback deleteCallback, Object deleteCallbackArg, Object[] parameters);
	
	
	public void processPutRequest(NodePointer sender, HyCubeMessage msg, int commandId, BigInteger key, String resourceDescriptorString, byte[] resourceData, long refreshTime) throws ProcessMessageException;
	public void processPutResponse(NodePointer sender, HyCubeMessage msg, int commandId, boolean putStatus) throws ProcessMessageException;

	public void processRefreshPutRequest(NodePointer sender, HyCubeMessage msg, int commandId, BigInteger key, String resourceDescriptorString, long refreshTime) throws ProcessMessageException;
	public void processRefreshPutResponse(NodePointer sender, HyCubeMessage msg, int commandId, boolean refreshStatus) throws ProcessMessageException;
	
	public void processGetRequest(NodePointer sender, HyCubeMessage msg, int commandId, BigInteger key, String criteriaString, boolean getFromClosestNode) throws ProcessMessageException;
	public void processGetResponse(NodePointer sender, HyCubeMessage msg, int commandId, String[] resourceDescriptorStrings, byte[][] resourcesData) throws ProcessMessageException;

	public void processDeleteRequest(NodePointer sender, HyCubeMessage msg, int commandId, BigInteger key, String resourceDescriptorString) throws ProcessMessageException;
	public void processDeleteResponse(NodePointer sender, HyCubeMessage msg, int commandId, boolean deleteStatus) throws ProcessMessageException;
	
	public void processReplicateMessage(NodePointer sender, HyCubeMessage msg, int resourcesNum, BigInteger[] keys, String[] resourceDescriptorStrings, long[] refreshTimes, int[] replicationSpreadNodesNums) throws ProcessMessageException;
	
	
	public void processDHT();
	

	public void replicate();
	
	
	public HyCubeResourceAccessController getResourceAccessController();
	
	
}
