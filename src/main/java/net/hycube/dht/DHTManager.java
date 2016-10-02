package net.hycube.dht;

import java.math.BigInteger;

import net.hycube.common.EntryPoint;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodeId;
import net.hycube.core.NodePointer;
import net.hycube.environment.NodeProperties;
import net.hycube.eventprocessing.EventType;

public interface DHTManager {

	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException;
	
	
	
	public PutCallback put(NodePointer np, BigInteger key, Object value, PutCallback putCallback, Object putCallbackArg);
	public RefreshPutCallback refreshPut(NodePointer np, BigInteger key, Object value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg);
	public GetCallback get(NodePointer np, BigInteger key, Object detail, GetCallback getCallback, Object getCallbackArg);
	
	public PutCallback put(BigInteger key, Object value, PutCallback putCallback, Object putCallbackArg);
	public RefreshPutCallback refreshPut(BigInteger key, Object value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg);
	public GetCallback get(BigInteger key, Object detail, GetCallback getCallback, Object getCallbackArg);

	public DeleteCallback delete(NodePointer np, BigInteger key, Object detail, DeleteCallback deleteCallback, Object deleteCallbackArg);
	
	public Object putToStorage(BigInteger key, NodeId sender, Object value);
	public Object refreshPutToStorage(BigInteger key, NodeId senderNodeId, Object value);
	public Object[] getFromStorage(BigInteger key, NodeId senderNodeId, Object detail);
	public Object deleteFromStorage(BigInteger key, NodeId sender, Object detail);
	
	
	public PutCallback put(NodePointer np, BigInteger key, Object value, PutCallback putCallback, Object putCallbackArg, Object[] parameters);
	public RefreshPutCallback refreshPut(NodePointer np, BigInteger key, Object value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg, Object[] parameters);
	public GetCallback get(NodePointer np, BigInteger key, Object detail, GetCallback getCallback, Object getCallbackArg, Object[] parameters);
	
	public PutCallback put(BigInteger key, Object value, PutCallback putCallback, Object putCallbackArg, Object[] parameters);
	public RefreshPutCallback refreshPut(BigInteger key, Object value, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg, Object[] parameters);
	public GetCallback get(BigInteger key, Object detail, GetCallback getCallback, Object getCallbackArg, Object[] parameters);

	public DeleteCallback delete(NodePointer np, BigInteger key, Object detail, DeleteCallback deleteCallback, Object deleteCallbackArg, Object[] parameters);
	
	public Object putToStorage(BigInteger key, NodeId sender, Object value, Object[] parameters);
	public Object refreshPutToStorage(BigInteger key, NodeId senderNodeId, Object value, Object[] parameters);
	public Object[] getFromStorage(BigInteger key, NodeId senderNodeId, Object detail, Object[] parameters);
	public Object deleteFromStorage(BigInteger key, NodeId sender, Object detail, Object[] parameters);
	
	
	public void discardOutdatedEntries(long discardTime);
	
	
	
	public EventType getPutCallbackEventType();
	public EventType getGetCallbackEventType();
	public EventType getDeleteCallbackEventType();
	public EventType getRefreshPutCallbackEventType();
	
	
	public EntryPoint getEntryPoint();


	public void discard();

	
	
}
