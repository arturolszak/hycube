package net.hycube.lookup;

import net.hycube.common.EntryPoint;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodeId;
import net.hycube.environment.NodeProperties;
import net.hycube.eventprocessing.EventType;

public interface LookupManager {

	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException;
	
	public LookupCallback lookup(NodeId lookupNodeId, LookupCallback lookupCallback, Object callbackArg);
	
	public LookupCallback lookup(NodeId lookupNodeId, LookupCallback lookupCallback, Object callbackArg, Object[] parameters);
	
	public EventType getLookupCallbackEventType();
	
	public EventType getLookupRequestTimeoutEventType();
	

	
	
	public EntryPoint getEntryPoint();

	public void discard();
	
	
}