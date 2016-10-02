package net.hycube.search;

import net.hycube.common.EntryPoint;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodeId;
import net.hycube.core.NodePointer;
import net.hycube.environment.NodeProperties;
import net.hycube.eventprocessing.EventType;

public interface SearchManager {

	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException;
	
	public SearchCallback search(NodeId seachNodeId, short k, SearchCallback searchCallback, Object callbackArg);
	
	public SearchCallback search(NodeId seachNodeId, NodePointer[] initialNodes, short k, SearchCallback searchCallback, Object callbackArg);
	
	public SearchCallback search(NodeId seachNodeId, short k, boolean ignoreTargetNode, SearchCallback searchCallback, Object callbackArg);
	
	public SearchCallback search(NodeId seachNodeId, NodePointer[] initialNodes, short k, boolean ignoreTargetNode, SearchCallback searchCallback, Object callbackArgg);
	
	public SearchCallback search(NodeId seachNodeId, short k, SearchCallback searchCallback, Object callbackArg, Object[]  parameters);
	
	public SearchCallback search(NodeId seachNodeId, NodePointer[] initialNodes, short k, SearchCallback searchCallback, Object callbackArg, Object[]  parameters);
	
	public SearchCallback search(NodeId seachNodeId, short k, boolean ignoreTargetNode, SearchCallback searchCallback, Object callbackArg, Object[]  parameters);
	
	public SearchCallback search(NodeId seachNodeId, NodePointer[] initialNodes, short k, boolean ignoreTargetNode, SearchCallback searchCallback, Object callbackArg, Object[]  parameters);
	
	public EventType getSearchCallbackEventType();
	
	public EventType getSearchRequestTimeoutEventType();

	

	public EntryPoint getEntryPoint();

	public void discard();
	
	
	
}