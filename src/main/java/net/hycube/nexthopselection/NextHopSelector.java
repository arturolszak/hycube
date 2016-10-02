package net.hycube.nexthopselection;

import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodeId;
import net.hycube.core.NodePointer;
import net.hycube.environment.NodeProperties;

public abstract class NextHopSelector {

	protected boolean initialized = false;
	
	protected NodeAccessor nodeAccessor;
	protected NodeProperties properties;
	


	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		this.nodeAccessor = nodeAccessor;
		this.properties = properties;
		this.initialized = true;

	}

	
	
//	public abstract NodePointer findNextHop(Message msg);
	
	public abstract NodePointer findNextHop(NodeId recipientId, NextHopSelectionParameters parameters);
	
	public abstract NodePointer[] findNextHops(NodeId recipientId, NextHopSelectionParameters parameters, int numNextHops);



	public void discard() {
		
	}

	
	
	
	
}
