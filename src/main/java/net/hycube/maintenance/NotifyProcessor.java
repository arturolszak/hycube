package net.hycube.maintenance;

import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodePointer;
import net.hycube.environment.NodeProperties;

public abstract class NotifyProcessor {
	
	
	protected boolean initialized = false;
	
	protected boolean isInitialized() {
		return initialized;
	}
	
	
	protected NodeAccessor nodeAccessor;
	protected NodeProperties properties;
	

	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		this.nodeAccessor = nodeAccessor;
		this.properties = properties;
		
		this.initialized = true;

	}
	
	public abstract void processNotify(NodePointer newNode, long currTimestamp);

	public void discard() {
		
		
	}

	

	
	
}
