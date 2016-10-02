package net.hycube.leave;

import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.environment.NodeProperties;

public interface LeaveManager {

	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException;
	
	public void leave();

	public void discard();
	
	
}
