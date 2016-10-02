package net.hycube.join;

import net.hycube.common.EntryPoint;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.environment.NodeProperties;
import net.hycube.eventprocessing.EventType;

public interface JoinManager {

	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException;
	
	public JoinCallback join(String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg);
	
	public JoinCallback join(String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, Object[] joinParameters);
	

	public EventType getJoinCallbackEventType();
	

	public EntryPoint getEntryPoint();

	public void discard();
	
	
}