package net.hycube.messaging.ack;

import java.util.HashMap;

import net.hycube.common.EntryPoint;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.environment.NodeProperties;
import net.hycube.extensions.Extension;

public class HyCubeAckExtension implements Extension {

	
	protected static final String PROP_KEY_ACK_MANAGER = "AckManager";
	
	
	protected NodeAccessor nodeAccessor;
    
    
    protected HyCubeAckManager ackManager;  
    
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		this.nodeAccessor = nodeAccessor;
		
		NodeProperties ackManagerProperties = properties.getNestedProperty(PROP_KEY_ACK_MANAGER);
		
		
		this.ackManager = new HyCubeAckManager();
		this.ackManager.initialize(nodeAccessor, ackManagerProperties);
		
		
	}
	
	@Override
	public void postInitialize() {
		
	}

	
	@Override
	public EntryPoint getExtensionEntryPoint() {
		return null;
	}

	
	
	
	public HashMap<Integer, AckProcessInfo> getAckAwaitingMap() {
		return ackManager.getAckAwaitingMap();
	}

	
	public Object getAckAwaitingLock() {
		return ackManager.getAckAwaitingLock();
	}
	
	
	public HyCubeAckManager getAckManager() {
		return ackManager;
	}
	

	@Override
	public void discard() {
		
	}








	
}
