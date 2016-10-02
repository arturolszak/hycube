package net.hycube.dht;

import net.hycube.core.HyCubeNodeId;
import net.hycube.core.NodeAccessor;
import net.hycube.environment.NodeProperties;

public interface HyCubeResourceAccessController {

	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties);
	
	
	public boolean checkPutAccess(HyCubeNodeId nodeId, HyCubeResourceDescriptor rd);
	
	public boolean checkPutAccess(HyCubeNodeId nodeId, HyCubeResourceDescriptor rd, boolean replicated);
	
	public boolean checkRefreshPutAccess(HyCubeNodeId nodeId, HyCubeResourceDescriptor rd);
	
	public boolean checkRefreshPutAccess(HyCubeNodeId nodeId, HyCubeResourceDescriptor rd, boolean replicated);
	
	public boolean checkGetAccess(HyCubeNodeId nodeId, HyCubeResourceDescriptor rd);
	
	public boolean checkDeleteAccess(HyCubeNodeId nodeId, HyCubeResourceDescriptor rd);
	
	
	
	public boolean checkPutAccess(HyCubeNodeId nodeId, HyCubeResourceDescriptor rd, Object[] parameters);
	
	public boolean checkPutAccess(HyCubeNodeId nodeId, HyCubeResourceDescriptor rd, boolean replicated, Object[] parameters);
	
	public boolean checkRefreshPutAccess(HyCubeNodeId nodeId, HyCubeResourceDescriptor rd, Object[] parameters);
	
	public boolean checkRefreshPutAccess(HyCubeNodeId nodeId, HyCubeResourceDescriptor rd, boolean replicated, Object[] parameters);
	
	public boolean checkGetAccess(HyCubeNodeId nodeId, HyCubeResourceDescriptor rd, Object[] parameters);
	
	public boolean checkDeleteAccess(HyCubeNodeId nodeId, HyCubeResourceDescriptor rd, Object[] parameters);
	
	
	
	
	
}
