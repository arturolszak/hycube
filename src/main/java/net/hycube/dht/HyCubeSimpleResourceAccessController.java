package net.hycube.dht;

import net.hycube.core.HyCubeNodeId;
import net.hycube.core.NodeAccessor;
import net.hycube.environment.NodeProperties;

public class HyCubeSimpleResourceAccessController implements HyCubeResourceAccessController {

	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) {
		
	}
	
	
	@Override
	public boolean checkPutAccess(HyCubeNodeId nodeId, HyCubeResourceDescriptor rd) {
		return checkPutAccess(nodeId, rd, false);
	}
	
	@Override
	public boolean checkPutAccess(HyCubeNodeId nodeId, HyCubeResourceDescriptor rd, boolean replicated) {
		return true;
	}

	@Override
	public boolean checkRefreshPutAccess(HyCubeNodeId nodeId, HyCubeResourceDescriptor rd) {
		return checkRefreshPutAccess(nodeId, rd, false);
	}
	
	@Override
	public boolean checkRefreshPutAccess(HyCubeNodeId nodeId, HyCubeResourceDescriptor rd, boolean replicated) {
		return true;
	}

	@Override
	public boolean checkGetAccess(HyCubeNodeId nodeId, HyCubeResourceDescriptor rd) {
		return true;
	}

	@Override
	public boolean checkDeleteAccess(HyCubeNodeId nodeId, HyCubeResourceDescriptor rd) {
		return true;
	}

	
	
	
	
	
	@Override
	public boolean checkPutAccess(HyCubeNodeId nodeId, HyCubeResourceDescriptor rd, Object[] parameters) {
		return checkPutAccess(nodeId, rd, false);
	}
	
	@Override
	public boolean checkPutAccess(HyCubeNodeId nodeId, HyCubeResourceDescriptor rd, boolean replicated, Object[] parameters) {
		return true;
	}

	@Override
	public boolean checkRefreshPutAccess(HyCubeNodeId nodeId, HyCubeResourceDescriptor rd, Object[] parameters) {
		return checkRefreshPutAccess(nodeId, rd, false);
	}
	
	@Override
	public boolean checkRefreshPutAccess(HyCubeNodeId nodeId, HyCubeResourceDescriptor rd, boolean replicated, Object[] parameters) {
		return true;
	}

	@Override
	public boolean checkGetAccess(HyCubeNodeId nodeId, HyCubeResourceDescriptor rd, Object[] parameters) {
		return true;
	}

	@Override
	public boolean checkDeleteAccess(HyCubeNodeId nodeId, HyCubeResourceDescriptor rd, Object[] parameters) {
		return true;
	}

	
	
	

}
