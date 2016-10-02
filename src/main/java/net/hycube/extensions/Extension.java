package net.hycube.extensions;

import net.hycube.common.EntryPoint;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.environment.NodeProperties;

public interface Extension {

	//Extension.initialize() method expects only the node accessor instance to be a valid reference to the node accessor, but should consider the node object as not yet initialized
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException;
	
	//Extension.postInitialize() will be called as a last step of the node initialization (after all other node components are initialized)
	public void postInitialize() throws InitializationException;
	
	
	/**
	 * This method returns an entry point to the Extension object for external calls. It may as well return null if the extension does not have an entry point (cannot be called from outside).
	 * @return
	 */
	public EntryPoint getExtensionEntryPoint();
	
	
	public void discard();
	
}
