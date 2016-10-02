package net.hycube.core;

import java.util.List;

import net.hycube.environment.NodeProperties;

public interface RoutingTable {

	public void initialize(NodeProperties properties) throws InitializationException;
	
	public List<RoutingTableEntry> getAllRoutingTableEntries();
	
	public List<RoutingTableEntry> getRoutingTableEntriesByNodeIdHash(long nodeIdHash);

	
	public void discard();
	
	
}
