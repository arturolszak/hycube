package net.hycube.dht;

import java.math.BigInteger;

import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodeId;
import net.hycube.environment.NodeProperties;

public class HyCubeSimpleResourceReplicationSpreadManager implements HyCubeResourceReplicationSpreadManager {

	
	public static final double SPREAD_FACTOR = 1;
	
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
	}
	
	
	
	@Override
	public void putToStorageProcessed(BigInteger key, NodeId senderNodeId,
			HyCubeResource r, long refreshTime, boolean replication,
			Object[] parameters, boolean result) {

	}

	@Override
	public void refreshPutToStorageProcessed(BigInteger key,
			NodeId senderNodeId, HyCubeResourceDescriptor rd, long refreshTime,
			boolean replication, Object[] parameters, boolean result) {

	}
	
	@Override
	public void getFromStorageProcessed(BigInteger key, NodeId senderNodeId,
			HyCubeResourceDescriptor criteria, Object[] parameters,
			HyCubeResourceEntry[] result) {

	}

	@Override
	public void deleteFromProcessed(BigInteger key, NodeId sender,
			HyCubeResourceDescriptor criteria, Object[] parameters,
			boolean result) {

	}


	
	

	@Override
	public double getReplicationNodesNumSpreadForResource(BigInteger key,
			HyCubeResourceDescriptor resourceDescriptor, long refreshTime) {
		
		return SPREAD_FACTOR;
		
	}



	@Override
	public int getReplicationNodesNumForResource(
			int deafaultReplicationNodesNum, BigInteger key,
			HyCubeResourceDescriptor resourceDescriptor, long refreshTime) {

		return (int) Math.round(SPREAD_FACTOR * ((double)deafaultReplicationNodesNum));
		
	}

	



}
