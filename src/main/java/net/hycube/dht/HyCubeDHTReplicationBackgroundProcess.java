package net.hycube.dht;

import net.hycube.backgroundprocessing.AbstractBackgroundProcess;
import net.hycube.backgroundprocessing.BackgroundProcessException;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.environment.NodeProperties;
import net.hycube.logging.LogHelper;

public class HyCubeDHTReplicationBackgroundProcess extends AbstractBackgroundProcess {

	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeDHTReplicationBackgroundProcess.class); 
	
	

	protected HyCubeDHTManager dhtManager;
	
	
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		if (userLog.isDebugEnabled()) {
			userLog.debug("Initializing DHT replication backround process...");
		}
		if (devLog.isDebugEnabled()) {
			devLog.debug("Initializing DHT replication backround process...");
		}

		
		super.initialize(nodeAccessor, properties);

		
		try {
			this.dhtManager = (HyCubeDHTManager) nodeAccessor.getDHTManager();
			if (this.dhtManager == null) throw new InitializationException(InitializationException.Error.MISSING_EXTENSION_ERROR, null, "The DHTManager is not set.");
		} catch (ClassCastException e) {
			throw new InitializationException(InitializationException.Error.MISSING_EXTENSION_ERROR, null, "The DHTManager is expected to be an instance of: " + HyCubeDHTManager.class.getName());
		}
		
				
	}

	@Override
	public void doProcess() {
		
		processReplicate();
		
	}

	
	
	protected void processReplicate() {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("processReplicate called");
		}
		
		try {
			dhtManager.replicate();
		} catch (Exception e) {
			throw new BackgroundProcessException("An exception thrown processing replication.", e);
		}
		
		
	}


}
