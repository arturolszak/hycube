package net.hycube.dht;

import net.hycube.backgroundprocessing.AbstractBackgroundProcess;
import net.hycube.backgroundprocessing.BackgroundProcessException;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.environment.NodeProperties;
import net.hycube.logging.LogHelper;

public class HyCubeDHTBackgroundProcess extends AbstractBackgroundProcess {

	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeDHTBackgroundProcess.class); 
	
	

	protected HyCubeDHTManager dhtManager;
	
	
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		if (userLog.isDebugEnabled()) {
			userLog.debug("Initializing backround processing of DHT...");
		}
		if (devLog.isDebugEnabled()) {
			devLog.debug("Initializing backround processing of DHT...");
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
		
		processDHT();
		
	}

	
	
	protected void processDHT() {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("processDHT called");
		}
		
		try {
			dhtManager.processDHT();
		} catch (Exception e) {
			throw new BackgroundProcessException("An exception thrown while processing DHT.", e);
		}
    	
		
	}


}
