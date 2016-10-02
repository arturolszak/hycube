package net.hycube.messaging.ack;

import net.hycube.backgroundprocessing.AbstractBackgroundProcess;
import net.hycube.backgroundprocessing.BackgroundProcessException;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.environment.NodeProperties;
import net.hycube.logging.LogHelper;

public class HyCubeAwaitingAcksBackgroundProcess extends AbstractBackgroundProcess {

	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeAwaitingAcksBackgroundProcess.class); 
	
	
	protected static final String PROP_KEY_ACK_EXTENSION_KEY = "AckExtensionKey";
	
	protected String ackExtensionKey;	
	protected HyCubeAckExtension ackExtension;

	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		if (userLog.isDebugEnabled()) {
			userLog.debug("Initializing backround processing of awaiting ack messages list...");
		}
		if (devLog.isDebugEnabled()) {
			devLog.debug("Initializing backround processing of awaiting ack messages list...");
		}
		
		super.initialize(nodeAccessor, properties);

		
		this.ackExtensionKey = properties.getProperty(PROP_KEY_ACK_EXTENSION_KEY);
		if (ackExtensionKey == null || ackExtensionKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_ACK_EXTENSION_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_ACK_EXTENSION_KEY));
		try {
			this.ackExtension = (HyCubeAckExtension) nodeAccessor.getExtension(this.ackExtensionKey);
			if (this.ackExtension == null) throw new InitializationException(InitializationException.Error.MISSING_EXTENSION_ERROR, this.ackExtensionKey, "The AckExtension is missing at the specified key: " + this.ackExtensionKey + ".");
		} catch (ClassCastException e) {
			throw new InitializationException(InitializationException.Error.MISSING_EXTENSION_ERROR, this.ackExtensionKey, "The AckExtension is expected to be an instance of: " + HyCubeAckExtension.class.getName());
		}
				
		
	}

	
	@Override
	protected void doProcess() {

		processAwaitingAcks();
		
	}
	

	public void processAwaitingAcks() throws BackgroundProcessException {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Running awaiting acks background process.");
		}
		
		
		ackExtension.getAckManager().processAwaitingAcks();
		
		
		
    	long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
    	long nextSchedTime = currTime + scheduleInterval; 
    	setNextSchedTime(nextSchedTime);
    	
    	
	}

	
	
}
