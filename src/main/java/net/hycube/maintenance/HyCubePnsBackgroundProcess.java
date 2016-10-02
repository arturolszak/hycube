package net.hycube.maintenance;

import net.hycube.backgroundprocessing.AbstractBackgroundProcess;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.environment.NodeProperties;
import net.hycube.logging.LogHelper;

public class HyCubePnsBackgroundProcess extends AbstractBackgroundProcess {

	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubePnsBackgroundProcess.class); 
	
	
	protected static final String PROP_KEY_PNS_EXTENSION_KEY = "PnsExtensionKey";
	
	
	protected String pnsExtensionKey;
	protected HyCubePnsExtension pnsExtension;
	
	
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {

		if (userLog.isDebugEnabled()) {
			userLog.debug("Initializing pns backround process...");
		}
		if (devLog.isDebugEnabled()) {
			devLog.debug("Initializing pns backround process...");
		}
		
		
		super.initialize(nodeAccessor, properties);

		
		this.pnsExtensionKey = properties.getProperty(PROP_KEY_PNS_EXTENSION_KEY);
		if (pnsExtensionKey == null || pnsExtensionKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_PNS_EXTENSION_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_PNS_EXTENSION_KEY));
		try {
			this.pnsExtension = (HyCubePnsExtension) nodeAccessor.getExtension(pnsExtensionKey);
			if (this.pnsExtension == null) throw new InitializationException(InitializationException.Error.MISSING_EXTENSION_ERROR, this.pnsExtensionKey, "The PnsExtension is missing at the specified key: " + this.pnsExtensionKey + ".");
		} catch (ClassCastException e) {
			throw new InitializationException(InitializationException.Error.MISSING_EXTENSION_ERROR, this.pnsExtensionKey, "The PnsExtension is missing at the specified key: " + this.pnsExtensionKey + ".");
		}

		
	}

	
	@Override
	public void doProcess() {
		
		processPns();
		
	}
	
	
	public void processPns() {
		
		
	}
	
		
}
