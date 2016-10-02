package net.hycube.maintenance;

import java.util.List;

import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodePointer;
import net.hycube.core.UnrecoverableRuntimeException;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.environment.NodeProperties;
import net.hycube.logging.LogHelper;
import net.hycube.maintenance.HyCubeKeepAliveExtension;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.HyCubeMessageType;
import net.hycube.messaging.processing.MessageSendProcessInfo;
import net.hycube.messaging.processing.MessageSendProcessor;
import net.hycube.messaging.processing.ProcessMessageException;

public class HyCubeMessageSendProcessorPing implements MessageSendProcessor {

	//private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeMessageSendProcessorPing.class); 
	
	protected static final String PROP_KEY_KEEP_ALIVE_EXTENSION_KEY = "KeepAliveExtensionKey";
	protected static final String PROP_KEY_MESSAGE_TYPES = "MessageTypes";
	
	
	protected NodeAccessor nodeAccessor;
	protected NodeProperties properties;
	
	protected List<Enum<?>> messageTypes;
	
	protected String keepAliveExtensionKey;
	protected HyCubeKeepAliveExtension keepAliveExtension;

	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Initializing HyCubeMessageSendProcessorPing.");
		}
		
		this.nodeAccessor = nodeAccessor;
		this.properties = properties;
		
		try {
			//load message types processed by this message processor:
			this.messageTypes = properties.getEnumListProperty(PROP_KEY_MESSAGE_TYPES, HyCubeMessageType.class);
			if (this.messageTypes == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES) + ".");

			//get the ping extension:
			keepAliveExtensionKey = properties.getProperty(PROP_KEY_KEEP_ALIVE_EXTENSION_KEY);
			if (keepAliveExtensionKey == null || keepAliveExtensionKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_KEEP_ALIVE_EXTENSION_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_KEEP_ALIVE_EXTENSION_KEY));
			try {
				keepAliveExtension = (HyCubeKeepAliveExtension) nodeAccessor.getExtension(keepAliveExtensionKey);
				if (this.keepAliveExtension == null) throw new InitializationException(InitializationException.Error.MISSING_EXTENSION_ERROR, this.keepAliveExtensionKey, "The KeepAliveExtension is missing at the specified key: " + this.keepAliveExtensionKey + ".");
			} catch (ClassCastException e) {
				throw new InitializationException(InitializationException.Error.MISSING_EXTENSION_ERROR, this.keepAliveExtensionKey, "The KeepAliveExtension is missing at the specified key: " + this.keepAliveExtensionKey + ".");
			}


			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize message send processor instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		
	}

	@Override
	public boolean processSendMessage(MessageSendProcessInfo mspi) throws ProcessMessageException {
		
		if (! mspi.getProcessBeforeSend()) return true;
		
		HyCubeMessage msg = (HyCubeMessage)mspi.getMsg();
		
		if (! messageTypes.contains(msg.getType())) return true;
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Processing message #" + msg.getSerialNoAndSenderString() + " before sending.");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing message #" + msg.getSerialNoAndSenderString() + " before sending.");
		}
		
		try {
			switch (msg.getType()) {
				case PING:
					processSendPingMessage(mspi);
					break;
				default:
					break;
			}
		}
		catch (Exception e) {
			throw new ProcessMessageException("An exception thrown while processing a message.", e);
		}
		
		return true;
		
	}
	

	
	public void processSendPingMessage(MessageSendProcessInfo mspi) {
		
    	if (devLog.isDebugEnabled()) {
			devLog.debug("Processing PING message #" + mspi.getMsg().getSerialNoAndSenderString() + " before sending.");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing PING message #" + mspi.getMsg().getSerialNoAndSenderString() + " before sending.");
		}	
		
		
		HyCubePingMessageSendProcessInfo pmspi = (HyCubePingMessageSendProcessInfo)mspi;

    	
    	NodePointer np = null;
    	long nodeIdHash = 0;
    	if (pmspi.getNodePointer() != null) {
    		np = pmspi.getNodePointer();
    		nodeIdHash = pmspi.getNodePointer().getNodeIdHash();
    	}
    	else throw new UnrecoverableRuntimeException("Node to be pinged not set.");
    	
    	HyCubePongProcessInfo pongPr = new HyCubePongProcessInfo(np.getNodeId(), nodeIdHash, np.getNetworkNodePointer().getAddressBytes());
    	pongPr.setPingSerialNo(pmspi.getMsg().getSerialNo());
    	pongPr.setDiscardTimestamp(nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime() + keepAliveExtension.getPongTimeout());
    	
    	synchronized (keepAliveExtension.getPongAwaitingLock()) {
    		keepAliveExtension.getPongAwaitingMap().put(pmspi.getMsg().getSerialNo(), pongPr);
    	}
    	
	}
	
	

	@Override
	public void discard() {	
	}
	
	
	
}
