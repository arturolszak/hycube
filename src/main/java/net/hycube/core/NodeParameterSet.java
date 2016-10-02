/**
 * 
 */
package net.hycube.core;

import net.hycube.configuration.GlobalConstants;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.environment.NodeProperties;
import net.hycube.utils.ObjectToStringConverter.MappedType;



/**
 * @author Artur Olszak
 *
 */
public class NodeParameterSet {
	

	//properties keys:
	
	public static final String PROP_KEY_NODE_ID_FACTORY = "NodeIdFactory";
	public static final String PROP_KEY_MESSAGE_FACTORY = "MessageFactory";
	public static final String PROP_KEY_ROUTING_TABLE = "RoutingTable";
	
	public static final String PROP_KEY_NEXT_HOP_SELECTORS = "NextHopSelectors";
	
	public static final String PROP_KEY_ROUTING_MANAGER = "RoutingManager";
	public static final String PROP_KEY_LOOKUP_MANAGER = "LookupManager";
	public static final String PROP_KEY_SEARCH_MANAGER = "SearchManager";
	public static final String PROP_KEY_JOIN_MANAGER = "JoinManager";
	public static final String PROP_KEY_LEAVE_MANAGER = "LeaveManager";
	public static final String PROP_KEY_DHT_MANAGER = "DHTManager";
	
	public static final String PROP_KEY_NOTIFY_PROCESSOR = "NotifyProcessor";
	
	public static final String PROP_KEY_NETWORK_ADAPTER = "NetworkAdapter";
	
	public static final String PROP_KEY_EXTENSIONS = "Extensions";
	public static final String PROP_KEY_BACKGROUND_PROCESSES = "BackgroundProcesses";
	public static final String PROP_KEY_RECEIVED_MESSAGE_PROCESSORS = "ReceivedMessageProcessors";
	public static final String PROP_KEY_MESSAGE_SEND_PROCESSORS = "MessageSendProcessors";
	
	
	
	
	
	
	public static final String PROP_KEY_MESSAGE_TTL = "MessageTTL";
	
	public static final String PROP_KEY_USE_PORTS = "UsePorts";
	
	public static final String PROP_KEY_ACK_ENABLED = "MessageAckEnabled"; 
	public static final String PROP_KEY_DIRECT_ACK = "DirectAck";
	public static final String PROP_KEY_ACK_TIMEOUT = "AckTimeout";
	public static final String PROP_KEY_PROCESS_ACK_INTERVAL = "ProcessAckInterval"; 
	public static final String PROP_KEY_RESEND_IF_NO_ACK = "ResendIfNoAck";
	public static final String PROP_KEY_SEND_RETRIES = "SendRetries";


	
	
	
	protected short messageTTL;
	
	protected boolean usePorts;
	protected boolean messageAckEnabled;
	protected boolean directAck;
	protected int ackTimeout;
	protected int processAckInterval;
	protected boolean resendIfNoAck;
	protected int sendRetries;
	
	
	

	public NodeParameterSet() {
		
	}
	
	
	
	

	public short getMessageTTL() {
		return messageTTL;
	}

	public void setMessageTTL(short messageTTL) {
		this.messageTTL = messageTTL;
	}

	public boolean isUsePorts() {
		return usePorts;
	}

	public void setUsePorts(boolean usePorts) {
		this.usePorts = usePorts;
	}

	public boolean isMessageAckEnabled() {
		return messageAckEnabled;
	}

	public void setMessageAckEnabled(boolean dataMessageAckEnabled) {
		this.messageAckEnabled = dataMessageAckEnabled;
	}

	public boolean isDirectAck() {
		return directAck;
	}

	public void setDirectAck(boolean directAck) {
		this.directAck = directAck;
	}

	public int getAckTimeout() {
		return ackTimeout;
	}

	public void setAckTimeout(int ackTimeout) {
		this.ackTimeout = ackTimeout;
	}

	public int getProcessAckInterval() {
		return processAckInterval;
	}

	public void setProcessAckInterval(int processAckInterval) {
		this.processAckInterval = processAckInterval;
	}

	public boolean isResendIfNoAck() {
		return resendIfNoAck;
	}

	public void setResendIfNoAck(boolean resendIfNoAck) {
		this.resendIfNoAck = resendIfNoAck;
	}

	public int getSendRetries() {
		return sendRetries;
	}

	public void setSendRetries(int sendRetries) {
		this.sendRetries = sendRetries;
	}
	
	
	
	public void readParameters(NodeProperties properties) throws InitializationException {
		//read parameters:
		try {
			this.messageTTL = (Short) properties.getProperty(NodeParameterSet.PROP_KEY_MESSAGE_TTL, MappedType.SHORT);
			if (this.messageTTL < 1) {
				throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, NodeParameterSet.PROP_KEY_MESSAGE_TTL, "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_MESSAGE_TTL));
			}
			
			this.usePorts = (Boolean) properties.getProperty(NodeParameterSet.PROP_KEY_USE_PORTS, MappedType.BOOLEAN);
			
			this.messageAckEnabled = (Boolean) properties.getProperty(NodeParameterSet.PROP_KEY_ACK_ENABLED, MappedType.BOOLEAN);
			
			this.directAck = (Boolean) properties.getProperty(NodeParameterSet.PROP_KEY_DIRECT_ACK, MappedType.BOOLEAN);
			
			this.ackTimeout = (Integer) properties.getProperty(NodeParameterSet.PROP_KEY_ACK_TIMEOUT, MappedType.INT);
			if (this.ackTimeout <= 0 || this.ackTimeout > GlobalConstants.MAX_ACK_TIMEOUT) {
				throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, NodeParameterSet.PROP_KEY_ACK_TIMEOUT, "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_ACK_TIMEOUT));
			}
			
			this.processAckInterval = (Integer) properties.getProperty(NodeParameterSet.PROP_KEY_PROCESS_ACK_INTERVAL, MappedType.INT);
			if (this.processAckInterval <= 0 || this.processAckInterval > GlobalConstants.MAX_PROCESS_ACK_INTERVAL) {
				throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, NodeParameterSet.PROP_KEY_PROCESS_ACK_INTERVAL, "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_PROCESS_ACK_INTERVAL));
			}
			
			this.resendIfNoAck = (Boolean) properties.getProperty(NodeParameterSet.PROP_KEY_RESEND_IF_NO_ACK, MappedType.BOOLEAN);
			
			this.sendRetries = (Integer) properties.getProperty(NodeParameterSet.PROP_KEY_SEND_RETRIES, MappedType.INT);
			if (this.sendRetries <= 0 || this.sendRetries > GlobalConstants.MAX_SEND_RETRIES) {
				throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, NodeParameterSet.PROP_KEY_SEND_RETRIES, "Invalid parameter value: " + properties.getAbsoluteKey(NodeParameterSet.PROP_KEY_SEND_RETRIES));
			}
			
			
			

		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, e.getKey(), "An error occured while reading a node parameter. The property could not be converted: " + e.getKey(), e);
		}

	}
	
	
}
