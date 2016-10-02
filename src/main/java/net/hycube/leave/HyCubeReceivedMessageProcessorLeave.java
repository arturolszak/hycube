package net.hycube.leave;

import java.util.List;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.HyCubeNodeIdFactory;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodeIdFactory;
import net.hycube.core.NodePointer;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.environment.NodeProperties;
import net.hycube.logging.LogHelper;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.HyCubeMessageType;
import net.hycube.messaging.messages.Message;
import net.hycube.messaging.messages.MessageByteConversionException;
import net.hycube.messaging.processing.MessageSendProcessInfo;
import net.hycube.messaging.processing.ProcessMessageException;
import net.hycube.messaging.processing.ReceivedMessageProcessor;
import net.hycube.transport.NetworkAdapterException;
import net.hycube.transport.NetworkNodePointer;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public class HyCubeReceivedMessageProcessorLeave implements ReceivedMessageProcessor {

	//private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeReceivedMessageProcessorLeave.class); 
	
	
	protected static final String PROP_KEY_MESSAGE_TYPES = "MessageTypes";
	protected static final String PROP_KEY_VALIDATE_LEAVE_MESSAGE_SENDER = "ValidateLeaveMessageSender";
	
	
	
	protected NodeAccessor nodeAccessor;
	protected NodeProperties properties;
	
	protected List<Enum<?>> messageTypes;
	
	protected boolean validateLeaveMessageSender;
	
	
	protected HyCubeLeaveManager leaveManager;
	
	protected int nodeIdDimensions;
	protected int nodeIdDigitsCount;
	
	protected int networkAddressByteLength;
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
	
		if (devLog.isDebugEnabled()) {
			devLog.debug("Initializing HyCubeReceivedMessageProcessorLeave.");
		}
		
		this.nodeAccessor = nodeAccessor;
		this.properties = properties;
		
		
		try {
			
			//load message types processed by this message processor:
			this.messageTypes = properties.getEnumListProperty(PROP_KEY_MESSAGE_TYPES, HyCubeMessageType.class);
			if (this.messageTypes == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES) + ".");
			
			if (nodeAccessor.getLeaveManager() instanceof HyCubeLeaveManager) {
				this.leaveManager = (HyCubeLeaveManager) nodeAccessor.getLeaveManager();
			}
			else {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize received message processor instance. The leave manager is expected to be an instance of: " + HyCubeLeaveManager.class.getName() + ".");
			}
			
			validateLeaveMessageSender = (Boolean) properties.getProperty(PROP_KEY_VALIDATE_LEAVE_MESSAGE_SENDER, MappedType.BOOLEAN);
			
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize received message processor instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		NodeIdFactory nodeIdFactory = nodeAccessor.getNodeIdFactory();
		if (nodeIdFactory instanceof HyCubeNodeIdFactory) {
			nodeIdDimensions = ((HyCubeNodeIdFactory)(nodeAccessor.getNodeIdFactory())).getDimensions();
			nodeIdDigitsCount = ((HyCubeNodeIdFactory)(nodeAccessor.getNodeIdFactory())).getDigitsCount();
			
			networkAddressByteLength = nodeAccessor.getNetworkAdapter().getAddressByteLength();
			
		}
		else {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize received message processor instance. node id factory should be an instance of: " + HyCubeNodeIdFactory.class.getName() + ".");
		}
		
		
	}
	
	
	@Override
	public boolean processMessage(Message message, NetworkNodePointer directSender) throws ProcessMessageException {
		
		HyCubeMessage msg = (HyCubeMessage)message;
		
		if (! messageTypes.contains(msg.getType())) return true;
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Message #" + msg.getSerialNoAndSenderString() + " received. Processing.");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Message #" + msg.getSerialNoAndSenderString() + " received. Processing.");
		}
		
		try {
			switch (msg.getType()) {
				case LEAVE:
					processLeaveMessage(msg, directSender);
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

	

	protected void processLeaveMessage(Message msg, NetworkNodePointer directSender) throws NetworkAdapterException, ProcessMessageException {
		
		if (msg.getRecipientId().equals(nodeAccessor.getNodeId())) {

			if (devLog.isDebugEnabled()) {
				devLog.debug("Processing LEAVE message #" + msg.getSerialNoAndSenderString() + ".");
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("Processing LEAVE message #" + msg.getSerialNoAndSenderString() + ".");
			}
					
			HyCubeLeaveMessageData leaveData = null;
			try {
				leaveData = HyCubeLeaveMessageData.fromBytes(msg.getData(), nodeIdDimensions, nodeIdDigitsCount, networkAddressByteLength);
			} catch (MessageByteConversionException e) {
				if (devLog.isDebugEnabled()) {
					devLog.debug("LEAVE message #" + msg.getSerialNoAndSenderString() + " is corrupted.", e);
				}
				if (msgLog.isInfoEnabled()) {
					msgLog.info("LEAVE message #" + msg.getSerialNoAndSenderString() + " is corrupted.");
				}
				return;
			}
			
			
			
			if (validateLeaveMessageSender) {
				//validate direct sender of the message - should be equal to the original sender
				if (! java.util.Arrays.equals(directSender.getAddressBytes(), (msg.getSenderNetworkAddress()))) {
					//drop the message:
					if (devLog.isDebugEnabled()) {
						devLog.debug("LEAVE message #" + msg.getSerialNoAndSenderString() + " dropped. Received from another node than the LEAVE sender.");
					}
					if (msgLog.isInfoEnabled()) {
						msgLog.info("LEAVE message #" + msg.getSerialNoAndSenderString() + " dropped. Received from another node than the LEAVE sender.");
					}
					return;					
				}
			}
			
			
			
			NodePointer sender = new NodePointer(nodeAccessor.getNetworkAdapter(), msg.getSenderNetworkAddress(), msg.getSenderId());
			
			NodePointer[] nodePointers = leaveData.getNodePointers(nodeAccessor.getNetworkAdapter());
			
			
			this.leaveManager.processLeaveMessage(sender, directSender, nodePointers);
	
		}
		else {
			nodeAccessor.sendMessage(new MessageSendProcessInfo(msg), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
		}
		
	}
    


	@Override
	public void discard() {	
	}
	
	
	
}
