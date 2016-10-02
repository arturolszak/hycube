package net.hycube.maintenance;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.HyCubeRoutingTable;
import net.hycube.core.HyCubeRoutingTableSlotInfo;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodeId;
import net.hycube.core.NodePointer;
import net.hycube.core.RoutingTableEntry;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.environment.NodeProperties;
import net.hycube.logging.LogHelper;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.HyCubeMessageFactory;
import net.hycube.messaging.messages.HyCubeMessageType;
import net.hycube.messaging.messages.Message;
import net.hycube.messaging.messages.MessageByteConversionException;
import net.hycube.messaging.processing.MessageSendProcessInfo;
import net.hycube.messaging.processing.ProcessMessageException;
import net.hycube.messaging.processing.ReceivedMessageProcessor;
import net.hycube.transport.NetworkAdapterException;
import net.hycube.transport.NetworkNodePointer;

public class HyCubeReceivedMessageProcessorPing implements ReceivedMessageProcessor {

	//private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeReceivedMessageProcessorPing.class); 
	
	
	protected static final String PROP_KEY_KEEP_ALIVE_EXTENSION_KEY = "KeepAliveExtensionKey";
	protected static final String PROP_KEY_MESSAGE_TYPES = "MessageTypes";
	
	
	protected static final boolean DIRECT_PONG = true;
	
	
	protected NodeAccessor nodeAccessor;
	
	protected HyCubeRoutingTable routingTable;
	
	protected NodeProperties properties;
	
	protected List<Enum<?>> messageTypes;
	
	protected String keepAliveExtensionKey;
	protected HyCubeKeepAliveExtension keepAliveExtension;
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
	
		if (devLog.isDebugEnabled()) {
			devLog.debug("Initializing HyCubeReceivedMessageProcessorPing.");
		}
		
		
		this.properties = properties;
		
		this.nodeAccessor = nodeAccessor;
		
		
		if (!(nodeAccessor.getRoutingTable() instanceof HyCubeRoutingTable)) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "The routing table is expected to be an instance of: " + HyCubeRoutingTable.class.getName());
		}
		this.routingTable = (HyCubeRoutingTable) nodeAccessor.getRoutingTable();
		
		
		
		
		try {
			//load message types processed by this message processor:
			this.messageTypes = properties.getEnumListProperty(PROP_KEY_MESSAGE_TYPES, HyCubeMessageType.class);
			if (this.messageTypes == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES) + ".");
			
			
			//get ping extension:
			keepAliveExtensionKey = properties.getProperty(PROP_KEY_KEEP_ALIVE_EXTENSION_KEY);
			if (keepAliveExtensionKey == null || keepAliveExtensionKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_KEEP_ALIVE_EXTENSION_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_KEEP_ALIVE_EXTENSION_KEY));
			try {
				keepAliveExtension = (HyCubeKeepAliveExtension) nodeAccessor.getExtension(keepAliveExtensionKey);
				if (this.keepAliveExtension == null) throw new InitializationException(InitializationException.Error.MISSING_EXTENSION_ERROR, this.keepAliveExtensionKey, "The KeepAliveExtension is missing at the specified key: " + this.keepAliveExtensionKey + ".");
			} catch (ClassCastException e) {
				throw new InitializationException(InitializationException.Error.MISSING_EXTENSION_ERROR, this.keepAliveExtensionKey, "The KeepAliveExtension is missing at the specified key: " + this.keepAliveExtensionKey + ".");
			}

			
			
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize received message processor instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		
		
	}
	
	
	@Override
	public boolean processMessage(Message message, NetworkNodePointer directSender) throws ProcessMessageException {
		
		HyCubeMessage msg = (HyCubeMessage)message;
		
		if (! messageTypes.contains(msg.getType())) return true;
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Processing message #" + msg.getSerialNoAndSenderString() + ". Message validated.");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Message #" + msg.getSerialNoAndSenderString() + " received. Processing.");
		}
		
		try {
			switch (msg.getType()) {
				case PING:
					processPingMessage(msg);
					break;
				case PONG:
					processPongMessage(msg);
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


	
    
	protected void processPingMessage(Message msg) throws NetworkAdapterException, ProcessMessageException {
		
		if (msg.getRecipientId().equals(nodeAccessor.getNodeId())) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("Received PING message #" + msg.getSerialNoAndSenderString() + ". Sending PONG.");
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("Received PING message #" + msg.getSerialNoAndSenderString() + ". Sending PONG.");
			}
			
			int pingSerialNo = msg.getSerialNo();
			byte[] pongData = (new HyCubePongMessageData(pingSerialNo)).getBytes();

			int pongSerialNo = nodeAccessor.getNextMessageSerialNo();
			HyCubeMessageFactory messageFactory = (HyCubeMessageFactory) nodeAccessor.getMessageFactory();
			Message pongMessage = messageFactory.newMessage(pongSerialNo, nodeAccessor.getNodeId(), msg.getSenderId(), nodeAccessor.getNetworkAdapter().getPublicAddressBytes(), HyCubeMessageType.PONG, nodeAccessor.getNodeParameterSet().getMessageTTL(), (short)0, false, false, (short)0, (short)0, pongData); 
			
			if (DIRECT_PONG) {
				NodePointer np = new NodePointer(nodeAccessor.getNetworkAdapter(), msg.getSenderNetworkAddress(), msg.getSenderId());
				nodeAccessor.sendMessage(new MessageSendProcessInfo(pongMessage, np.getNetworkNodePointer(), false), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
			}
			else {
				nodeAccessor.sendMessage(new MessageSendProcessInfo(pongMessage), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
			}
		}
		else {
			nodeAccessor.sendMessage(new MessageSendProcessInfo(msg), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
		}
	}
	
	protected void processPongMessage(Message msg) {
		if (devLog.isDebugEnabled()) {
			devLog.debug("Received PONG message #" + msg.getSerialNoAndSenderString() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Received PONG message #" + msg.getSerialNoAndSenderString() + ".");
		}
		
		HyCubePongMessageData pongData = null;
		try {
			pongData = HyCubePongMessageData.fromBytes(msg.getData());
		} catch (MessageByteConversionException e) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("PONG message #" + msg.getSerialNoAndSenderString() + " is corrupted.", e);
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("PONG message #" + msg.getSerialNoAndSenderString() + "is corrupted.");
			}
		}
		
		//process the pong
		if (devLog.isDebugEnabled()) {
			devLog.debug("Processing PONG message #" + msg.getSerialNoAndSenderString() + " for PING message #" + pongData.getPingSerialNo() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing PONG message #" + msg.getSerialNoAndSenderString() + " for PING message #" + pongData.getPingSerialNo() + ".");
		}
		synchronized (keepAliveExtension.getPongAwaitingLock()) {
			Map<Integer, HyCubePongProcessInfo> pongAwaitingMap = keepAliveExtension.getPongAwaitingMap();
			if (pongAwaitingMap.containsKey(pongData.getPingSerialNo())) {
				HyCubePongProcessInfo pongPr = pongAwaitingMap.get(pongData.getPingSerialNo());
				
				if (Arrays.equals(pongPr.getNodeNetworkAddress(), msg.getSenderNetworkAddress())
						&& NodeId.compareIds(pongPr.getNodeId(), msg.getSenderId())) {
					
					routingTable.lockRoutingTableForRead();
					List<RoutingTableEntry> rtes = nodeAccessor.getRoutingTable().getRoutingTableEntriesByNodeIdHash(pongPr.getNodeIdHash());
					routingTable.unlockRoutingTableForRead();
					
					for (RoutingTableEntry rte : rtes) {
						if (rte != null && rte.getNode().getNetworkNodePointer().getAddressString().equals(pongPr.getNodeNetworkAddress())) {
							
							HyCubeRoutingTableSlotInfo slotInfo = (HyCubeRoutingTableSlotInfo) rte.getOuterRef();
		    				
		    				//acquire the write lock
	    					routingTable.getLockByRtType(slotInfo.getType()).writeLock().lock();
							
							double rtePingResponseIndicator = (Double) rte.getData(keepAliveExtension.getPingResponseIndicatorRteKey(), keepAliveExtension.getInitialPingResponseIndicatorValue());
							rtePingResponseIndicator = rtePingResponseIndicator * (1 - keepAliveExtension.getPingResponseIndicatorUpdateCoefficient()) + keepAliveExtension.getMaxPingResponseIndicatorValue() * keepAliveExtension.getPingResponseIndicatorUpdateCoefficient();
							rte.setData(keepAliveExtension.getPingResponseIndicatorRteKey(), rtePingResponseIndicator);
							
							//release the write lock
	    					routingTable.getLockByRtType(slotInfo.getType()).writeLock().unlock();
	    					
						}
					}
					
					boolean processResult = pongPr.process(msg);
					if (processResult) pongAwaitingMap.remove(pongData.getPingSerialNo());
				}
				
			}
		}
		
	}

    

	@Override
	public void discard() {	
	}
	
	

}
