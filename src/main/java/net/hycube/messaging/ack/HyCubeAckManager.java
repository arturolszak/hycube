package net.hycube.messaging.ack;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import net.hycube.backgroundprocessing.BackgroundProcessException;
import net.hycube.configuration.GlobalConstants;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodeId;
import net.hycube.core.NodePointer;
import net.hycube.environment.NodeProperties;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventCategory;
import net.hycube.logging.LogHelper;
import net.hycube.messaging.data.DataMessageSendProcessInfo;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.HyCubeMessageFactory;
import net.hycube.messaging.messages.HyCubeMessageType;
import net.hycube.messaging.messages.Message;
import net.hycube.messaging.messages.MessageByteConversionException;
import net.hycube.messaging.processing.MessageSendProcessInfo;
import net.hycube.messaging.processing.ProcessMessageException;
import net.hycube.routing.HyCubeRoutingManager;
import net.hycube.transport.NetworkAdapterException;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public class HyCubeAckManager {

	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeAckManager.class); 
	
	
	protected static final String PROP_KEY_APPLY_SECURE_ROUTING_AFTER_NOT_DELIVERED_COUNT = "ApplySecureRoutingAfterNotDeliveredCount";
	protected static final String PROP_KEY_APPLY_SKIPPING_NEXT_HOPS_AFTER_NOT_DELIVERED_COUNT = "ApplySkippingNextHopsAfterNotDeliveredCount";
	protected static final String PROP_KEY_VALIDATE_ACK_SENDER = "ValidateAckSender";
	
	
	
	protected NodeAccessor nodeAccessor;
	
	protected final Object ackAwaitingLock = new Object();
	
	protected HashMap<Integer, AckProcessInfo> ackAwaitingMap;
	
	
	protected int applySecureRoutingAfterNotDeliveredCount;
	protected int applySkippingNextHopsAfterNotDeliveredCount;
	
	protected boolean validateAckSender;
	
	
	
	
	public Object getAckAwaitingLock() {
		return ackAwaitingLock;
	}
	
	public HashMap<Integer, AckProcessInfo> getAckAwaitingMap() {
		return ackAwaitingMap;
	}
	
	
	
	
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Initializing HyCubeReceivedMessageProcessorData.");
		}
		
		this.nodeAccessor = nodeAccessor;
		
		this.ackAwaitingMap = new HashMap<Integer, AckProcessInfo>();
		
		try {
			if (properties.containsKey(PROP_KEY_APPLY_SECURE_ROUTING_AFTER_NOT_DELIVERED_COUNT)) {
				this.applySecureRoutingAfterNotDeliveredCount = (Integer) properties.getProperty(PROP_KEY_APPLY_SECURE_ROUTING_AFTER_NOT_DELIVERED_COUNT, MappedType.INT);
			}
			else {
				this.applySecureRoutingAfterNotDeliveredCount = 0;
			}

			if (properties.containsKey(PROP_KEY_APPLY_SKIPPING_NEXT_HOPS_AFTER_NOT_DELIVERED_COUNT)) {
				this.applySkippingNextHopsAfterNotDeliveredCount = (Integer) properties.getProperty(PROP_KEY_APPLY_SKIPPING_NEXT_HOPS_AFTER_NOT_DELIVERED_COUNT, MappedType.INT);
			}
			else {
				this.applySkippingNextHopsAfterNotDeliveredCount = 0;
			}

			
			this.validateAckSender = (Boolean) properties.getProperty(PROP_KEY_VALIDATE_ACK_SENDER, MappedType.BOOLEAN);
			
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, e.getKey(), "An error occured while reading a node parameter. The property could not be converted: " + e.getKey(), e);
		} 
				
				
		
	}
	
	
	public void processSendDataMessage(MessageSendProcessInfo mspi) {
		
    	if (devLog.isDebugEnabled()) {
			devLog.debug("Processing DATA message #" + mspi.getMsg().getSerialNoAndSenderString() + " before sending.");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing DATA message #" + mspi.getMsg().getSerialNoAndSenderString() + " before sending.");
		}
		
		DataMessageSendProcessInfo dmspi = (DataMessageSendProcessInfo)mspi;
		
		

		if (HyCubeRoutingManager.getRoutingParameterAnonymousRoute(dmspi.getRoutingParameters()) && (!(HyCubeRoutingManager.getRoutingParameterRegisterRoute(dmspi.getRoutingParameters())))) {
			return;
		}
		
		
    	if (nodeAccessor.getNodeParameterSet().isMessageAckEnabled()) {
    		synchronized (ackAwaitingLock) {    			
   				dmspi.getAckProcessInfo().setDiscardTimestamp(nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime() + dmspi.getAckProcessInfo().getAckTimeout());
   				ackAwaitingMap.put(dmspi.getMsg().getSerialNo(), dmspi.getAckProcessInfo());
   				
    		}		    	
    	}
    	
	}
	
	
	public void processDeliveredDataMessage(Message message) throws NetworkAdapterException, ProcessMessageException {

		HyCubeMessage msg = (HyCubeMessage) message;
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Received DATA message #" + msg.getSerialNoAndSenderString() + ". Port number: " + msg.getDestinationPort() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Received DATA message #" + msg.getSerialNoAndSenderString() + ". Port number: " + msg.getDestinationPort() + ".");
		}
			

		if (nodeAccessor.getNodeParameterSet().isMessageAckEnabled()) {
			
			if ((!msg.isAnonymousRoute()) || msg.isRegisterRoute()) {	//if the route is anonymous and not registered, the ack would probably be not delivered to the original message sender, so it should not be sent
			
				//send ack:
				int ackSerialNo = nodeAccessor.getNextMessageSerialNo();
				byte[] ackData = (new HyCubeAckMessageData(msg.getSerialNo())).getBytes();
				HyCubeMessageFactory messageFactory = (HyCubeMessageFactory) nodeAccessor.getMessageFactory();
				
				//send with route back if the route was registered (even if direct ack -> the message may have anonymous sender)
				Message ackMessage = messageFactory.newMessage(ackSerialNo, nodeAccessor.getNodeId(), msg.getSenderId(), nodeAccessor.getNetworkAdapter().getPublicAddressBytes(), false, msg.isRegisterRoute(), (msg.isRegisterRoute() ? msg.getRouteId() : 0), false, HyCubeMessageType.DATA_ACK, nodeAccessor.getNodeParameterSet().getMessageTTL(), (short)0, msg.isSecureRoutingApplied(), msg.isSkipRandomNumOfNodesApplied(), msg.getDestinationPort(), msg.getSourcePort(), ackData); 
	
				if (devLog.isDebugEnabled()) {
					devLog.debug("Sending ACK");
				}
				
				if (nodeAccessor.getNodeParameterSet().isDirectAck()) {
					NodePointer np = new NodePointer(nodeAccessor.getNetworkAdapter(), msg.getSenderNetworkAddress(), msg.getSenderId());
					MessageSendProcessInfo mspi = new MessageSendProcessInfo(ackMessage, np.getNetworkNodePointer(), false);
					nodeAccessor.sendMessage(mspi, GlobalConstants.WAIT_ON_BKG_MSG_SEND);
				}
				else {
					nodeAccessor.sendMessage(new MessageSendProcessInfo(ackMessage), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
				}
			}
			
		}
		
	}

	
	
	
	public void processDataAckMessage(Message message) {

		HyCubeMessage msg = (HyCubeMessage) message;
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Received DATA_ACK message #" + msg.getSerialNoAndSenderString() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Received DATA_ACK message #" + msg.getSerialNoAndSenderString() + ".");
		}
		
		HyCubeAckMessageData ackData = null;
		try {
			ackData = HyCubeAckMessageData.fromBytes(msg.getData());
		} catch (MessageByteConversionException e) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("DATA_ACK message #" + msg.getSerialNoAndSenderString() + " is corrupted.", e);
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("DATA_ACK message #" + msg.getSerialNoAndSenderString() + "is corrupted.");
			}
		}
		
		//process the ack
		if (devLog.isDebugEnabled()) {
			devLog.debug("Processing DATA_ACK message #" + msg.getSerialNoAndSenderString() + " for message #" + ackData.getAckSerialNo() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Processing DATA_ACK message #" + msg.getSerialNoAndSenderString() + " for message #" + ackData.getAckSerialNo() + ".");
		}
		
		
		synchronized(ackAwaitingLock) {
			if (ackAwaitingMap.containsKey(ackData.getAckSerialNo())) {
				AckProcessInfo ackPr = ackAwaitingMap.get(ackData.getAckSerialNo());
				synchronized (ackPr) {
					if (validateAckSender == false || NodeId.compareIds(ackPr.getMessage().getRecipientId(), msg.getSenderId())) {
						if (! ackPr.isProcessed()) {
							if (devLog.isDebugEnabled()) {
								devLog.debug("Processing ACK");
							}
							ackPr.process(msg);
							ackAwaitingMap.remove(ackData.getAckSerialNo());
							if (ackPr.getAckCallback() != null) {
								BlockingQueue<Event> processAckCallbackEventQueue = nodeAccessor.getEventQueue(EventCategory.processAckCallbackEvent);
								processAckCallbackEventQueue.add(new MessageAckCallbackEvent(nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime(), nodeAccessor.getProcessEventProxy(), MessageAckCallbackType.DELIVERED, ackPr.getAckCallback(), ackPr.getAckCallbackArg()));
							}
						}
					}
				}
			}
		}
		
	}
	
	
	
	public void processAwaitingAcks() throws BackgroundProcessException {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Processing awaiting acks.");
		}
		
    	long currTimestamp = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
    	List<Integer> ackPrToRemove = null;
    	List<AckProcessInfo> ackPrAwaitingList = null;
    	

    	//check map
    	
    	synchronized (ackAwaitingLock) {
	    	ackPrToRemove = new ArrayList<Integer>(ackAwaitingMap.size());
	    	ackPrAwaitingList = new ArrayList<AckProcessInfo>(ackAwaitingMap.size());
	    	ackPrAwaitingList.addAll(ackAwaitingMap.values());
    	}
	    
    	for (AckProcessInfo ackPr : ackPrAwaitingList) {
	    	synchronized (ackPr) {
	    		if (! ackPr.isProcessed()) {	//for thread safety check if processed (another thread could have processed it meanwhile)
			   		if (ackPr.getDiscardTimestamp() <= currTimestamp) {
			   			ackPr.discard();	//will decrease the remaining send attempts number
			   			ackPr.setProcessed();
			   			
			   			if (ackPr.getSendAttempts() > 0) {

			   				//resend, (don't remove from ackResendAwaitingMap -> the element will be replaced by the new one)
			   				if (applySecureRoutingAfterNotDeliveredCount != 0 && ackPr.getSendCounter() >= applySecureRoutingAfterNotDeliveredCount) {
			   					ackPr.getMessage().setSecureRoutingApplied(true);
			   				}
			   				
			   				if (applySkippingNextHopsAfterNotDeliveredCount != 0 && ackPr.getSendCounter() >= applySkippingNextHopsAfterNotDeliveredCount) {
			   					((HyCubeMessage)ackPr.getMessage()).setSkipRandomNumOfNodesApplied(true);
			   				}
			   				
			   				if (devLog.isDebugEnabled()) {
			   					devLog.debug("Resending");
			   				}
			   				
			   				try {
			   					nodeAccessor.resendMessage(ackPr);
								//the ackProcessInfo will be replaced by the new one before resending -> no need to remove this one (will not be processed again, -> setProcessed() called)
							} catch (Exception e) {
								throw new BackgroundProcessException("An exception thrown while resending a message.", e);
							}
			   				
			   			}
			   			else {
			   				//set the processed flag (so that the ack info will not be processed again)
			   				
			   				if (devLog.isDebugEnabled()) {
			   					devLog.debug("DISCARDING_ACK");
			   				}
			   				
			   				//do not retry sending
			   				
				   			//remove from ackResendAwaitingMap
			   				ackPrToRemove.add(ackPr.getMessageSerialNo());
			   				
			   				if (ackPr.getAckCallback() != null) {
					    		//don't callback immediately, let the user callback code be executed by a designated thread, add to the queue
						   		BlockingQueue<Event> processAckCallbackEventQueue = nodeAccessor.getEventQueue(EventCategory.processAckCallbackEvent);
						   		processAckCallbackEventQueue.add(new MessageAckCallbackEvent(nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime(), this.nodeAccessor.getProcessEventProxy(), MessageAckCallbackType.UNDELIVERED, ackPr.getAckCallback(), ackPr.getAckCallbackArg()));
			   				}

			   			}
			   		}
	    		}

	    	}
	    }
    	synchronized (ackAwaitingLock) {
		    for (int msgSerialNo : ackPrToRemove) {
		    	ackAwaitingMap.remove(msgSerialNo);
		    }
    	}
    	
    	
    	
	}
	
	
	
	
}
