package net.hycube.messaging.data;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.environment.NodeProperties;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventCategory;
import net.hycube.logging.LogHelper;
import net.hycube.messaging.ack.HyCubeAckExtension;
import net.hycube.messaging.ack.HyCubeAckManager;
import net.hycube.messaging.callback.MessageReceivedCallback;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.HyCubeMessageType;
import net.hycube.messaging.messages.Message;
import net.hycube.messaging.processing.MessageSendProcessInfo;
import net.hycube.messaging.processing.ProcessMessageException;
import net.hycube.messaging.processing.ReceivedMessageInfo;
import net.hycube.messaging.processing.ReceivedMessageProcessor;
import net.hycube.transport.NetworkAdapterException;
import net.hycube.transport.NetworkNodePointer;
import net.hycube.utils.HashMapUtils;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public class HyCubeReceivedMessageProcessorData implements ReceivedMessageProcessor {

	//private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeReceivedMessageProcessorData.class); 
	
	
	protected static final String PROP_KEY_MESSAGE_TYPES = "MessageTypes";
	protected static final String PROP_KEY_ACK_ENABLED = "AckEnabled";
	protected static final String PROP_KEY_ACK_EXTENSION_KEY = "AckExtensionKey";
	protected static final String PROP_KEY_PROCESS_DATA_MESSAGE_IF_CANNOT_ROUTE = "ProcessDataMessageIfCannotRoute";
	protected static final String PROP_KEY_PREVENT_DUPLICATES = "PreventDuplicates";
	protected static final String PROP_KEY_PREVENT_ANONYMOUS_DUPLICATES = "PreventAnonymousDuplicates";
	protected static final String PROP_KEY_PREVENT_DUPLICATES_INCLUDE_CRC = "PreventDuplicatesIncludeCRC";
	protected static final String PROP_KEY_PREVENT_DUPLICATES_RETENTION_PERIOD = "PreventDuplicatesRetentionPeriod";
	protected static final String PROP_KEY_PREVENT_DUPLICATES_CACHE_MAX_SIZE = "PreventDuplicatesCacheMaxSize";

	
	protected static final int INITIAL_RECENT_MESSAGES_COLLECTION_SIZE = GlobalConstants.DEFAULT_INITIAL_COLLECTION_SIZE;
	
	
	
	protected NodeAccessor nodeAccessor;
	protected NodeProperties properties;
	
	protected List<Enum<?>> messageTypes;
	
	protected boolean ackEnabled;
	protected String ackExtensionKey;
	protected HyCubeAckExtension ackExtension;
	protected HyCubeAckManager ackManager;
	
	protected boolean processDataMessageIfCannotRoute;
	
	
	protected boolean preventDuplicates;
	protected boolean preventAnonymousDuplicates;
	protected boolean preventDuplicatesIncludeCRC;
	protected int preventDuplicatesRetentionPeriod;
	protected int preventDuplicatesCacheMaxSize;
	
	
	//for determining the duplicates:
	protected LinkedList<ReceivedMessageInfo> recentMessagesForDuplicatesDetection;
	protected HashSet<String> recentMessagesForDuplicatesDetectionSet;

	
	
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
	
		if (devLog.isDebugEnabled()) {
			devLog.debug("Initializing HyCubeReceivedMessageProcessorData.");
		}
		
		this.nodeAccessor = nodeAccessor;
		this.properties = properties;
		
		
		try {
			//load message types processed by this message processor:
			this.messageTypes = properties.getEnumListProperty(PROP_KEY_MESSAGE_TYPES, HyCubeMessageType.class);
			if (this.messageTypes == null) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_MESSAGE_TYPES) + ".");

			
			this.ackEnabled = (Boolean) properties.getProperty(PROP_KEY_ACK_ENABLED, MappedType.BOOLEAN);
			
			if (this.ackEnabled) {
				
				//get the ack extension:
				ackExtensionKey = properties.getProperty(PROP_KEY_ACK_EXTENSION_KEY);
				if (ackExtensionKey == null || ackExtensionKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_ACK_EXTENSION_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_ACK_EXTENSION_KEY));
				try {
					ackExtension = (HyCubeAckExtension) nodeAccessor.getExtension(ackExtensionKey);
					if (this.ackExtension == null) throw new InitializationException(InitializationException.Error.MISSING_EXTENSION_ERROR, this.ackExtensionKey, "The AckExtension is missing at the specified key: " + this.ackExtensionKey + ".");
				} catch (ClassCastException e) {
					throw new InitializationException(InitializationException.Error.MISSING_EXTENSION_ERROR, this.ackExtensionKey, "The AckExtension is missing at the specified key: " + this.ackExtensionKey + ".");
				}
				this.ackManager = ackExtension.getAckManager();
				
			}
			
			
			this.processDataMessageIfCannotRoute = (Boolean) properties.getProperty(PROP_KEY_PROCESS_DATA_MESSAGE_IF_CANNOT_ROUTE, MappedType.BOOLEAN);
			
			
			
			this.preventDuplicates = (Boolean) properties.getProperty(PROP_KEY_PREVENT_DUPLICATES, MappedType.BOOLEAN);
			
			this.preventAnonymousDuplicates = (Boolean) properties.getProperty(PROP_KEY_PREVENT_ANONYMOUS_DUPLICATES, MappedType.BOOLEAN);

			this.preventDuplicatesIncludeCRC = (Boolean) properties.getProperty(PROP_KEY_PREVENT_DUPLICATES_INCLUDE_CRC, MappedType.BOOLEAN);
			
			this.preventDuplicatesRetentionPeriod = (Integer) properties.getProperty(PROP_KEY_PREVENT_DUPLICATES_RETENTION_PERIOD, MappedType.INT);
			
			this.preventDuplicatesCacheMaxSize = (Integer) properties.getProperty(PROP_KEY_PREVENT_DUPLICATES_CACHE_MAX_SIZE, MappedType.INT);

			
			
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize received message processor instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		
		recentMessagesForDuplicatesDetection = new LinkedList<ReceivedMessageInfo>();
		recentMessagesForDuplicatesDetectionSet = new HashSet<String>(HashMapUtils.getHashMapCapacityForElementsNum(INITIAL_RECENT_MESSAGES_COLLECTION_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);

		
		
		
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
				case DATA:
					processDataMessage(msg, directSender);
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


	
	protected void processDataMessage(HyCubeMessage msg, NetworkNodePointer directSender) throws NetworkAdapterException, ProcessMessageException {
		
		//check if the message reached its destination
		
		if (msg.getRecipientId().equals(nodeAccessor.getNodeId())){
		
			//process
			processReceivedDataMessage(msg, directSender);
			return;
		
		}
		else {	// ! msg.getRecipientId().equals(nodeAccessor.getNodeId())
			
			boolean sent = nodeAccessor.sendMessage(new MessageSendProcessInfo(msg), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
			
			//this is the closest node reached, process the message or not, depending of the conf. parameter value
			if (!sent && processDataMessageIfCannotRoute) {
				processReceivedDataMessage(msg, directSender);
			}
			
			return;
			
		}
		
		
	}
	
	protected void processReceivedDataMessage(HyCubeMessage msg, NetworkNodePointer directSender) throws NetworkAdapterException, ProcessMessageException {
		
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Received DATA message #" + msg.getSerialNoAndSenderString() + ". Port number: " + msg.getDestinationPort() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Received DATA message #" + msg.getSerialNoAndSenderString() + ". Port number: " + msg.getDestinationPort() + ".");
		}
		
		
		
		//check if the message was already processed:
		if (preventDuplicates && ((!(msg.isAnonymousRoute())) || preventAnonymousDuplicates)) {
			if (msg.getRecipientId().equals(nodeAccessor.getNodeId())) {
				//prevent duplicates only if this is the final destination -> routing algorithm may include the same nodes several times (pmh)
				boolean isDuplicate = ! checkIfProcessAndCacheDuplicate(msg);
				if (isDuplicate) {
					
					if (devLog.isDebugEnabled()) {
						devLog.debug("Discarding message #" + msg.getSerialNoAndSenderString() + ". The message is a duplicate.");
					}
					if (msgLog.isInfoEnabled()) {
						msgLog.info("Discarding message #" + msg.getSerialNoAndSenderString() + ". The message is a duplicate.");
					}
					
					//the message is a duplicate, but the acknowledgment should be sent again (if the message is stored in the duplicates collection, it means that it was processed):
					ackManager.processDeliveredDataMessage(msg);
					
					return;
					
				}
			}
		}

		
		
		
		String senderNetworkAddressString = nodeAccessor.getNetworkAdapter().createNetworkNodePointer(msg.getSenderNetworkAddress()).getAddressString();
		
		ReceivedDataMessage dataMessage = new ReceivedDataMessage(msg.getSerialNo(), ((!msg.isRouteBack()) ? msg.getSenderId() : null), msg.getRecipientId(), ((!msg.isRouteBack()) ? senderNetworkAddressString : null), msg.getSourcePort(), msg.getDestinationPort(), msg.getData(), msg.isRegisterRoute(), msg.isRouteBack(), (msg.isRegisterRoute() ? msg.getRouteId() : 0), msg.isAnonymousRoute(), msg.isSecureRoutingApplied(), msg.isSkipRandomNumOfNodesApplied());

		
		
		BlockingQueue<Event> processMsgReceivedCallbackEventQueue = nodeAccessor.getEventQueue(EventCategory.processMsgReceivedCallbackEvent);
		
		boolean messageReceivedByNode = false;
		
		if (nodeAccessor.getNodeParameterSet().isUsePorts()) {
		
			Map<Short, LinkedBlockingQueue<ReceivedDataMessage>> appPortMessageInQueues = nodeAccessor.getAppPortMessageInQueues();
			Map<Short, MessageReceivedCallback> appPortMessageReceivedCallbacks = nodeAccessor.getAppPortMessageReceivedCallbacks();
			
			synchronized(nodeAccessor.getAppMessageInLock()) {
				if (appPortMessageInQueues.containsKey(msg.getDestinationPort())) {
					
					appPortMessageInQueues.get(msg.getDestinationPort()).add(dataMessage);
					MessageReceivedCallback callback = appPortMessageReceivedCallbacks.get(msg.getDestinationPort());
					if (callback != null) {
						//callback.notifyReceived(msg.getDestinationPort(), dataMessage);
						//don't call the user callback immediately, enqueue and let it be executed by the designated thread
						processMsgReceivedCallbackEventQueue.add(new MessageReceivedCallbackEvent(nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime(), nodeAccessor.getProcessEventProxy(), callback, dataMessage, msg.getDestinationPort()));
					}
					
					messageReceivedByNode = true;
					
				}
				else {
					//discard message, port not registered
					if (msgLog.isInfoEnabled()) {
						msgLog.info("Discarding DATA message #" + msg.getSerialNoAndSenderString() + ". Port number " + msg.getDestinationPort() + " not registered.");
					}
				}
			}
			
		}
		else {
			
			//add the message to the queue
			nodeAccessor.getAppMessageInQueue().add(dataMessage);
			MessageReceivedCallback appMessageReceivedCallback = nodeAccessor.getAppMessageReceivedCallback();
			if (appMessageReceivedCallback != null) {
				//don't call the user callback immediately, enqueue and let it be executed by the designated thread
				processMsgReceivedCallbackEventQueue.add(new MessageReceivedCallbackEvent(nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime(), nodeAccessor.getProcessEventProxy(), appMessageReceivedCallback, dataMessage));
				
				messageReceivedByNode = true;
				
			}
		}

		if (messageReceivedByNode) {
			ackManager.processDeliveredDataMessage(msg);
		}
		
	}

        

	
	
	protected boolean checkIfProcessAndCacheDuplicate(HyCubeMessage msg) {
		synchronized (recentMessagesForDuplicatesDetection) {
			long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
			purgeRecentMessagesForDuplicatesDetection(currTime);
			long senderIdHash = msg.getSenderId().calculateHash();
			String strIdHash = Long.toHexString(senderIdHash);
			String strMsgSerialNo = Integer.toHexString(msg.getSerialNo());
			String strRouteId = Integer.toHexString(msg.getRouteId());
			String strCrc = Integer.toHexString(msg.getCRC32());
			String str = null;
			if (preventDuplicatesIncludeCRC) {
				str = new StringBuilder(strIdHash.length() + strMsgSerialNo.length() + strRouteId.length() + strCrc.length() + 3).append(strIdHash).append('.').append(strMsgSerialNo).append('.').append(strRouteId).append(strCrc).toString();
			}
			else {
				str = new StringBuilder(strIdHash.length() + strMsgSerialNo.length() + strRouteId.length() + 2).append(strIdHash).append('.').append(strMsgSerialNo).append('.').append(strRouteId).toString();
			}
			if (recentMessagesForDuplicatesDetectionSet.contains(str)) {
				return false;
			}
			else {
				ReceivedMessageInfo rmi = new ReceivedMessageInfo(senderIdHash, msg.getSenderId(), msg.getSerialNo(), msg.getCRC32(), currTime, str);
				recentMessagesForDuplicatesDetection.add(rmi);
				recentMessagesForDuplicatesDetectionSet.add(str);
				while (recentMessagesForDuplicatesDetection.size() > preventDuplicatesCacheMaxSize) {
					ReceivedMessageInfo removed = recentMessagesForDuplicatesDetection.removeFirst();
					recentMessagesForDuplicatesDetectionSet.remove(removed.getStr());
				}
				
				return true;
			}
			
		}
	}

		


	protected void purgeRecentMessagesForDuplicatesDetection(long time) {
		
		ListIterator<ReceivedMessageInfo> iter = recentMessagesForDuplicatesDetection.listIterator();
		while (iter.hasNext()) {
			ReceivedMessageInfo rmi = iter.next();
			if (time >= rmi.getReceiveTime() + preventDuplicatesRetentionPeriod) {
				iter.remove();
				recentMessagesForDuplicatesDetectionSet.remove(rmi);
				
			}
			else {
				//the list is sorted by time -> no more elements have to be checked
				break;
			}
		}		
		
	}
    

	
	
	
	
	
	@Override
	public void discard() {	
	}
	
	
	
}
