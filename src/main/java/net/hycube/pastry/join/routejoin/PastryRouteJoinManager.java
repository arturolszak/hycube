package net.hycube.pastry.join.routejoin;

import java.util.HashMap;
import java.util.Queue;

import net.hycube.common.EntryPoint;
import net.hycube.configuration.GlobalConstants;
import net.hycube.core.HyCubeNodeId;
import net.hycube.core.HyCubeNodeIdFactory;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodePointer;
import net.hycube.core.RoutingTableEntry;
import net.hycube.core.UnrecoverableRuntimeException;
import net.hycube.environment.NodeProperties;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventCategory;
import net.hycube.eventprocessing.EventProcessException;
import net.hycube.eventprocessing.EventScheduler;
import net.hycube.eventprocessing.EventType;
import net.hycube.eventprocessing.ProcessEventProxy;
import net.hycube.join.JoinCallback;
import net.hycube.join.JoinCallbackEvent;
import net.hycube.join.routejoin.HyCubeRouteJoinManager;
import net.hycube.join.routejoin.HyCubeRouteJoinMessageData;
import net.hycube.join.routejoin.HyCubeRouteJoinReplyMessageData;
import net.hycube.join.routejoin.HyCubeRouteJoinTimeoutEvent;
import net.hycube.logging.LogHelper;
import net.hycube.maintenance.HyCubeRecoveryExtension;
import net.hycube.maintenance.HyCubeRecoveryManager;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.HyCubeMessageFactory;
import net.hycube.messaging.messages.HyCubeMessageType;
import net.hycube.messaging.messages.Message;
import net.hycube.messaging.processing.MessageSendProcessInfo;
import net.hycube.messaging.processing.ProcessMessageException;
import net.hycube.nexthopselection.NextHopSelector;
import net.hycube.pastry.core.PastryRoutingTable;
import net.hycube.pastry.nexthopselection.PastryNextHopSelectionParameters;
import net.hycube.transport.NetworkAdapterException;
import net.hycube.transport.NetworkNodePointer;
import net.hycube.utils.HashMapUtils;
import net.hycube.utils.ObjectToStringConverter.MappedType;

/**
 * Join manager class.
 * @author Artur Olszak
 *
 */
public class PastryRouteJoinManager extends HyCubeRouteJoinManager {

	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(PastryRouteJoinManager.class);
	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
	
	protected static final String PROP_KEY_NEXT_HOP_SELECTOR_KEY = "NextHopSelectorKey";
	protected static final String PROP_KEY_PREFIX_MISMATCH_HEURISTIC_DISABLED_FOR_JOIN_MESSAGES = "PrefixMismatchHeuristicDisabledForJoinMessages";
	protected static final String PROP_KEY_JOIN_CALLBACK_EVENT_KEY = "JoinCallbackEventKey";
	protected static final String PROP_KEY_JOIN_TIMEOUT_EVENT_KEY = "JoinTimeoutEventKey";
	protected static final String PROP_KEY_WAIT_AFTER_FINAL_JOIN_REPLY_TIMEOUT_EVENT_KEY = "WaitAfterFinalJoinReplyTimeoutEventKey";
	protected static final String PROP_KEY_JOIN_TIMEOUT = "JoinTimeout";
	protected static final String PROP_KEY_WAIT_TIME_AFTER_FINAL_JOIN_REPLY = "WaitTimeAfterFinalJoinReply";
	protected static final String PROP_KEY_INCLUDE_LS_IN_JOIN_REPLY = "IncludeLSInJoinReply";
	protected static final String PROP_KEY_INCLUDE_RT_IN_JOIN_REPLY = "IncludeRTInJoinReply";
	protected static final String PROP_KEY_INCLUDE_SELF_IN_JOIN_REPLY = "IncludeSelfInJoinReply";
	protected static final String PROP_KEY_INCLUDE_LS_IN_JOIN_REPLY_FINAL = "IncludeLSInJoinReplyFinal";
	protected static final String PROP_KEY_INCLUDE_RT_IN_JOIN_REPLY_FINAL = "IncludeRTInJoinReplyFinal";
	protected static final String PROP_KEY_INCLUDE_SELF_IN_JOIN_REPLY_FINAL = "IncludeSelfInJoinReplyFinal";
	
	protected static final String PROP_KEY_RECOVERY_LS_AFTER_JOIN = "RecoveryLSAfterJoin";
	protected static final String PROP_KEY_RECOVERY_AFTER_JOIN = "RecoveryAfterJoin";
	protected static final String PROP_KEY_RECOVERY_EXTENSION_KEY = "RecoveryExtensionKey";
	
	protected static final String PROP_KEY_DISCOVER_PUBLIC_NETWORK_ADDRESS = "DiscoverPublicNetworkAddress";
	
	
	protected NodeAccessor nodeAccessor;
	protected NodeProperties properties;
	
	protected HyCubeNodeId nodeId;
	
	protected PastryRoutingTable routingTable;
	
	protected String joinCallbackEventTypeKey;
	protected String joinTimeoutEventKey;
	protected String waitAfterFinalJoinReplyTimeoutEventTypeKey;
	
	protected String nextHopSelectorKey;
	protected boolean pmhDisabledForJoinMessages;
	protected int joinTimeout;
	protected int waitTimeAfterFinalJoinReply;
	protected boolean includeLSInJoinReply;
	protected boolean includeRTInJoinReply;
	protected boolean includeSelfInJoinReply;
	protected boolean includeLSInJoinReplyFinal;
	protected boolean includeRTInJoinReplyFinal;
	protected boolean includeSelfInJoinReplyFinal;
	
	protected boolean discoverPublicNetworkAddress;
	
	
	protected EventType joinCallbackEventType;
	protected EventType joinTimeoutEventType;
	protected EventType waitAfterFinalJoinReplyTimeoutEventType;
	
	HyCubeRouteJoinTimeoutEventProxy joinTimeoutEventProxy;
	HyCubeRouteJoinWaitAfterFinalJoinReplyTimeoutEventProxy waitAfterFinalJoinReplyTimeoutEventProxy;
	
	protected NextHopSelector nextHopSelector;
	
	protected boolean recoveryLSAfterJoin;
	protected boolean recoveryAfterJoin;
	protected HyCubeRecoveryManager recoveryManager;
	
	protected HyCubeMessageFactory messageFactory;
	protected HyCubeNodeIdFactory nodeIdFactory;
	
	
	protected Object joinManagerLock = new Object();	//used top synchronize operations on the manager's data (like joines list)

	protected int nextJoinId;
	
	protected HashMap<Integer, PastryRouteJoinData> joinsData;
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		nextJoinId = Integer.MIN_VALUE;
		
		this.nodeAccessor = nodeAccessor;
		this.properties = properties;
	
		joinCallbackEventTypeKey = properties.getProperty(PROP_KEY_JOIN_CALLBACK_EVENT_KEY);
		joinCallbackEventType = new EventType(EventCategory.extEvent, joinCallbackEventTypeKey);
		
		joinTimeoutEventKey = properties.getProperty(PROP_KEY_JOIN_TIMEOUT_EVENT_KEY);
		joinTimeoutEventType = new EventType(EventCategory.extEvent, joinTimeoutEventKey);
		
		waitAfterFinalJoinReplyTimeoutEventTypeKey = properties.getProperty(PROP_KEY_WAIT_AFTER_FINAL_JOIN_REPLY_TIMEOUT_EVENT_KEY);
		waitAfterFinalJoinReplyTimeoutEventType = new EventType(EventCategory.extEvent, waitAfterFinalJoinReplyTimeoutEventTypeKey);
		
		
		nextHopSelectorKey = properties.getProperty(PROP_KEY_NEXT_HOP_SELECTOR_KEY);
		nextHopSelector = nodeAccessor.getNextHopSelector(nextHopSelectorKey);
		
		
		
		
		if (! (nodeAccessor.getNodeId() instanceof HyCubeNodeId)) throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, "The node id is expected to be an instance of:" + HyCubeNodeId.class.getName());
		this.nodeId = (HyCubeNodeId) nodeAccessor.getNodeId();
		
		if (! (nodeAccessor.getRoutingTable() instanceof PastryRoutingTable)) throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, "The routing table is expected to be an instance of:" + PastryRoutingTable.class.getName());
		routingTable = (PastryRoutingTable) nodeAccessor.getRoutingTable();
		
		
		String recoveryExtensionKey = properties.getProperty(PROP_KEY_RECOVERY_EXTENSION_KEY);
		if (recoveryExtensionKey == null || recoveryExtensionKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_RECOVERY_EXTENSION_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_RECOVERY_EXTENSION_KEY));
		if (!(nodeAccessor.getExtension(recoveryExtensionKey) instanceof HyCubeRecoveryExtension)) throw new InitializationException(InitializationException.Error.MESSAGE_RECEIVER_INITIALIZATION_ERROR, null, "The recovery extension is expected to be an instance of " + HyCubeRecoveryExtension.class.getName());
		HyCubeRecoveryExtension recoveryExtension = (HyCubeRecoveryExtension) nodeAccessor.getExtension(recoveryExtensionKey);
		
		
		
		try {

			pmhDisabledForJoinMessages = (Boolean) properties.getProperty(PROP_KEY_PREFIX_MISMATCH_HEURISTIC_DISABLED_FOR_JOIN_MESSAGES, MappedType.BOOLEAN);
			
			joinTimeout = (Integer) properties.getProperty(PROP_KEY_JOIN_TIMEOUT, MappedType.INT);
			
			waitTimeAfterFinalJoinReply = (Integer) properties.getProperty(PROP_KEY_WAIT_TIME_AFTER_FINAL_JOIN_REPLY, MappedType.INT);
			
			includeLSInJoinReply = (Boolean) properties.getProperty(PROP_KEY_INCLUDE_LS_IN_JOIN_REPLY, MappedType.BOOLEAN);
			includeRTInJoinReply = (Boolean) properties.getProperty(PROP_KEY_INCLUDE_RT_IN_JOIN_REPLY, MappedType.BOOLEAN);
			includeSelfInJoinReply = (Boolean) properties.getProperty(PROP_KEY_INCLUDE_SELF_IN_JOIN_REPLY, MappedType.BOOLEAN);
			includeLSInJoinReplyFinal = (Boolean) properties.getProperty(PROP_KEY_INCLUDE_LS_IN_JOIN_REPLY_FINAL, MappedType.BOOLEAN);
			includeRTInJoinReplyFinal = (Boolean) properties.getProperty(PROP_KEY_INCLUDE_RT_IN_JOIN_REPLY_FINAL, MappedType.BOOLEAN);
			includeSelfInJoinReplyFinal = (Boolean) properties.getProperty(PROP_KEY_INCLUDE_SELF_IN_JOIN_REPLY_FINAL, MappedType.BOOLEAN);

			discoverPublicNetworkAddress = (Boolean) properties.getProperty(PROP_KEY_DISCOVER_PUBLIC_NETWORK_ADDRESS, MappedType.BOOLEAN);
			
			recoveryLSAfterJoin = (Boolean) properties.getProperty(PROP_KEY_RECOVERY_LS_AFTER_JOIN, MappedType.BOOLEAN);
			recoveryAfterJoin = (Boolean) properties.getProperty(PROP_KEY_RECOVERY_AFTER_JOIN, MappedType.BOOLEAN);
			
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize the join manager instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		
		if (recoveryLSAfterJoin || recoveryAfterJoin) {
			recoveryManager = recoveryExtension.getRecoveryManager();
			if (recoveryManager == null) throw new InitializationException(InitializationException.Error.MESSAGE_RECEIVER_INITIALIZATION_ERROR, null, "The recovery manager is null");
		}
		
		
		if (! (nodeAccessor.getMessageFactory() instanceof HyCubeMessageFactory)) throw new UnrecoverableRuntimeException("The message factory is expected to be an instance of: " + HyCubeMessageFactory.class.getName() + ".");
		messageFactory = (HyCubeMessageFactory) nodeAccessor.getMessageFactory();
		
		if (! (nodeAccessor.getNodeIdFactory() instanceof HyCubeNodeIdFactory)) throw new UnrecoverableRuntimeException("The node id factory is expected to be an instance of: " + HyCubeNodeIdFactory.class.getName() + ".");
		nodeIdFactory = (HyCubeNodeIdFactory) nodeAccessor.getNodeIdFactory();
		
		
		joinTimeoutEventProxy = new HyCubeRouteJoinTimeoutEventProxy();
		waitAfterFinalJoinReplyTimeoutEventProxy = new HyCubeRouteJoinWaitAfterFinalJoinReplyTimeoutEventProxy();
		
		
		joinsData = new HashMap<Integer, PastryRouteJoinData>(HashMapUtils.getHashMapCapacityForElementsNum(GlobalConstants.INITIAL_JOINS_DATA_COLLECTION_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		
		
		
	}

	
	protected int getNextJoinId() {
		synchronized (joinManagerLock) {
			int joinId = nextJoinId;
			nextJoinId++;
			return joinId;
		}
	}
	
	
	
	@Override
	public JoinCallback join(String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, Object[] joinParameters) {
	
		boolean secureRouting = false;
		boolean skipRandomNextHops = false;
		
		if (joinParameters != null) {
			
			secureRouting = getJoinParameterSecureRouting(joinParameters);
			skipRandomNextHops = getJoinParameterSkipRandomNextHops(joinParameters);

		}
		
		synchronized (joinManagerLock) {
			
			//generate the join id
			int joinId = getNextJoinId();
			
			
			//if exists, discard previous join with the same id:
			PastryRouteJoinData oldJoinData = null;
			synchronized (joinManagerLock) {
				if (joinsData.containsKey(joinId)) {
					oldJoinData = joinsData.get(joinId);
					joinsData.remove(joinId);
				}
			}
			if (oldJoinData != null) oldJoinData.discard();
			
			
			
			//prepare the join structure
			PastryRouteJoinData joinData = new PastryRouteJoinData(joinId, bootstrapNodeAddress, secureRouting, skipRandomNextHops);
			
			joinsData.put(joinId, joinData);
			
			
			if (joinCallback != null) {
			
				//registerJoinCallback
				joinData.setJoinCallback(joinCallback);
				joinData.setCallbackArg(callbackArg);
							
			}

			if (bootstrapNodeAddress != null && bootstrapNodeAddress.length() != 0) {
				//send the join message to the bootstrapNode
				sendJoinRequest(joinId, nodeId, bootstrapNodeAddress, discoverPublicNetworkAddress, joinData.secureRouting, joinData.skipRandomNextHops);
			}
			else {
				//no join message sent - this node is the only node in the DHT
				
				//schedule the join callback event:
				enqueueCallbackEvent(joinCallback, callbackArg);
				
				//remove this join from the joins list:
				synchronized (joinManagerLock) {
					joinsData.remove(joinData.getJoinId());
				}

			}
			
			
			
			
		}
		
		
		return joinCallback;
		
	}

	
	@Override
	public EventType getJoinCallbackEventType() {
		return joinCallbackEventType;
	}

	
	public EventType getJoinTimeoutEventType() {
		return joinTimeoutEventType;
	}
	
	public EventType getWaitAfterFinalJoinReplyTimeoutEventType() {
		return waitAfterFinalJoinReplyTimeoutEventType;
	}

	
	
	protected void sendJoinRequest(int joinId, HyCubeNodeId joinNodeId, String bootstrapNodeAddress, boolean discoverPublicNetworkAddress, boolean secureRouting, boolean skipRandomNextHops) {

		//prepare the message:
		
		int messageSerialNo = nodeAccessor.getNextMessageSerialNo();
		byte[] joinMessageData = (new HyCubeRouteJoinMessageData(joinId, joinNodeId, discoverPublicNetworkAddress)).getBytes(nodeIdFactory.getDimensions(), nodeIdFactory.getDigitsCount());
		HyCubeMessage joinMessage = messageFactory.newMessage(messageSerialNo, nodeAccessor.getNodeId(), joinNodeId, nodeAccessor.getNetworkAdapter().getPublicAddressBytes(), HyCubeMessageType.JOIN, nodeAccessor.getNodeParameterSet().getMessageTTL(), (short)0, false, false, (short)0, (short)0, joinMessageData);
		
		//this message is being routed (the join manager is also a routing manager):
		joinMessage.setTtl((short) (joinMessage.getTtl() - 1));
		joinMessage.setHopCount((short) (joinMessage.getHopCount() + 1));

		
		//set the default routing paremeters:
		joinMessage.setPMHApplied(false);
		joinMessage.setSteinhausPoint(null);
		
		joinMessage.setSecureRoutingApplied(secureRouting);
		joinMessage.setSkipRandomNumOfNodesApplied(skipRandomNextHops);
		
		
		//send the message directly to the recipient:
		try {
			nodeAccessor.sendMessage(new MessageSendProcessInfo(joinMessage, nodeAccessor.getNetworkAdapter().createNetworkNodePointer(bootstrapNodeAddress), false), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
		} catch (NetworkAdapterException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a join request to a node.", e);
		} catch (ProcessMessageException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a join request to a node.", e);
		}
		
		
		//schedule the request time out event
		scheduleJoinTimeout(joinId);
		
		
	}
	
	
	protected void sendJoinReply(int joinId, NodePointer recipient, NetworkNodePointer requestor, NodePointer[] nodesReturned, boolean finalJoinReply) {
		
		//prepare the message:
		
		int messageSerialNo = nodeAccessor.getNextMessageSerialNo();
		byte[] joinReplyMessageData = (new HyCubeRouteJoinReplyMessageData(joinId, requestor, nodesReturned, finalJoinReply)).getBytes(nodeIdFactory.getDimensions(), nodeIdFactory.getDigitsCount());
		Message joinMessage = messageFactory.newMessage(messageSerialNo, nodeAccessor.getNodeId(), recipient.getNodeId(), nodeAccessor.getNetworkAdapter().getPublicAddressBytes(), HyCubeMessageType.JOIN_REPLY, nodeAccessor.getNodeParameterSet().getMessageTTL(), (short)0, false, false, (short)0, (short)0, joinReplyMessageData); 

		
		//send the message directly to the recipient:
		try {
			nodeAccessor.sendMessage(new MessageSendProcessInfo(joinMessage, recipient.getNetworkNodePointer(), false), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
		} catch (NetworkAdapterException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a join request to a node.", e);
		} catch (ProcessMessageException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a join request to a node.", e);
		}
		
		
	}
	
	
	
	protected void scheduleJoinTimeout(int joinId) {
		//create the event:
		Event event = new PastryRouteJoinTimeoutEvent(this, joinTimeoutEventProxy, joinId);
				
		//schedule the event:
		Queue<Event> queue = nodeAccessor.getEventQueue(joinTimeoutEventType);
		EventScheduler scheduler = nodeAccessor.getEventScheduler();
		scheduler.scheduleEventWithDelay(event, queue, joinTimeout);
		
	}

	
	public class HyCubeRouteJoinTimeoutEventProxy implements ProcessEventProxy {
		@Override
		public void processEvent(Event event) throws EventProcessException {
			if (! (event instanceof HyCubeRouteJoinTimeoutEvent)) throw new EventProcessException("Invalid event type scheduled to be processed by " + getClass().getName() + ". The expected event class: " + HyCubeRouteJoinTimeoutEvent.class.getName() + ".");
			PastryRouteJoinTimeoutEvent joinEvent = (PastryRouteJoinTimeoutEvent)event;
			try {
				joinTimedOut(joinEvent.joinId);
			}
			catch (Exception e) {
				throw new EventProcessException("An exception was thrown while processing a join request timeout event.", e);
			}
		}
	}
	
	
	protected void scheduleWaitAfterFinalReplyTimeout(int joinId) {
		
		//create the event:
		Event event = new PastryRouteJoinWaitAfterFinalJoinReplyTimeoutEvent(this, waitAfterFinalJoinReplyTimeoutEventProxy, joinId);
						
		//schedule the event:
		Queue<Event> queue = nodeAccessor.getEventQueue(waitAfterFinalJoinReplyTimeoutEventType);
		EventScheduler scheduler = nodeAccessor.getEventScheduler();
		scheduler.scheduleEventWithDelay(event, queue, waitTimeAfterFinalJoinReply);
		
	}
	
	
	public class HyCubeRouteJoinWaitAfterFinalJoinReplyTimeoutEventProxy implements ProcessEventProxy {
		@Override
		public void processEvent(Event event) throws EventProcessException {
			if (! (event instanceof PastryRouteJoinWaitAfterFinalJoinReplyTimeoutEvent)) throw new EventProcessException("Invalid event type scheduled to be processed by " + getClass().getName() + ". The expected event class: " + PastryRouteJoinWaitAfterFinalJoinReplyTimeoutEvent.class.getName() + ".");
			PastryRouteJoinWaitAfterFinalJoinReplyTimeoutEvent joinEvent = (PastryRouteJoinWaitAfterFinalJoinReplyTimeoutEvent)event;
			waitAfterFinalReplyTimedOut(joinEvent.joinId);
		}
	}
	
	
	
	protected void joinTimedOut(int joinId) {

		synchronized (joinManagerLock) {
			if (joinsData.containsKey(joinId)) {
				
				PastryRouteJoinData joinData = joinsData.get(joinId);
				joinsData.remove(joinId);
				
				if (recoveryLSAfterJoin) {
					//schedule recovery LS
					recoveryManager.recoverNS();
				}
				
				if (recoveryAfterJoin) {
					//scheduleRecovery
					recoveryManager.recover();
				}
				
				
				//schedule the join callback
				enqueueCallbackEvent(joinData.joinCallback, joinData.callbackArg);
				
				
			}
			//else the join procedure was already finished - do nothing
		}
		
	}
	
	
	protected void waitAfterFinalReplyTimedOut(int joinId) {
		
		synchronized (joinManagerLock) {
			if (joinsData.containsKey(joinId)) {
				PastryRouteJoinData joinData = joinsData.get(joinId);
				joinsData.remove(joinId);
				
				if (recoveryLSAfterJoin) {
					//schedule recovery LS
					recoveryManager.recoverNS();
				}
				
				if (recoveryAfterJoin) {
					//scheduleRecovery
					recoveryManager.recover();
				}
				
				
				//schedule the join callback
				enqueueCallbackEvent(joinData.joinCallback, joinData.callbackArg);
				
				
				
			}
			//else the join procedure was already finished
		}
		
	}
	
	
	protected void enqueueCallbackEvent(JoinCallback joinCallback, Object callbackArg) {
		
		if (joinCallback == null) return;
		
		//create the event
		Event event = new JoinCallbackEvent(this, joinCallback, callbackArg);
		
		//insert to the appropriate event queue
		try {
			nodeAccessor.getEventQueue(joinCallbackEventType).put(event);
		} catch (InterruptedException e) {
			//this should never happen
			throw new UnrecoverableRuntimeException("An exception was thrown while inserting an event to an event queue.");
		}

	}
	

	public void processJoinRequest(NodePointer sender, int joinId, HyCubeNodeId joinNodeId, HyCubeMessage joinMsg, NetworkNodePointer directSender, boolean discoverPublicNetworkAddress) {
		
		boolean finalJoinReply = false;

		
		PastryNextHopSelectionParameters parameters = new PastryNextHopSelectionParameters();
		parameters.setPMHApplied(joinMsg.isPMHApplied());
		parameters.setSkipRandomNumOfNodesApplied(joinMsg.isSkipRandomNumOfNodesApplied());
		parameters.setSecureRoutingApplied(joinMsg.isSecureRoutingApplied());
		parameters.setIncludeMoreDistantNodes(false);
		parameters.setSkipTargetNode(true);
		
		if (pmhDisabledForJoinMessages) parameters.setPreventPMH(true);
		else parameters.setPreventPMH(false);

		//find next hop for the join message
		NodePointer nextHop = nextHopSelector.findNextHop(joinMsg.getRecipientId(), parameters);
		
		if (nextHop == null || joinMsg.getTtl() == 0) finalJoinReply = true;
		
		NodePointer[] nodesReturned = null;
		
		
		
		
		int nodesReturnedNum = 0;

		if (includeSelfInJoinReply || (finalJoinReply && includeSelfInJoinReplyFinal)) {
			nodesReturnedNum++;
		}
		
		if (includeLSInJoinReply || (finalJoinReply && includeLSInJoinReplyFinal)) {
			routingTable.getLsLock().readLock().lock();
			nodesReturnedNum += routingTable.getLsMap().size();
		}
		
		if (includeRTInJoinReply || (finalJoinReply && includeRTInJoinReplyFinal)) {
			routingTable.getRtLock().readLock().lock();
			nodesReturnedNum += routingTable.getRtMap().size();
		}
		
		HashMap<Long, NodePointer> nodesReturnedMap = new HashMap<Long, NodePointer>(HashMapUtils.getHashMapCapacityForElementsNum(nodesReturnedNum, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		
		if (includeSelfInJoinReply || (finalJoinReply && includeSelfInJoinReplyFinal)) {
			nodesReturnedMap.put(nodeAccessor.getNodePointer().getNodeIdHash(), nodeAccessor.getNodePointer());
		}
		
		if (includeLSInJoinReply || (finalJoinReply && includeLSInJoinReplyFinal)) {
			for (RoutingTableEntry rte : routingTable.getLsMap().values()) {
				nodesReturnedMap.put(rte.getNodeIdHash(), rte.getNode());
			}
			routingTable.getLsLock().readLock().unlock();
		}
		
		if (includeRTInJoinReply || (finalJoinReply && includeRTInJoinReplyFinal)) {
			for (RoutingTableEntry rte : routingTable.getRtMap().values()) {
				nodesReturnedMap.put(rte.getNodeIdHash(), rte.getNode());
			}
			routingTable.getRtLock().readLock().unlock();
			
		}
			

			
		nodesReturned = nodesReturnedMap.values().toArray(new NodePointer[0]);
		
		
		
		
		NodePointer joinNode;
		NetworkNodePointer joinNodePublicAddress;
		if (discoverPublicNetworkAddress) {
			//return the direct sender node address only if the request was sent directly from the initiating node:
			if (joinMsg.getHopCount() == 1) {
				joinNodePublicAddress = directSender;
				
				joinNode = new NodePointer();
				joinNode.setNodeId(sender.getNodeId());
				joinNode.setNetworkNodePointer(directSender);
				
			}
			else {
				joinNodePublicAddress = null;
				
				joinNode = sender;
				
			}
		}
		else {
			joinNodePublicAddress = null;
			
			joinNode = sender;
			
		}
		
		
		
		
		//send the reply to the requesting node:
		
		sendJoinReply(joinId, joinNode, joinNodePublicAddress, nodesReturned, finalJoinReply);
		
		
		//if next hop was found, route the message:
		if (nextHop != null && joinMsg.getTtl() > 0) {
			try {
				
				if (devLog.isDebugEnabled()) {
					devLog.debug("Routing message #" + joinMsg.getSerialNoAndSenderString() + ".");
				}
				if (msgLog.isInfoEnabled()) {
					msgLog.info("Routing message #" + joinMsg.getSerialNoAndSenderString() + ".");
				}
				
				if (joinMsg.getTtl() == 0) {
					if (devLog.isDebugEnabled()) {
						devLog.debug("Message #" + joinMsg.getSerialNoAndSenderString() + " dropped. TTL = 0.");
					}
					if (msgLog.isInfoEnabled()) {
						msgLog.info("Message #" + joinMsg.getSerialNoAndSenderString() + " dropped. TTL = 0.");
					}
				}
				
				//update the join node - to the one with its real network address
				joinMsg.setSenderNetworkAddress(joinNode.getNetworkNodePointer().getAddressBytes());
				
				
				joinMsg.setPMHApplied(parameters.isPMHApplied());
				joinMsg.setSkipRandomNumOfNodesApplied(parameters.isSkipRandomNumOfNodesApplied());
				joinMsg.setSecureRoutingApplied(parameters.isSecureRoutingApplied());
				joinMsg.setTtl((short) (joinMsg.getTtl() - 1));
				joinMsg.setHopCount((short) (joinMsg.getHopCount() + 1));

								
				MessageSendProcessInfo info = new MessageSendProcessInfo(joinMsg, nextHop.getNetworkNodePointer());
				
				if (devLog.isDebugEnabled()) {
					devLog.debug("Sending message #" + joinMsg.getSerialNoAndSenderString() + " to " + info.getDirectRecipient().getAddressString() + ".");
				}
				if (msgLog.isInfoEnabled()) {
					msgLog.debug("Sending message #" + joinMsg.getSerialNoAndSenderString() + " to " + info.getDirectRecipient().getAddressString() + ".");
				}
				
				nodeAccessor.sendMessageToNode(info, GlobalConstants.WAIT_ON_BKG_MSG_SEND);
				
			} catch (NetworkAdapterException e) {
				throw new UnrecoverableRuntimeException("An exception has been thrown while trying to route a join message.", e);
			} catch (ProcessMessageException e) {
				throw new UnrecoverableRuntimeException("An exception has been thrown while trying to route a join message.", e);
			}
		}
		
		
	}

	
	public void processJoinResponse(NodePointer sender, int joinId,	NetworkNodePointer publicNetworkNodePoiner, NodePointer[] nodesFound, boolean finalJoinReply) {
		
		PastryRouteJoinData joinData;
		synchronized(joinManagerLock) {
			if (joinsData.containsKey(joinId)) {
				joinData = joinsData.get(joinId);
			}
			else return;	//the join has already been terminated, or the join id is wrong
		}
		
		
		synchronized (joinData) {
			if (discoverPublicNetworkAddress) {
				if (! joinData.publicNetworkAddressDiscovered) {
					if (publicNetworkNodePoiner != null) {
						nodeAccessor.getNetworkAdapter().setPublicAddress(publicNetworkNodePoiner);
						joinData.publicNetworkAddressDiscovered = true;
					}
				}
			}	
		}
		
		
		//process the nodes found:
		for (NodePointer np : nodesFound) {
			nodeAccessor.getNotifyProcessor().processNotify(np, nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime());
		}
		
		if (finalJoinReply) {
			//schedule the wait after final join:
			scheduleWaitAfterFinalReplyTimeout(joinId);
			
		}

		
		
	}


	@Override
	public EntryPoint getEntryPoint() {
		return null;
	}
	
	
	
	@Override
	public void discard() {	
	}
	
	
	
	
	public static Object[] createJoinParameters(Boolean secureRouting, Boolean skipRandomNextHops) {
		Object[] parameters = new Object[] {secureRouting, skipRandomNextHops};
		return parameters;
	}
	
	public static boolean getJoinParameterSecureRouting(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 0) || (!(parameters[0] instanceof Boolean))) return false;
		return (Boolean)parameters[0];
	}
	
	public static boolean getJoinParameterSkipRandomNextHops(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 1) || (!(parameters[1] instanceof Boolean))) return false;
		return (Boolean)parameters[1];
	}
	
	
	
}
