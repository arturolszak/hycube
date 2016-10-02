package net.hycube.pastry.join.searchjoin;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map.Entry;
import java.util.Queue;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.HyCubeNodeId;
import net.hycube.core.HyCubeNodeIdFactory;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodeId;
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
import net.hycube.join.searchjoin.HyCubeSearchJoinManager;
import net.hycube.join.searchjoin.HyCubeSearchJoinMessageData;
import net.hycube.join.searchjoin.HyCubeSearchJoinNextHopSelectionParameters;
import net.hycube.join.searchjoin.HyCubeSearchJoinReplyMessageData;
import net.hycube.logging.LogHelper;
import net.hycube.maintenance.HyCubeRecoveryExtension;
import net.hycube.maintenance.HyCubeRecoveryManager;
import net.hycube.messaging.messages.HyCubeMessageFactory;
import net.hycube.messaging.messages.HyCubeMessageType;
import net.hycube.messaging.messages.Message;
import net.hycube.messaging.processing.MessageSendProcessInfo;
import net.hycube.messaging.processing.ProcessMessageException;
import net.hycube.nexthopselection.NextHopSelector;
import net.hycube.pastry.core.PastryRoutingTable;
import net.hycube.transport.NetworkAdapterException;
import net.hycube.transport.NetworkNodePointer;
import net.hycube.utils.HashMapUtils;
import net.hycube.utils.ObjectToStringConverter.MappedType;

/**
 * Join manager class.
 * @author Artur Olszak
 *
 */
public class PastrySearchJoinManager extends HyCubeSearchJoinManager {
	
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(PastrySearchJoinManager.class); 
	
	protected static final String PROP_KEY_NEXT_HOP_SELECTOR_KEY = "NextHopSelectorKey";
	protected static final String PROP_KEY_JOIN_CALLBACK_EVENT_KEY = "JoinCallbackEventKey";
	protected static final String PROP_KEY_JOIN_REQUEST_TIMEOUT_EVENT_KEY = "JoinRequestTimeoutEventKey";
	protected static final String PROP_KEY_JOIN_K = "JoinK";
	protected static final String PROP_KEY_JOIN_ALPHA = "JoinAlpha";
	protected static final String PROP_KEY_JOIN_BETA = "JoinBeta";
	protected static final String PROP_KEY_JOIN_GAMMA = "JoinGamma";
	protected static final String PROP_KEY_REQUEST_TIMEOUT = "JoinRequestTimeout";
	
	protected static final String PROP_KEY_SEND_CLOSEST_IN_INITIAL_JOIN_REPLY = "SendClosestInInitialJoinReply";
	protected static final String PROP_KEY_INCLUDE_LS_IN_INITIAL_JOIN_REPLY = "IncludeLSInInitialJoinReply";
	protected static final String PROP_KEY_INCLUDE_RT_IN_INITIAL_JOIN_REPLY = "IncludeRTInInitialJoinReply";
	protected static final String PROP_KEY_INCLUDE_SELF_IN_INITIAL_JOIN_REPLY = "IncludeSelfInInitialJoinReply";
	
	protected static final String PROP_KEY_MARK_INITIAL_JOIN_REPLY_SENDER_AS_RESPONDED = "MarkInitialJoinReplySenderAsResponded";
	
	protected static final String PROP_KEY_RECOVERY_LS_AFTER_JOIN = "RecoveryLSAfterJoin";
	protected static final String PROP_KEY_RECOVERY_AFTER_JOIN = "RecoveryAfterJoin";
	protected static final String PROP_KEY_RECOVERY_EXTENSION_KEY = "RecoveryExtensionKey";
	
	protected static final String PROP_KEY_DISCOVER_PUBLIC_NETWORK_ADDRESS = "DiscoverPublicNetworkAddress";
	
	
	protected static final boolean DEFAULT_IGNORE_TARGET_NODE = false;
	
	protected HyCubeNodeId nodeId;
	
	protected int nextJoinId;
	
	protected Object joinManagerLock = new Object();	//used top synchronize operations on the manager's data (like joins list)
	
	protected HashMap<Integer, PastrySearchJoinData> joinsData;
	
	protected short joinK;
	
	protected short joinAlpha;
	protected short joinBeta;
	protected short joinGamma;
	
	protected int requestTimeout;
	
	protected boolean sendClosestInInitialJoinReply;
	protected boolean includeLSInInitialJoinReply;
	protected boolean includeRTInInitialJoinReply;
	protected boolean includeSelfInInitialJoinReply;

	protected boolean markInitialJoinReplySenderAsResponded;
	
	
	protected boolean discoverPublicNetworkAddress;
	
	
	protected NodeAccessor nodeAccessor;
	protected NodeProperties properties;
	
	protected EventType joinCallbackEventType;
	
	protected String joinRequestTimeoutEventTypeKey;
	protected EventType joinRequestTimeoutEventType;
	protected ProcessEventProxy joinRequestTimeoutEventProxy;
	
	protected NextHopSelector nextHopSelector;
	protected NodeProperties nextHopSelectorProperties;
	
	protected boolean recoveryLSAfterJoin;
	protected boolean recoveryAfterJoin;
	protected HyCubeRecoveryManager recoveryManager;
	
	protected HyCubeMessageFactory messageFactory;
	protected HyCubeNodeIdFactory nodeIdFactory;
	protected PastryRoutingTable routingTable;
	
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		nextJoinId = Integer.MIN_VALUE;

		this.nodeAccessor = nodeAccessor;
		this.properties = properties;
		
		
		if (nodeAccessor.getNodeId() instanceof HyCubeNodeId) {
			this.nodeId = (HyCubeNodeId)nodeAccessor.getNodeId();
		}
		else {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize the join manager instance. The node id is expected to be an instance of: " + HyCubeNodeId.class.getName());
		}
		
		
		if (! (nodeAccessor.getMessageFactory() instanceof HyCubeMessageFactory)) throw new UnrecoverableRuntimeException("The message factory is expected to be an instance of: " + HyCubeMessageFactory.class.getName() + ".");
		messageFactory = (HyCubeMessageFactory) nodeAccessor.getMessageFactory();
		
		if (! (nodeAccessor.getNodeIdFactory() instanceof HyCubeNodeIdFactory)) throw new UnrecoverableRuntimeException("The node id factory is expected to be an instance of: " + HyCubeNodeIdFactory.class.getName() + ".");
		nodeIdFactory = (HyCubeNodeIdFactory) nodeAccessor.getNodeIdFactory();
		
		
		String nextHopSelectorKey = properties.getProperty(PROP_KEY_NEXT_HOP_SELECTOR_KEY);
		if (nextHopSelectorKey == null || nextHopSelectorKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_NEXT_HOP_SELECTOR_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_NEXT_HOP_SELECTOR_KEY));
		nextHopSelector = (NextHopSelector) nodeAccessor.getNextHopSelector(nextHopSelectorKey);
		
		if (! (nodeAccessor.getRoutingTable() instanceof PastryRoutingTable)) throw new UnrecoverableRuntimeException("The routing table is expected to be an instance of: " + PastryRoutingTable.class.getName() + ".");
		routingTable = (PastryRoutingTable) nodeAccessor.getRoutingTable();
		
		
		String recoveryExtensionKey = properties.getProperty(PROP_KEY_RECOVERY_EXTENSION_KEY);
		if (recoveryExtensionKey == null || recoveryExtensionKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_RECOVERY_EXTENSION_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_RECOVERY_EXTENSION_KEY));
		if (!(nodeAccessor.getExtension(recoveryExtensionKey) instanceof HyCubeRecoveryExtension)) throw new InitializationException(InitializationException.Error.MESSAGE_RECEIVER_INITIALIZATION_ERROR, null, "The recovery extension is expected to be an instance of " + HyCubeRecoveryExtension.class.getName());
		HyCubeRecoveryExtension recoveryExtension = (HyCubeRecoveryExtension) nodeAccessor.getExtension(recoveryExtensionKey);
		
		
		
		try {
			
			this.joinK = (Short) properties.getProperty(PROP_KEY_JOIN_K, MappedType.SHORT);
			
			this.joinAlpha = (Short) properties.getProperty(PROP_KEY_JOIN_ALPHA, MappedType.SHORT);
			this.joinBeta = (Short) properties.getProperty(PROP_KEY_JOIN_BETA, MappedType.SHORT);
			this.joinGamma = (Short) properties.getProperty(PROP_KEY_JOIN_GAMMA, MappedType.SHORT);
			
			this.requestTimeout = (Integer) properties.getProperty(PROP_KEY_REQUEST_TIMEOUT, MappedType.INT);
			
			
			
			this.sendClosestInInitialJoinReply = (Boolean) properties.getProperty(PROP_KEY_SEND_CLOSEST_IN_INITIAL_JOIN_REPLY, MappedType.BOOLEAN);
			this.includeLSInInitialJoinReply = (Boolean) properties.getProperty(PROP_KEY_INCLUDE_LS_IN_INITIAL_JOIN_REPLY, MappedType.BOOLEAN);
			this.includeRTInInitialJoinReply = (Boolean) properties.getProperty(PROP_KEY_INCLUDE_RT_IN_INITIAL_JOIN_REPLY, MappedType.BOOLEAN);
			this.includeSelfInInitialJoinReply = (Boolean) properties.getProperty(PROP_KEY_INCLUDE_SELF_IN_INITIAL_JOIN_REPLY, MappedType.BOOLEAN);

			this.markInitialJoinReplySenderAsResponded = (Boolean) properties.getProperty(PROP_KEY_MARK_INITIAL_JOIN_REPLY_SENDER_AS_RESPONDED, MappedType.BOOLEAN);
			
			
			this.discoverPublicNetworkAddress = (Boolean) properties.getProperty(PROP_KEY_DISCOVER_PUBLIC_NETWORK_ADDRESS, MappedType.BOOLEAN);
			
			this.recoveryLSAfterJoin = (Boolean) properties.getProperty(PROP_KEY_RECOVERY_LS_AFTER_JOIN, MappedType.BOOLEAN);
			this.recoveryAfterJoin = (Boolean) properties.getProperty(PROP_KEY_RECOVERY_AFTER_JOIN, MappedType.BOOLEAN);
			
			
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize the join manager instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		
		
		if (recoveryLSAfterJoin || recoveryAfterJoin) {
			recoveryManager = recoveryExtension.getRecoveryManager();
			if (recoveryManager == null) throw new InitializationException(InitializationException.Error.MESSAGE_RECEIVER_INITIALIZATION_ERROR, null, "The recovery manager is null");
		}
		
		
		
		joinsData = new HashMap<Integer, PastrySearchJoinData>(HashMapUtils.getHashMapCapacityForElementsNum(GlobalConstants.INITIAL_JOINS_DATA_COLLECTION_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		

		String joinCallbackEventTypeKey = properties.getProperty(PROP_KEY_JOIN_CALLBACK_EVENT_KEY);
		joinCallbackEventType = new EventType(EventCategory.extEvent, joinCallbackEventTypeKey);
		
		String joinRequestTimeoutEventTypeKey = properties.getProperty(PROP_KEY_JOIN_REQUEST_TIMEOUT_EVENT_KEY);
		joinRequestTimeoutEventType = new EventType(EventCategory.extEvent, joinRequestTimeoutEventTypeKey);
		
		joinRequestTimeoutEventProxy = new JoinRequestTimeoutEventProxy();
		
		
		
	}

	
	
	

	@Override
	public JoinCallback join(HyCubeNodeId joinNodeId, String[] bootstrapNodeAddresses, JoinCallback joinCallback, Object callbackArg, short alpha, short beta, short gamma, Object[] joinParameters) {

		boolean secureSearch = false;
		boolean skipRandomNextHops = false;
		
		if (joinParameters != null) {
			
			secureSearch = getJoinParameterSecureSearch(joinParameters);
			skipRandomNextHops = getJoinParameterSkipRandomNextHops(joinParameters);

		}
		
		synchronized(joinManagerLock) {
		
			//generate the join id
			int joinId = getNextJoinId();
			if (devLog.isDebugEnabled()) devLog.debug("Join ID: " + joinId);
			
			//if exists, discard previous join with the same id:
			PastrySearchJoinData oldJoinData = null;
			synchronized (joinManagerLock) {
				if (joinsData.containsKey(joinId)) {
					oldJoinData = joinsData.get(joinId);
					joinsData.remove(joinId);
				}
			}
			if (oldJoinData != null) oldJoinData.discard();
			
			
			//prepare the join structure
			PastrySearchJoinData joinData = new PastrySearchJoinData(joinId, bootstrapNodeAddresses, alpha, beta, gamma, secureSearch, skipRandomNextHops);
			
			joinsData.put(joinId, joinData);
			
			
			//initial phase:
			joinData.initialSearch = true;
			
			
			//registerJoinCallback
			joinData.setJoinCallback(joinCallback);
			joinData.setCallbackArg(callbackArg);
			
			
			
			//send the join request to bootstrap nodes:
			sendJoinRequestToBootstrapNodes(joinData);
			
	
			
		}
		
		return joinCallback;
		
	}
	
	
	
	
	protected void sendJoinRequestToBootstrapNodes(PastrySearchJoinData joinData) {

		//prepare the join parameters:
		
		//a temporary beta value (for initial request will be used: max(beta, alpha, gamma):
		short beta = joinData.beta;
		if (joinData.alpha > beta) beta = joinData.alpha;
		if (joinData.gamma > beta) beta = joinData.gamma;
		
		PastrySearchJoinNextHopSelectionParameters parameters = createPastryJoinParameters(createDefaultJoinParameters(joinData.beta));
		
		parameters.setSecureRoutingApplied(joinData.secureSearch);
		parameters.setSkipRandomNumOfNodesApplied(joinData.skipRandomNextHops);
		
		//set the initial request flag:
		parameters.initialRequest = true;
		
		boolean joinSent = false;
		
		if (joinData.bootstrapNodeAddresses != null && joinData.bootstrapNodeAddresses.length != 0) {
			for (String bootstrapNodeAddress : joinData.bootstrapNodeAddresses) {
			
				if (bootstrapNodeAddress == null || bootstrapNodeAddress.isEmpty()) continue;
				
				if (devLog.isDebugEnabled()) devLog.debug("Sending JOIN message to bootstrap node: " + bootstrapNodeAddress);
				
				NodePointer np = new NodePointer(nodeAccessor.getNetworkAdapter(), bootstrapNodeAddress, nodeId);	//self as node Id
				
				sendJoinRequest(joinData.joinId, np, nodeId, parameters);
	
				synchronized (joinData) {
					joinData.bootstrapNodes.add(bootstrapNodeAddress);
				}
				
				joinSent = true;
				
			}
		}
			
		if (joinSent == false) {
			
			//no join message sent - this node is the only node in the DHT
			
			//schedule the join callback event:
			enqueueCallbackEvent(joinData.joinCallback, joinData.callbackArg);
			
			//remove this join from the joins list:
			synchronized (joinManagerLock) {
				joinsData.remove(joinData.joinId);
			}
			
		}
		
		
	}




	protected int getNextJoinId() {
		synchronized (joinManagerLock) {
			int joinId = nextJoinId;
			nextJoinId++;
			return joinId;
		}
	}




	public void discard() {
		synchronized (joinManagerLock) {
			for (Entry<Integer, PastrySearchJoinData> entry : joinsData.entrySet()) {
				PastrySearchJoinData joinData = entry.getValue();
				enqueueCallbackEvent(joinData.joinCallback, joinData.callbackArg);
				joinData.discard();
			}
		}
		
	}


	@Override
	public EventType getJoinCallbackEventType() {
		return joinCallbackEventType;
	}

	
	public EventType getJoinRequestTimeoutEventType() {
		return joinCallbackEventType;
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
	
	
	
	
	public void processJoinRequest(NodePointer sender, int joinId, NodeId joinNodeId, HyCubeSearchJoinNextHopSelectionParameters joinParameters, NetworkNodePointer directSender, boolean discoverPublicNetworkAddress) {

		if (devLog.isDebugEnabled()) devLog.debug("Received join request from " + sender.getNodeId().toHexString() + ". Join id: " + joinId);
		
		//find best nodes according to the specified parameters

		//use the join parameters specified by the requestor:
		PastrySearchJoinNextHopSelectionParameters parameters = createPastryJoinParameters(joinParameters);
		
		short beta = joinParameters.getBeta();
		
		NodePointer[] nodesReturned = null;
		if (parameters.initialRequest) {
			
			if (devLog.isDebugEnabled()) devLog.debug("Processing initial join request.");
			
			if (sendClosestInInitialJoinReply) {
				if (includeSelfInInitialJoinReply) parameters.setIncludeSelf(true);
				nodesReturned = nextHopSelector.findNextHops(joinNodeId, parameters, beta);

			}
			else {
				
				int nodesReturnedNum = 0;

				if (includeSelfInInitialJoinReply) {
					nodesReturnedNum++;
				}
				
				if (includeLSInInitialJoinReply) {
					routingTable.getLsLock().readLock().lock();
					nodesReturnedNum += routingTable.getLsMap().size();
				}
				
				if (includeRTInInitialJoinReply) {
					routingTable.getRtLock().readLock().lock();
					nodesReturnedNum += routingTable.getRtMap().size();
				}
				
				HashMap<Long, NodePointer> nodesReturnedMap = new HashMap<Long, NodePointer>(HashMapUtils.getHashMapCapacityForElementsNum(nodesReturnedNum, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
				
				if (includeSelfInInitialJoinReply) {
					nodesReturnedMap.put(nodeAccessor.getNodePointer().getNodeIdHash(), nodeAccessor.getNodePointer());
				}
				
				if (includeLSInInitialJoinReply) {
					for (RoutingTableEntry rte : routingTable.getLsMap().values()) {
						nodesReturnedMap.put(rte.getNodeIdHash(), rte.getNode());
					}
					routingTable.getLsLock().readLock().unlock();
				}
				
				if (includeRTInInitialJoinReply) {
					for (RoutingTableEntry rte : routingTable.getRtMap().values()) {
						nodesReturnedMap.put(rte.getNodeIdHash(), rte.getNode());
					}
					routingTable.getRtLock().readLock().unlock();
					
				}
					
	
					
				nodesReturned = nodesReturnedMap.values().toArray(new NodePointer[0]);
	
									
			}
			
		}
		else {
			nodesReturned = nextHopSelector.findNextHops(joinNodeId, parameters, beta);
		}
				
			
		NodePointer joinNode;
		NetworkNodePointer joinNodePublicAddress;
		if (discoverPublicNetworkAddress) {
			joinNodePublicAddress = directSender;
			
			joinNode = new NodePointer();
			joinNode.setNodeId(sender.getNodeId());
			joinNode.setNetworkNodePointer(directSender);
			
		}
		else {
			joinNodePublicAddress = null;
			
			joinNode = sender;
			
		}
		

		
		
		//send response to the sender:
		sendJoinResponse(joinId, joinNode, joinNodePublicAddress, parameters, nodesReturned);
		
		
	}
	
	
	public void processJoinResponse(NodePointer sender, int joinId, NetworkNodePointer publicNetworkNodePoiner, HyCubeSearchJoinNextHopSelectionParameters joinParameters, NodePointer[] nodesFound) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Processing join response from node: " + sender.getNodeId().toHexString());
			StringBuilder sb = new StringBuilder();
			for (NodePointer np : nodesFound) {
				sb.append(np.getNodeId().toHexString() + ", ");
			}
			devLog.debug("Returned nodes: " + sb.toString());
		}
		
		PastrySearchJoinNextHopSelectionParameters parameters = createPastryJoinParameters(joinParameters);
		
		PastrySearchJoinData joinData;
		synchronized(joinManagerLock) {
			if (joinsData.containsKey(joinId)) {
				joinData = joinsData.get(joinId);
			}
			else {
				return;	//the join has already been terminated, or the join id is wrong
			}
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
			
			
			if (joinData.initialSearch) {
				
				String nodeAddress = sender.getNetworkNodePointer().getAddressString();
				if (joinData.bootstrapNodes.contains(nodeAddress)) joinData.bootstrapNodesResponded.add(nodeAddress);

				//use the parameters sent with the request and (the same) returned in the response, but update the beta parameter (a different beta might have been used for the initial join request):
				parameters.setBeta(joinData.beta);
				
				processNewNodes(joinData, nodesFound, sender, parameters);
				
				
			}
			else if ((! parameters.finalSearch) && (! joinData.finalSearch)) {
			
				if (joinData.nodesRequested.contains(sender.getNodeIdHash())) {
					
					joinData.nodesResponded.add(sender.getNodeIdHash());
					
					//update the parameters used by this node:
					joinData.pmhApplied.put(sender.getNodeIdHash(), parameters.isPMHApplied());
					
					processNewNodes(joinData, nodesFound, sender, parameters);
					
				}
				
			}
			if (parameters.finalSearch && joinData.finalSearch) {	//final join
				
				if (joinData.nodesRequestedFinal.contains(sender.getNodeIdHash())) {
					
					joinData.nodesRespondedFinal.add(sender.getNodeIdHash());
					
					//update the parameters used by this node:
					joinData.pmhApplied.put(sender.getNodeIdHash(), parameters.isPMHApplied());
					
					processNewNodes(joinData, nodesFound, sender, parameters);
					
					
				}
				
			}
			
			
			
			//and process the updated join data
			processJoin(joinData);
			
		}


	}
	
	
	
	protected void processNewNodes(PastrySearchJoinData joinData, NodePointer[] newNodes, NodePointer sender, PastrySearchJoinNextHopSelectionParameters joinParameters) {
		
		devLog.debug("CCC");
		
		//update the join data structure with the response
		//assume optimistically that the nodes are closer and already sorted and start inserting from the last element 
		for (int i = newNodes.length - 1; i >=0; i--) {
			NodePointer np = newNodes[i];
			
			if (joinData.closestNodesSet.contains(np.getNodeIdHash())) {
				//ignore that node, it is already in the closest nodes collection
				continue;
			}
			
			
			
			if (devLog.isDebugEnabled()) devLog.debug("Executing notify processor for node: " + np.getNodeId().toHexString());
			
			//process this node as a candidte for the routing tables:
			nodeAccessor.getNotifyProcessor().processNotify(np, nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime());
			
			
			if (! (np.getNodeId() instanceof HyCubeNodeId)) throw new IllegalArgumentException("The join node id should be an instance of: " + HyCubeNodeId.class.getName());
			double dist = HyCubeNodeId.calculateRingDistance((HyCubeNodeId)np.getNodeId(), nodeId);
			
			int insertIndex = 0;
			
			//ListIterator<NodePointer> nodeIter = joinData.closestNodes.listIterator();
			ListIterator<Double> distIter = joinData.distances.listIterator();
			
			//while (nodeIter.hasNext()) {
			while (distIter.hasNext()) {
				//NodePointer nextNode = nodeIter.next();
				double nextDist = distIter.next();
				if (dist < nextDist) break;
				else insertIndex++;
			}

			if (insertIndex == joinData.closestNodesStoredNum) {
				//skip this node - we store only closestNodesStoredNum closest nodes found
				continue;
			}
			else {
				if (joinData.closestNodes.size() == joinData.closestNodesStoredNum) {
					//the closest nodes collection already contains closestNodesStoredNum (number of closed nodes stored) nodes, so we should remove the last element
					NodePointer nodeRemoved = joinData.closestNodes.removeLast();
					joinData.distances.removeLast();
					joinData.closestNodesSet.remove(nodeRemoved.getNodeIdHash());
					joinData.pmhApplied.remove(nodeRemoved.getNodeIdHash());
				}
			}
			
			joinData.closestNodes.add(insertIndex, np);
			joinData.distances.add(insertIndex, dist);
			
			joinData.closestNodesSet.add(np.getNodeIdHash());
			
			//set the parameters for this virtual route
			joinData.pmhApplied.put(np.getNodeIdHash(), joinParameters.isPMHApplied());
			
			
			
			//if the response for an initial request contains the requested node, mark the sender as requested and responded:
			if (markInitialJoinReplySenderAsResponded) {
				if (joinParameters.initialRequest) {
					if (sender.getNetworkNodePointer().getAddressString().equals(np.getNetworkNodePointer().getAddressString())) {
						if (! joinParameters.finalSearch) {
							joinData.nodesRequested.add(np.getNodeIdHash());
							joinData.nodesResponded.add(np.getNodeIdHash());
						}
					}
				}
			}
			
			
		}
		
		if (devLog.isDebugEnabled()) {
			StringBuilder sb = new StringBuilder();
			for (NodePointer cn : joinData.closestNodes) {
				sb.append(cn.getNodeId().toHexString() + ", ");
			}
			devLog.debug("New closest nodes list: " + sb.toString());
		}
		
	}
	
	
	public void requestTimedOut(int joinId, String nodeAddressString, long nodeIdHash, boolean initialRequest, boolean finalSearch) {
		
		PastrySearchJoinData joinData;
		synchronized(joinManagerLock) {
			if (joinsData.containsKey(joinId)) {
				joinData = joinsData.get(joinId);
			}
			else return;	//the join has already been terminated
		}
		
		
		synchronized (joinData) {
			
			//mark the node (for which the response time limit was reached) as "responded"
			if (initialRequest) {
				if (joinData.bootstrapNodes.contains(nodeAddressString)) {
					joinData.bootstrapNodesResponded.add(nodeAddressString);
				}
			}
			else if (! finalSearch) {
				if (joinData.nodesRequested.contains(nodeIdHash)) {
					joinData.nodesResponded.add(nodeIdHash);
				}
			}
			else {	//final search
				if (joinData.nodesRequestedFinal.contains(nodeIdHash)) {
					joinData.nodesRespondedFinal.add(nodeIdHash);
				}
			}
		
			
			//and process the join data:
			processJoin(joinData);
			
		}
		
		
	}
	
	
	protected void processJoin(PastrySearchJoinData joinData) {
		
		//go through the nodes list and continue the join procedure
	
		synchronized (joinData) {
		

			if (joinData.initialSearch) {
				if (joinData.bootstrapNodes.size() == joinData.bootstrapNodesResponded.size()) {
					//all initial noders responded, set the initial search flag to false -> it will allow switching to the final phase of the search
					joinData.initialSearch = false;
					if (devLog.isDebugEnabled()) devLog.debug("Finished the initial join phase.");
				}
				
			}
			if ((! joinData.finalSearch) && (! joinData.initialSearch)) {

				boolean switchToFinalSearch = true;
				ListIterator<NodePointer> nodeIter = joinData.closestNodes.listIterator();
				while (nodeIter.hasNext() && nodeIter.nextIndex() < joinData.alpha) { 
					NodePointer np = nodeIter.next();
					if (! joinData.nodesResponded.contains(np.getNodeIdHash())) {
						switchToFinalSearch = false;
						break;
					}
				}

				if (switchToFinalSearch) {
					
					if (devLog.isDebugEnabled()) devLog.debug("Switching to final join phase.");
					
					//enable final search option:
					joinData.finalSearch = true;

					//set all the nodes that were already requested with the settings used for final search
					for (long nodeIdHash : joinData.closestNodesSet) {
						if (joinData.nodesRequested.contains(nodeIdHash) 
								&& joinData.pmhApplied.get(nodeIdHash) == true) {

							joinData.nodesRequestedFinal.add(nodeIdHash);
							joinData.nodesRespondedFinal.add(nodeIdHash);

						}

					}
				}


			}
			if (joinData.finalSearch) {
				
				boolean joinFinished = true;
				ListIterator<NodePointer> nodeIter = joinData.closestNodes.listIterator();
				while (nodeIter.hasNext() && nodeIter.nextIndex() < joinData.gamma) { 
					NodePointer np = nodeIter.next();
					if (! joinData.nodesRespondedFinal.contains(np.getNodeIdHash())) {
						joinFinished = false;
						break;
					}
				}
				
				if (joinFinished) {
					
					if (devLog.isDebugEnabled()) devLog.debug("Join finishede.");
					
					//all expected join reply messages received
	
					if (recoveryLSAfterJoin) {
						//schedule Recovery LS
						recoveryManager.recoverNS();
					}
					
					if (recoveryAfterJoin) {
						//enqueue the recovery event
						recoveryManager.recover();
					}
	
	
					//enqueue the callback event
					enqueueCallbackEvent(joinData.joinCallback, joinData.callbackArg);
	
	
					//remove this join from the joins list:
					synchronized (joinManagerLock) {
						joinsData.remove(joinData.joinId);
					}
				}

			}


			
			
			if (! joinData.finalSearch) {	//not the final search (also in the initial phase)
				//find closest nodes to which the request has not yet been sent (within first alpha nodes):
				LinkedList<NodePointer> reqNodes = new LinkedList<NodePointer>();
				ListIterator<NodePointer> nodeIter = joinData.closestNodes.listIterator();
				while (nodeIter.hasNext() && nodeIter.nextIndex() < joinData.alpha) {		//in first phase (finalJoin = false), a request should be sent to max. alpha closest nodes 
					NodePointer np = nodeIter.next();

					if (! joinData.nodesRequested.contains(np.getNodeIdHash())) {
						//this node is withing the first alpha closest nodes and has not yet been requested
						reqNodes.add(np);
					}
					
				}
				
				for (NodePointer reqNode : reqNodes) {
					
					//prepare the join parameters:
					PastrySearchJoinNextHopSelectionParameters parameters = createPastryJoinParameters(createDefaultJoinParameters(joinData.beta));
					
					//set the request parameters (the values for this virtual route):
					parameters.setPMHApplied(joinData.pmhApplied.get(reqNode.getNodeIdHash()));
					
					
					parameters.setSecureRoutingApplied(joinData.secureSearch);
					parameters.setSkipRandomNumOfNodesApplied(joinData.skipRandomNextHops);
					
					//send the join request to this node:
					sendJoinRequest(joinData.joinId, reqNode, nodeId, parameters);
					
					//mark this node as requested:
					joinData.nodesRequested.add(reqNode.getNodeIdHash());
					
				}
				
			}
			
			else {	//final join:
				//find first (closest) node to which the request has not yet been sent:
				LinkedList<NodePointer> reqNodes = new LinkedList<NodePointer>();
				ListIterator<NodePointer> nodeIter = joinData.closestNodes.listIterator();
				while (nodeIter.hasNext() && nodeIter.nextIndex() < joinData.gamma) {		//in second phase (finalJoin = true), a request should be sent to max. gamma closest nodes
					NodePointer np = nodeIter.next();
					if (! joinData.nodesRequestedFinal.contains(np.getNodeIdHash())) {
						//this node is withing the first alpha closest nodes and has not yet been requested
						reqNodes.add(np);
					}
				}
				
				for (NodePointer reqNode : reqNodes) {
					//prepare the join parameters:
					PastrySearchJoinNextHopSelectionParameters parameters = createPastryJoinParameters(createDefaultJoinParameters(joinData.beta));
					//set the request parameters (the values for this virtual route):
					parameters.setFinalSearch(true);
					parameters.setPMHApplied(true);
					
					parameters.setSecureRoutingApplied(joinData.secureSearch);
					parameters.setSkipRandomNumOfNodesApplied(joinData.skipRandomNextHops);
					
					
					//send the join request to this node:
					sendJoinRequest(joinData.joinId, reqNode, nodeId, parameters);
					
					//mark this node as requested:
					joinData.nodesRequestedFinal.add(reqNode.getNodeIdHash());
				}
			}
			
			
		}
		
		
	}
	
	
	protected void sendJoinRequest(int joinId, NodePointer recipient, HyCubeNodeId joinNodeId, PastrySearchJoinNextHopSelectionParameters joinParameters) {
		
		//prepare the message:
		
		int messageSerialNo = nodeAccessor.getNextMessageSerialNo();
		byte[] joinMessageData = (new HyCubeSearchJoinMessageData(joinId, joinNodeId, createHyCubeJoinParameters(joinParameters), discoverPublicNetworkAddress)).getBytes(nodeIdFactory.getDimensions(), nodeIdFactory.getDigitsCount());
		Message joinMessage = messageFactory.newMessage(messageSerialNo, nodeAccessor.getNodeId(), recipient.getNodeId(), nodeAccessor.getNetworkAdapter().getPublicAddressBytes(), HyCubeMessageType.JOIN, nodeAccessor.getNodeParameterSet().getMessageTTL(), (short)0, false, false, (short)0, (short)0, joinMessageData); 

		
		//send the message directly to the recipient:
		try {
			nodeAccessor.sendMessage(new MessageSendProcessInfo(joinMessage, recipient.getNetworkNodePointer(), false), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
		} catch (NetworkAdapterException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a join request to a node.", e);
		} catch (ProcessMessageException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a join request to a node.", e);
		}
		
		
		//schedule the request time out event
		scheduleRequestTimeout(joinId, recipient.getNetworkNodePointer().getAddressString(), recipient.getNodeIdHash(), joinParameters.initialRequest, joinParameters.finalSearch);
		
		
	}
	

	protected void sendJoinResponse(int joinId, NodePointer recipient, NetworkNodePointer joinNodePublicAddress, PastrySearchJoinNextHopSelectionParameters joinParameters, NodePointer[] nodesReturned) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Sending join response to node: " + recipient.getNodeId().toHexString());
			StringBuilder sb = new StringBuilder();
			for (NodePointer np : nodesReturned) {
				sb.append(np.getNodeId().toHexString() + ", ");
			}
			devLog.debug("Returning nodes: " + sb.toString());
		}
		
		
		//prepare the message:
		
		int messageSerialNo = nodeAccessor.getNextMessageSerialNo();
		byte[] joinReplyMessageData = (new HyCubeSearchJoinReplyMessageData(joinId, joinNodePublicAddress, createHyCubeJoinParameters(joinParameters), nodesReturned)).getBytes(nodeIdFactory.getDimensions(), nodeIdFactory.getDigitsCount()); 
		Message joinReplyMessage = messageFactory.newMessage(messageSerialNo, nodeAccessor.getNodeId(), recipient.getNodeId(), nodeAccessor.getNetworkAdapter().getPublicAddressBytes(), HyCubeMessageType.JOIN_REPLY, nodeAccessor.getNodeParameterSet().getMessageTTL(), (short)0, false, false, (short)0, (short)0, joinReplyMessageData); 

		
		//send the message directly to the recipient:
		try {
			nodeAccessor.sendMessage(new MessageSendProcessInfo(joinReplyMessage, recipient.getNetworkNodePointer(), false), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
		} catch (NetworkAdapterException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a join response to a node.", e);
		} catch (ProcessMessageException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a join response to a node.", e);
		}

		
	}
	
	
	protected void scheduleRequestTimeout(int joinId, String nodeAddressString, long nodeIdHash, boolean initialRequest, boolean finalSearch) {
		enqueueJoinRequestTimeoutEvent(joinId, nodeAddressString, nodeIdHash, initialRequest, finalSearch);
		
		
	}
	
	protected void enqueueJoinRequestTimeoutEvent(int joinId, String nodeAddressString, long nodeIdHash, boolean initialRequest, boolean finalSearch) {
		
		if (devLog.isDebugEnabled()) devLog.debug("Join " + joinId + " timed out.");
		
		//create the event:
		Event event = new PastrySearchJoinRequestTimeoutEvent(this, joinRequestTimeoutEventProxy, joinId, nodeAddressString, nodeIdHash, initialRequest, finalSearch);
		
		//schedule the event:
		Queue<Event> queue = nodeAccessor.getEventQueue(joinRequestTimeoutEventType);
		EventScheduler scheduler = nodeAccessor.getEventScheduler();
		scheduler.scheduleEventWithDelay(event, queue, requestTimeout);
		
		
	}
	
	
	public class JoinRequestTimeoutEventProxy implements ProcessEventProxy {
		@Override
		public void processEvent(Event event) throws EventProcessException {
			if (! (event instanceof PastrySearchJoinRequestTimeoutEvent)) throw new EventProcessException("Invalid event type scheduled to be processed by " + PastrySearchJoinManager.class.getName() + ". The expected event class: " + PastrySearchJoinRequestTimeoutEvent.class.getName() + ".");
			PastrySearchJoinRequestTimeoutEvent joinEvent = (PastrySearchJoinRequestTimeoutEvent)event;
			try {
				requestTimedOut(joinEvent.joinId, joinEvent.nodeAddressString, joinEvent.nodeIdHash, joinEvent.initialRequest, joinEvent.finalSearch);
			}
			catch (Exception e) {
				throw new EventProcessException("An exception was thrown while processing a join request timeout event.", e);
			}
		}
	}
	
	

	
	
	protected PastrySearchJoinNextHopSelectionParameters createPastryJoinParameters(HyCubeSearchJoinNextHopSelectionParameters hycubeJoinParameters) {
		
		PastrySearchJoinNextHopSelectionParameters parameters = new PastrySearchJoinNextHopSelectionParameters();
		
		parameters.setIncludeMoreDistantNodes(hycubeJoinParameters.isIncludeMoreDistantNodes());
		parameters.setPMHApplied(hycubeJoinParameters.isPMHApplied());
		parameters.setPreventPMH(hycubeJoinParameters.isPreventPMH());
		parameters.setSecureRoutingApplied(hycubeJoinParameters.isSecureRoutingApplied());
		parameters.setSkipRandomNumOfNodesApplied(hycubeJoinParameters.isSkipRandomNumOfNodesApplied());
		parameters.setSkipTargetNode(hycubeJoinParameters.isSkipTargetNode());
		parameters.setFinalSearch(hycubeJoinParameters.isFinalSearch());
		
		parameters.setBeta(hycubeJoinParameters.getBeta());
		parameters.setInitialRequest(hycubeJoinParameters.isInitialRequest());
		parameters.setFinalSearch(hycubeJoinParameters.isFinalSearch());
		
		return parameters;
		
	}
	
	
	
	protected HyCubeSearchJoinNextHopSelectionParameters createHyCubeJoinParameters(PastrySearchJoinNextHopSelectionParameters pastryJoinParameters) {
		
		HyCubeSearchJoinNextHopSelectionParameters parameters = new HyCubeSearchJoinNextHopSelectionParameters();
		
		parameters.setIncludeMoreDistantNodes(pastryJoinParameters.isIncludeMoreDistantNodes());
		parameters.setPMHApplied(pastryJoinParameters.isPMHApplied());
		parameters.setPreventPMH(pastryJoinParameters.isPreventPMH());
		parameters.setSecureRoutingApplied(pastryJoinParameters.isSecureRoutingApplied());
		parameters.setSkipRandomNumOfNodesApplied(pastryJoinParameters.isSkipRandomNumOfNodesApplied());
		parameters.setSkipTargetNode(pastryJoinParameters.isSkipTargetNode());
		parameters.setFinalSearch(pastryJoinParameters.isFinalSearch());
		
		parameters.setBeta(pastryJoinParameters.getBeta());
		parameters.setInitialRequest(pastryJoinParameters.isInitialRequest());
		parameters.setFinalSearch(pastryJoinParameters.isFinalSearch());
		
		return parameters;
		
	}


	
	
	public static Object[] createJoinParameters(Boolean secureSearch, Boolean skipRandomNextHops) {
		Object[] parameters = new Object[] {secureSearch, skipRandomNextHops};
		return parameters;
	}
	
	public static boolean getJoinParameterSecureSearch(Object[] parameters) {
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
