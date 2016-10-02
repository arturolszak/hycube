package net.hycube.join.searchjoin;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map.Entry;
import java.util.Queue;

import net.hycube.common.EntryPoint;
import net.hycube.configuration.GlobalConstants;
import net.hycube.core.HyCubeNodeId;
import net.hycube.core.HyCubeNodeIdFactory;
import net.hycube.core.HyCubeRoutingTable;
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
import net.hycube.join.JoinManager;
import net.hycube.logging.LogHelper;
import net.hycube.maintenance.HyCubeRecoveryExtension;
import net.hycube.maintenance.HyCubeRecoveryManager;
import net.hycube.messaging.messages.HyCubeMessageFactory;
import net.hycube.messaging.messages.HyCubeMessageType;
import net.hycube.messaging.messages.Message;
import net.hycube.messaging.processing.MessageSendProcessInfo;
import net.hycube.messaging.processing.ProcessMessageException;
import net.hycube.metric.Metric;
import net.hycube.nexthopselection.NextHopSelector;
import net.hycube.transport.NetworkAdapterException;
import net.hycube.transport.NetworkNodePointer;
import net.hycube.utils.HashMapUtils;
import net.hycube.utils.ObjectToStringConverter.MappedType;

/**
 * Join manager class.
 * @author Artur Olszak
 *
 */
public class HyCubeSearchJoinManager implements JoinManager {
	
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeSearchJoinManager.class); 
	
	protected static final String PROP_KEY_NEXT_HOP_SELECTOR_KEY = "NextHopSelectorKey";
	protected static final String PROP_KEY_JOIN_CALLBACK_EVENT_KEY = "JoinCallbackEventKey";
	protected static final String PROP_KEY_JOIN_REQUEST_TIMEOUT_EVENT_KEY = "JoinRequestTimeoutEventKey";
	protected static final String PROP_KEY_JOIN_ALPHA = "JoinAlpha";
	protected static final String PROP_KEY_JOIN_BETA = "JoinBeta";
	protected static final String PROP_KEY_JOIN_GAMMA = "JoinGamma";
	protected static final String PROP_KEY_METRIC = "Metric";
	protected static final String PROP_KEY_USE_STEINHAUS_TRANSFORM = "UseSteinhausTransform";
	protected static final String PROP_KEY_REQUEST_TIMEOUT = "JoinRequestTimeout";
	
	protected static final String PROP_KEY_SEND_CLOSEST_IN_INITIAL_JOIN_REPLY = "SendClosestInInitialJoinReply";
	protected static final String PROP_KEY_INCLUDE_NS_IN_INITIAL_JOIN_REPLY = "IncludeNSInInitialJoinReply";
	protected static final String PROP_KEY_INCLUDE_RT_IN_INITIAL_JOIN_REPLY = "IncludeRTInInitialJoinReply";
	protected static final String PROP_KEY_INCLUDE_SELF_IN_INITIAL_JOIN_REPLY = "IncludeSelfInInitialJoinReply";
	
	protected static final String PROP_KEY_MARK_INITIAL_JOIN_REPLY_SENDER_AS_RESPONDED = "MarkInitialJoinReplySenderAsResponded";
	
	protected static final String PROP_KEY_RECOVERY_NS_AFTER_JOIN = "RecoveryNSAfterJoin";
	protected static final String PROP_KEY_RECOVERY_AFTER_JOIN = "RecoveryAfterJoin";
	protected static final String PROP_KEY_RECOVERY_EXTENSION_KEY = "RecoveryExtensionKey";
	
	protected static final String PROP_KEY_DISCOVER_PUBLIC_NETWORK_ADDRESS = "DiscoverPublicNetworkAddress";
	
	
	protected static final boolean DEFAULT_IGNORE_TARGET_NODE = false;
	
	protected HyCubeNodeId nodeId;
	
	protected int nextJoinId;
	
	protected Object joinManagerLock = new Object();	//used top synchronize operations on the manager's data (like joins list)
	
	protected HashMap<Integer, HyCubeSearchJoinData> joinsData;
	
	
	protected short joinAlpha;
	protected short joinBeta;
	protected short joinGamma;
	
	protected Metric metric;
	protected boolean useSteinhausTransform;
	
	protected int requestTimeout;
	
	protected boolean sendClosestInInitialJoinReply;
	protected boolean includeNSInInitialJoinReply;
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
	
	protected boolean recoveryNSAfterJoin;
	protected boolean recoveryAfterJoin;
	protected HyCubeRecoveryManager recoveryManager;
	
	protected HyCubeMessageFactory messageFactory;
	protected HyCubeNodeIdFactory nodeIdFactory;
	protected HyCubeRoutingTable routingTable;
	
	
	
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
		
		if (! (nodeAccessor.getRoutingTable() instanceof HyCubeRoutingTable)) throw new UnrecoverableRuntimeException("The routing table is expected to be an instance of: " + HyCubeRoutingTable.class.getName() + ".");
		routingTable = (HyCubeRoutingTable) nodeAccessor.getRoutingTable();
		
		
		String recoveryExtensionKey = properties.getProperty(PROP_KEY_RECOVERY_EXTENSION_KEY);
		if (recoveryExtensionKey == null || recoveryExtensionKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_RECOVERY_EXTENSION_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_RECOVERY_EXTENSION_KEY));
		if (!(nodeAccessor.getExtension(recoveryExtensionKey) instanceof HyCubeRecoveryExtension)) throw new InitializationException(InitializationException.Error.MESSAGE_RECEIVER_INITIALIZATION_ERROR, null, "The recovery extension is expected to be an instance of " + HyCubeRecoveryExtension.class.getName());
		HyCubeRecoveryExtension recoveryExtension = (HyCubeRecoveryExtension) nodeAccessor.getExtension(recoveryExtensionKey);
		
		
		
		try {
			
			this.joinAlpha = (Short) properties.getProperty(PROP_KEY_JOIN_ALPHA, MappedType.SHORT);
			this.joinBeta = (Short) properties.getProperty(PROP_KEY_JOIN_BETA, MappedType.SHORT);
			this.joinGamma = (Short) properties.getProperty(PROP_KEY_JOIN_GAMMA, MappedType.SHORT);
			
			this.metric = (Metric) properties.getEnumProperty(PROP_KEY_METRIC, Metric.class);
			
			this.useSteinhausTransform = (Boolean) properties.getProperty(PROP_KEY_USE_STEINHAUS_TRANSFORM, MappedType.BOOLEAN);
			
			this.requestTimeout = (Integer) properties.getProperty(PROP_KEY_REQUEST_TIMEOUT, MappedType.INT);
			
			
			
			this.sendClosestInInitialJoinReply = (Boolean) properties.getProperty(PROP_KEY_SEND_CLOSEST_IN_INITIAL_JOIN_REPLY, MappedType.BOOLEAN);
			this.includeNSInInitialJoinReply = (Boolean) properties.getProperty(PROP_KEY_INCLUDE_NS_IN_INITIAL_JOIN_REPLY, MappedType.BOOLEAN);
			this.includeRTInInitialJoinReply = (Boolean) properties.getProperty(PROP_KEY_INCLUDE_RT_IN_INITIAL_JOIN_REPLY, MappedType.BOOLEAN);
			this.includeSelfInInitialJoinReply = (Boolean) properties.getProperty(PROP_KEY_INCLUDE_SELF_IN_INITIAL_JOIN_REPLY, MappedType.BOOLEAN);

			this.markInitialJoinReplySenderAsResponded = (Boolean) properties.getProperty(PROP_KEY_MARK_INITIAL_JOIN_REPLY_SENDER_AS_RESPONDED, MappedType.BOOLEAN);
			
			
			this.discoverPublicNetworkAddress = (Boolean) properties.getProperty(PROP_KEY_DISCOVER_PUBLIC_NETWORK_ADDRESS, MappedType.BOOLEAN);
			
			this.recoveryNSAfterJoin = (Boolean) properties.getProperty(PROP_KEY_RECOVERY_NS_AFTER_JOIN, MappedType.BOOLEAN);
			this.recoveryAfterJoin = (Boolean) properties.getProperty(PROP_KEY_RECOVERY_AFTER_JOIN, MappedType.BOOLEAN);
			
			
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize the join manager instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		
		
		if (recoveryNSAfterJoin || recoveryAfterJoin) {
			recoveryManager = recoveryExtension.getRecoveryManager();
			if (recoveryManager == null) throw new InitializationException(InitializationException.Error.MESSAGE_RECEIVER_INITIALIZATION_ERROR, null, "The recovery manager is null");
		}
		
		
		
		joinsData = new HashMap<Integer, HyCubeSearchJoinData>(HashMapUtils.getHashMapCapacityForElementsNum(GlobalConstants.INITIAL_JOINS_DATA_COLLECTION_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		

		String joinCallbackEventTypeKey = properties.getProperty(PROP_KEY_JOIN_CALLBACK_EVENT_KEY);
		joinCallbackEventType = new EventType(EventCategory.extEvent, joinCallbackEventTypeKey);
		
		String joinRequestTimeoutEventTypeKey = properties.getProperty(PROP_KEY_JOIN_REQUEST_TIMEOUT_EVENT_KEY);
		joinRequestTimeoutEventType = new EventType(EventCategory.extEvent, joinRequestTimeoutEventTypeKey);
		
		joinRequestTimeoutEventProxy = new JoinRequestTimeoutEventProxy();
		
		
		
	}

	
	
	@Override
	public JoinCallback join(String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg) {
		return join(bootstrapNodeAddress, joinCallback, callbackArg, null);
	}
	
	@Override
	public JoinCallback join(String bootstrapNodeAddress, JoinCallback joinCallback, Object callbackArg, Object[] joinParameters) {
	
		return join(new String[] {bootstrapNodeAddress}, joinCallback, callbackArg, joinParameters);
		
	}
	

	public JoinCallback join(String[] bootstrapNodeAddresses, JoinCallback joinCallback, Object callbackArg, Object[] joinParameters) {
		return join(nodeId, bootstrapNodeAddresses, joinCallback, callbackArg, joinAlpha, joinBeta, joinGamma, joinParameters);
	}
	

	
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
			HyCubeSearchJoinData oldJoinData = null;
			synchronized (joinManagerLock) {
				if (joinsData.containsKey(joinId)) {
					oldJoinData = joinsData.get(joinId);
					joinsData.remove(joinId);
				}
			}
			if (oldJoinData != null) oldJoinData.discard();
			
			
			//prepare the join structure
			HyCubeSearchJoinData joinData = new HyCubeSearchJoinData(joinId, bootstrapNodeAddresses, alpha, beta, gamma, secureSearch, skipRandomNextHops);
			
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
	
	
	
	
	protected void sendJoinRequestToBootstrapNodes(HyCubeSearchJoinData joinData) {

		//prepare the join parameters:
		
		//a temporary beta value (for initial request will be used: max(beta, alpha, gamma):
		short beta = joinData.beta;
		if (joinData.alpha > beta) beta = joinData.alpha;
		if (joinData.gamma > beta) beta = joinData.gamma;
		
		HyCubeSearchJoinNextHopSelectionParameters parameters = createDefaultJoinParameters(joinData.beta);
		if (parameters.isSteinhausTransformApplied()) {
			parameters.setSteinhausPoint(null);	//the Steinhaus point will be set by the nodes performing the first next hop selection
		}
		
		
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
			for (Entry<Integer, HyCubeSearchJoinData> entry : joinsData.entrySet()) {
				HyCubeSearchJoinData joinData = entry.getValue();
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
		HyCubeSearchJoinNextHopSelectionParameters parameters = joinParameters;
		
		//this is necessary, otherwise the Steinhaus point would equal the joinNodeId, which would cause all the distances to the destination to equal 1
		if (joinNodeId.equals(nodeId)) {
			parameters.setSteinhausTransformApplied(false);
			parameters.setSteinhausPoint(null);
		}
		
		short beta = joinParameters.getBeta();
		
		NodePointer[] nodesReturned = null;
		if (joinParameters.initialRequest) {
			
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
				
				if (includeNSInInitialJoinReply) {
					routingTable.getNsLock().readLock().lock();
					nodesReturnedNum += routingTable.getNsMap().size();
				}
				
				if (includeRTInInitialJoinReply) {
					routingTable.getRt1Lock().readLock().lock();
					routingTable.getRt2Lock().readLock().lock();
					nodesReturnedNum += routingTable.getRt1Map().size() + routingTable.getRt2Map().size();
				}
				
				HashMap<Long, NodePointer> nodesReturnedMap = new HashMap<Long, NodePointer>(HashMapUtils.getHashMapCapacityForElementsNum(nodesReturnedNum, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
				
				if (includeSelfInInitialJoinReply) {
					nodesReturnedMap.put(nodeAccessor.getNodePointer().getNodeIdHash(), nodeAccessor.getNodePointer());
				}
				
				if (includeNSInInitialJoinReply) {
					for (RoutingTableEntry rte : routingTable.getNsMap().values()) {
						nodesReturnedMap.put(rte.getNodeIdHash(), rte.getNode());
					}
					routingTable.getNsLock().readLock().unlock();
				}
				
				if (includeRTInInitialJoinReply) {
					for (RoutingTableEntry rte : routingTable.getRt1Map().values()) {
						nodesReturnedMap.put(rte.getNodeIdHash(), rte.getNode());
					}
					routingTable.getRt1Lock().readLock().unlock();
					
					for (RoutingTableEntry rte : routingTable.getRt2Map().values()) {
						nodesReturnedMap.put(rte.getNodeIdHash(), rte.getNode());
					}
					routingTable.getRt2Lock().readLock().unlock();
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
		
		HyCubeSearchJoinData joinData;
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
				joinParameters.setBeta(joinData.beta);
				
				processNewNodes(joinData, nodesFound, sender, joinParameters);
				
				
			}
			else if ((! joinParameters.finalSearch) && (! joinData.finalSearch)) {
			
				if (joinData.nodesRequested.contains(sender.getNodeIdHash())) {
					
					joinData.nodesResponded.add(sender.getNodeIdHash());
					
					//update the parameters used by this node:
					joinData.pmhApplied.put(sender.getNodeIdHash(), joinParameters.isPMHApplied());
					joinData.steinhausTransformApplied.put(sender.getNodeIdHash(), joinParameters.isSteinhausTransformApplied());
					joinData.steinhausPoints.put(sender.getNodeIdHash(), joinParameters.getSteinhausPoint());
					
					processNewNodes(joinData, nodesFound, sender, joinParameters);
					
				}
				
			}
			if (joinParameters.finalSearch && joinData.finalSearch) {	//final join
				
				if (joinData.nodesRequestedFinal.contains(sender.getNodeIdHash())) {
					
					joinData.nodesRespondedFinal.add(sender.getNodeIdHash());
					
					//update the parameters used by this node:
					joinData.pmhApplied.put(sender.getNodeIdHash(), joinParameters.isPMHApplied());
					joinData.steinhausTransformApplied.put(sender.getNodeIdHash(), joinParameters.isSteinhausTransformApplied());
					joinData.steinhausPoints.put(sender.getNodeIdHash(), joinParameters.getSteinhausPoint());
					
					processNewNodes(joinData, nodesFound, sender, joinParameters);
					
					
				}
				
			}
			
			
			
			//and process the updated join data
			processJoin(joinData);
			
		}


	}
	
	
	
	protected void processNewNodes(HyCubeSearchJoinData joinData, NodePointer[] newNodes, NodePointer sender, HyCubeSearchJoinNextHopSelectionParameters joinParameters) {
		

		//update the join data structure with the response
		//assume optimistically that the nodes are closer and already sorted with the same metric and start inserting from the last element 
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
			double dist = HyCubeNodeId.calculateDistance((HyCubeNodeId)np.getNodeId(), nodeId, metric);
			
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
					joinData.steinhausTransformApplied.remove(nodeRemoved.getNodeIdHash());
					joinData.steinhausPoints.remove(nodeRemoved.getNodeIdHash());
				}
			}
			
			joinData.closestNodes.add(insertIndex, np);
			joinData.distances.add(insertIndex, dist);
			
			joinData.closestNodesSet.add(np.getNodeIdHash());
			
			//set the parameters for this virtual route
			joinData.pmhApplied.put(np.getNodeIdHash(), joinParameters.isPMHApplied());
			joinData.steinhausTransformApplied.put(np.getNodeIdHash(), joinParameters.isSteinhausTransformApplied());
			joinData.steinhausPoints.put(np.getNodeIdHash(), joinParameters.getSteinhausPoint());
			
			
			
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
		
		HyCubeSearchJoinData joinData;
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
	
	
	protected void processJoin(HyCubeSearchJoinData joinData) {
		
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
					//if the Steinhaus transform is enabled, there should be no such nodes
					// - because nodes may return more distant nodes than themselves, there would never be a situation that no next hop is returned 
					//   (Steinhaus transform disabling condition) - only when the routing tables are completely empty,
					//   in which case next hop selection according to Euclidean metric would also not return any nodes
					for (long nodeIdHash : joinData.closestNodesSet) {
						if (joinData.nodesRequested.contains(nodeIdHash) 
								&& joinData.steinhausTransformApplied.get(nodeIdHash) == false
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
	
					if (recoveryNSAfterJoin) {
						//scheduleRecoveryNS
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
					HyCubeSearchJoinNextHopSelectionParameters parameters = createDefaultJoinParameters(joinData.beta);
					
					//set the request parameters (the values for this virtual route):
					parameters.setPMHApplied(joinData.pmhApplied.get(reqNode.getNodeIdHash()));
					parameters.setSteinhausTransformApplied(joinData.steinhausTransformApplied.get(reqNode.getNodeIdHash()));
					
//					//if PMH is applied, do not use Steinhaus transform anymore:
//					if (parameters.isPMHApplied()) parameters.setSteinhausTransformApplied(false);
//					else parameters.setSteinhausTransformApplied(joinData.steinhausTransformApplied.get(reqNode.getNodeIdHash()));
					
					
					if (parameters.isSteinhausTransformApplied()) parameters.setSteinhausPoint(joinData.steinhausPoints.get(reqNode.getNodeIdHash()));
					
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
					HyCubeSearchJoinNextHopSelectionParameters parameters = createDefaultJoinParameters(joinData.beta);
					//set the request parameters (the values for this virtual route):
					parameters.setFinalSearch(true);
					parameters.setPMHApplied(true);
					parameters.setSteinhausTransformApplied(false);
					parameters.setSteinhausPoint(null);
					//parameters.setPMHApplied(joinData.pmhApplied.get(reqNode.getNodeIdHash()));
					//parameters.setSteinhausTransformApplied(joinData.steinhausTransformApplied.get(reqNode.getNodeIdHash()));
					//parameters.setSteinhausPoint(joinData.steinhausPoints.get(reqNode.getNodeIdHash()));
					
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
	
	
	protected void sendJoinRequest(int joinId, NodePointer recipient, HyCubeNodeId joinNodeId, HyCubeSearchJoinNextHopSelectionParameters joinParameters) {
		
		//prepare the message:
		
		int messageSerialNo = nodeAccessor.getNextMessageSerialNo();
		byte[] joinMessageData = (new HyCubeSearchJoinMessageData(joinId, joinNodeId, joinParameters, discoverPublicNetworkAddress)).getBytes(nodeIdFactory.getDimensions(), nodeIdFactory.getDigitsCount());
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
	

	protected void sendJoinResponse(int joinId, NodePointer recipient, NetworkNodePointer joinNodePublicAddress, HyCubeSearchJoinNextHopSelectionParameters joinParameters, NodePointer[] nodesReturned) {
		
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
		byte[] joinReplyMessageData = (new HyCubeSearchJoinReplyMessageData(joinId, joinNodePublicAddress, joinParameters, nodesReturned)).getBytes(nodeIdFactory.getDimensions(), nodeIdFactory.getDigitsCount()); 
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
		Event event = new HyCubeSearchJoinRequestTimeoutEvent(this, joinRequestTimeoutEventProxy, joinId, nodeAddressString, nodeIdHash, initialRequest, finalSearch);
		
		//schedule the event:
		Queue<Event> queue = nodeAccessor.getEventQueue(joinRequestTimeoutEventType);
		EventScheduler scheduler = nodeAccessor.getEventScheduler();
		scheduler.scheduleEventWithDelay(event, queue, requestTimeout);
		
		
	}
	
	
	public class JoinRequestTimeoutEventProxy implements ProcessEventProxy {
		@Override
		public void processEvent(Event event) throws EventProcessException {
			if (! (event instanceof HyCubeSearchJoinRequestTimeoutEvent)) throw new EventProcessException("Invalid event type scheduled to be processed by " + HyCubeSearchJoinManager.class.getName() + ". The expected event class: " + HyCubeSearchJoinRequestTimeoutEvent.class.getName() + ".");
			HyCubeSearchJoinRequestTimeoutEvent joinEvent = (HyCubeSearchJoinRequestTimeoutEvent)event;
			try {
				requestTimedOut(joinEvent.joinId, joinEvent.nodeAddressString, joinEvent.nodeIdHash, joinEvent.initialRequest, joinEvent.finalSearch);
			}
			catch (Exception e) {
				throw new EventProcessException("An exception was thrown while processing a join request timeout event.", e);
			}
		}
	}
	
	

	
	
	
	protected HyCubeSearchJoinNextHopSelectionParameters createDefaultJoinParameters() {
		
		HyCubeSearchJoinNextHopSelectionParameters parameters = new HyCubeSearchJoinNextHopSelectionParameters();
		
		parameters.setIncludeMoreDistantNodes(true);
		parameters.setPMHApplied(false);
		parameters.setPreventPMH(false);
		parameters.setSecureRoutingApplied(false);
		parameters.setSkipRandomNumOfNodesApplied(false);
		parameters.setSkipTargetNode(true);
		parameters.setSteinhausTransformApplied(useSteinhausTransform);
		parameters.setSteinhausPoint(null);
		parameters.setFinalSearch(false);
		
		return parameters;
		
	}
	
	
	protected HyCubeSearchJoinNextHopSelectionParameters createDefaultJoinParameters(short beta) {
		
		HyCubeSearchJoinNextHopSelectionParameters parameters = createDefaultJoinParameters();
		
		parameters.setBeta(beta);
		
		return parameters;
		
	}




	@Override
	public EntryPoint getEntryPoint() {
		return null;
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
