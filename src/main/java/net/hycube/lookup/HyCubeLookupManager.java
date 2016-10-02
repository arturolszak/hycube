package net.hycube.lookup;

import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map.Entry;
import java.util.Queue;

import net.hycube.common.EntryPoint;
import net.hycube.configuration.GlobalConstants;
import net.hycube.core.HyCubeNodeId;
import net.hycube.core.HyCubeNodeIdFactory;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodeId;
import net.hycube.core.NodePointer;
import net.hycube.core.UnrecoverableRuntimeException;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.environment.NodeProperties;
import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventCategory;
import net.hycube.eventprocessing.EventProcessException;
import net.hycube.eventprocessing.EventScheduler;
import net.hycube.eventprocessing.EventType;
import net.hycube.eventprocessing.ProcessEventProxy;
import net.hycube.messaging.messages.HyCubeMessageFactory;
import net.hycube.messaging.messages.HyCubeMessageType;
import net.hycube.messaging.messages.Message;
import net.hycube.messaging.processing.MessageSendProcessInfo;
import net.hycube.messaging.processing.ProcessMessageException;
import net.hycube.metric.Metric;
import net.hycube.nexthopselection.NextHopSelector;
import net.hycube.transport.NetworkAdapterException;
import net.hycube.utils.HashMapUtils;
import net.hycube.utils.ObjectToStringConverter.MappedType;

/**
 * Lookup manager class.
 * @author Artur Olszak
 *
 */
public class HyCubeLookupManager implements LookupManager {
	
	protected static final String PROP_KEY_NEXT_HOP_SELECTOR_KEY = "NextHopSelectorKey";
	protected static final String PROP_KEY_LOOKUP_CALLBACK_EVENT_KEY = "LookupCallbackEventKey";
	protected static final String PROP_KEY_LOOKUP_REQUEST_TIMEOUT_EVENT_KEY = "LookupRequestTimeoutEventKey";
	protected static final String PROP_KEY_DEFAULT_BETA = "DefaultBeta";
	protected static final String PROP_KEY_DEFAULT_GAMMA = "DefaultGamma";
	protected static final String PROP_KEY_METRIC = "Metric";
	protected static final String PROP_KEY_USE_STEINHAUS_TRANSFORM = "UseSteinhausTransform";
	protected static final String PROP_KEY_REQUEST_TIMEOUT = "LookupRequestTimeout";
	
	
	protected HyCubeNodeId nodeId;
	
	protected int nextLookupId;
	
	protected Object lookupManagerLock = new Object();	//used top synchronize operations on the manager's data (like lookups list)
	
	protected HashMap<Integer, HyCubeLookupData> lookupsData;
	
	protected short defaultBeta;
	protected short defaultGamma;
	
	protected Metric metric;
	protected boolean useSteinhausTransform;
	
	protected int requestTimeout;
	
	protected NodeAccessor nodeAccessor;
	protected NodeProperties properties;
	
	protected EventType lookupCallbackEventType;
	
	protected String lookupRequestTimeoutEventTypeKey;
	protected EventType lookupRequestTimeoutEventType;
	protected ProcessEventProxy lookupRequestTimeoutEventProxy;
	
	protected NextHopSelector nextHopSelector;
	protected NodeProperties nextHopSelectorProperties;
	
	protected HyCubeMessageFactory messageFactory;
	protected HyCubeNodeIdFactory nodeIdFactory;
	
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		nextLookupId = Integer.MIN_VALUE;

		this.nodeAccessor = nodeAccessor;
		this.properties = properties;
		
		
		if (nodeAccessor.getNodeId() instanceof HyCubeNodeId) {
			this.nodeId = (HyCubeNodeId)nodeAccessor.getNodeId();
		}
		else {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize the lookup manager instance. The node id is expected to be an instance of: " + HyCubeNodeId.class.getName());
		}
		
		
		if (! (nodeAccessor.getMessageFactory() instanceof HyCubeMessageFactory)) throw new UnrecoverableRuntimeException("The message factory is expected to be an instance of: " + HyCubeMessageFactory.class.getName() + ".");
		messageFactory = (HyCubeMessageFactory) nodeAccessor.getMessageFactory();
		
		if (! (nodeAccessor.getNodeIdFactory() instanceof HyCubeNodeIdFactory)) throw new UnrecoverableRuntimeException("The node id factory is expected to be an instance of: " + HyCubeNodeIdFactory.class.getName() + ".");
		nodeIdFactory = (HyCubeNodeIdFactory) nodeAccessor.getNodeIdFactory();
		
		
		String nextHopSelectorKey = properties.getProperty(PROP_KEY_NEXT_HOP_SELECTOR_KEY);
		if (nextHopSelectorKey == null || nextHopSelectorKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_NEXT_HOP_SELECTOR_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_NEXT_HOP_SELECTOR_KEY));
		nextHopSelector = (NextHopSelector) nodeAccessor.getNextHopSelector(nextHopSelectorKey);
		
		try {
			
			this.defaultBeta = (Short) properties.getProperty(PROP_KEY_DEFAULT_BETA, MappedType.SHORT);
			this.defaultGamma = (Short) properties.getProperty(PROP_KEY_DEFAULT_GAMMA, MappedType.SHORT);
			
			this.metric = (Metric) properties.getEnumProperty(PROP_KEY_METRIC, Metric.class);
			
			this.useSteinhausTransform = (Boolean) properties.getProperty(PROP_KEY_USE_STEINHAUS_TRANSFORM, MappedType.BOOLEAN);
			
			this.requestTimeout = (Integer) properties.getProperty(PROP_KEY_REQUEST_TIMEOUT, MappedType.INT);
			
			
			
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize the lookup manager instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		

		
		
		lookupsData = new HashMap<Integer, HyCubeLookupData>(HashMapUtils.getHashMapCapacityForElementsNum(GlobalConstants.INITIAL_LOOKUPS_DATA_COLLECTION_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		

		String lookupCallbackEventTypeKey = properties.getProperty(PROP_KEY_LOOKUP_CALLBACK_EVENT_KEY);
		lookupCallbackEventType = new EventType(EventCategory.extEvent, lookupCallbackEventTypeKey);
		
		String lookupRequestTimeoutEventTypeKey = properties.getProperty(PROP_KEY_LOOKUP_REQUEST_TIMEOUT_EVENT_KEY);
		lookupRequestTimeoutEventType = new EventType(EventCategory.extEvent, lookupRequestTimeoutEventTypeKey);
		
		lookupRequestTimeoutEventProxy = new LookupRequestTimeoutEventProxy();
		
		
		
	}

	
	

	@Override
	public LookupCallback lookup(NodeId lookupNodeId, LookupCallback lookupCallback, Object callbackArg) {
		return lookup(lookupNodeId, lookupCallback, callbackArg, defaultBeta, defaultGamma, false, false);
	}
	
	@Override
	public LookupCallback lookup(NodeId lookupNodeId, LookupCallback lookupCallback, Object callbackArg, Object[] parameters) {
		
		short beta = defaultBeta;
		short gamma = defaultGamma;
		
		boolean secureLookup = false;
		boolean skipRandomNextHops = false;
		
		if (parameters != null) {
			
			short paramBeta = getLookupParameterBeta(parameters);
			short paramGamma = getLookupParameterGamma(parameters);
			
			if (paramBeta != 0) beta = paramBeta;
			if (paramGamma != 0) gamma = paramGamma;

			secureLookup = getLookupParameterSecureLookup(parameters);
			
			skipRandomNextHops = getLookupParameterSkipRandomNextHops(parameters);
			
		}
		
		return lookup(lookupNodeId, lookupCallback, callbackArg, beta, gamma, secureLookup, skipRandomNextHops);
		
	}
	
	public LookupCallback lookup(NodeId lookupNodeId, LookupCallback lookupCallback, Object callbackArg, short beta, short gamma, boolean secureLookup, boolean skipRandomNextHops) {
		if (lookupNodeId instanceof HyCubeNodeId) {
			return lookup((HyCubeNodeId)lookupNodeId, lookupCallback, callbackArg, beta, gamma, secureLookup, skipRandomNextHops);
		}
		else {
			throw new IllegalArgumentException("The lookup node id should be an instance of: " + HyCubeNodeId.class.getName());
		}
		
	}
		
	public LookupCallback lookup(HyCubeNodeId lookupNodeId, LookupCallback lookupCallback, Object callbackArg, short beta, short gamma, boolean secureLookup, boolean skipRandomNextHops) {
		
		synchronized(lookupManagerLock) {
		
			//generate the lookup id
			int lookupId = getNextLookupId();
			
			
			//if exists, discard previous lookup with the same id:   (the range of int is big enough, so it will not limit the number of concurrent lookup operations)
			HyCubeLookupData oldLookupData = null;
			synchronized (lookupManagerLock) {
				if (lookupsData.containsKey(lookupId)) {
					oldLookupData = lookupsData.get(lookupId);
					lookupsData.remove(lookupId);
				}
			}
			if (oldLookupData != null) oldLookupData.discard();
			
			
			//prepare the lookup structure
			HyCubeLookupData lookupData = new HyCubeLookupData(lookupId, lookupNodeId, beta, gamma, secureLookup, skipRandomNextHops);
			
			lookupsData.put(lookupId, lookupData);
			
			
			//registerLookupCallback
			lookupData.setLookupCallback(lookupCallback);
			lookupData.setCallbackArg(callbackArg);
			
			
			//send the lookup request:
			
			
			HyCubeLookupNextHopSelectionParameters parameters = createDefaultLookupParameters(beta);
			
			parameters.setSteinhausPoint(null);
			
			if (lookupNodeId.equals(nodeId)) {
				//this is necessary, otherwise the Steinhaus point would equal the lookupNodeId, which would cause all the distances to the destination to equal 1
				parameters.setSteinhausTransformApplied(false);
			}
			
			if (parameters.isSteinhausTransformApplied()) {
				parameters.setSteinhausPoint(nodeId);
			}
			

			parameters.setSecureRoutingApplied(lookupData.secureLookup);
			parameters.setSkipRandomNumOfNodesApplied(lookupData.skipRandomNextHops);
			

			
			
			NodePointer[] initialNodes = null;
			
			if (lookupNodeId.equals(nodeId)) {
				initialNodes = new NodePointer[] {nodeAccessor.getNodePointer()};
			}
			else {
				
				//just for initial local search, include self in results (self might be one of the gamma closest)
				parameters.setIncludeSelf(true);
				
				initialNodes = nextHopSelector.findNextHops(lookupNodeId, parameters, gamma);
				
				//the nodes requested should not return self:
				parameters.setIncludeSelf(false);
				
			}
			
			
			
			
			processNewNodes(lookupData, initialNodes, parameters);
			
			//and process the lookup list:
			processLookup(lookupData);
		
			
		}
		
		return lookupCallback;
		
	}
	
	
	protected int getNextLookupId() {
		synchronized (lookupManagerLock) {
			int lookupId = nextLookupId;
			nextLookupId++;
			return lookupId;
		}
	}




	public void discard() {
		synchronized (lookupManagerLock) {
			for (Entry<Integer, HyCubeLookupData> entry : lookupsData.entrySet()) {
				int lookupId = entry.getKey();
				HyCubeLookupData lookupData = entry.getValue();
				enqueueCallbackEvent(lookupData.lookupCallback, lookupData.callbackArg, lookupId, null);
				lookupData.discard();
			}
		}
		
	}


	@Override
	public EventType getLookupCallbackEventType() {
		return lookupCallbackEventType;
	}
	
	
	@Override
	public EventType getLookupRequestTimeoutEventType() {
		return lookupRequestTimeoutEventType;
	}


	protected void enqueueCallbackEvent(LookupCallback lookupCallback, Object callbackArg, int lookupId, NodePointer result) {
		//create the event
		Event event = new LookupCallbackEvent(this, lookupCallback, callbackArg, lookupId, result);
		
		//insert to the appropriate event queue
		try {
			nodeAccessor.getEventQueue(lookupCallbackEventType).put(event);
		} catch (InterruptedException e) {
			//this should never happen
			throw new UnrecoverableRuntimeException("An exception was thrown while inserting an event to an event queue.");
		}
		
	}
	
	
	
	
	public void processLookupRequest(NodePointer sender, int lookupId, NodeId lookupNodeId, HyCubeLookupNextHopSelectionParameters lookupParameters) {
		
		//find best nodes according to the specified parameters

		//use the lookup parameters specified by the requestor:
		HyCubeLookupNextHopSelectionParameters parameters = lookupParameters;
		
		short beta = lookupParameters.getBeta();
		
		if (lookupNodeId.equals(nodeId)) {
			//this is necessary, otherwise the Steinhaus point would equal the lookupNodeId, which would cause all the distances to the destination to equal 1
			parameters.setSteinhausTransformApplied(false);
			parameters.setSteinhausPoint(null);
		}
		
		NodePointer[] nodesFound = nextHopSelector.findNextHops(lookupNodeId, parameters, beta);
				
		
		//send response to the sender:
		sendLookupResponse(lookupId, sender, parameters, nodesFound);
		
		
	}
	
	
	public void processLookupResponse(NodePointer sender, int lookupId, HyCubeLookupNextHopSelectionParameters lookupParameters, NodePointer[] nodesFound) {
		
		HyCubeLookupData lookupData;
		synchronized(lookupManagerLock) {
			if (lookupsData.containsKey(lookupId)) {
				lookupData = lookupsData.get(lookupId);
			}
			else return;	//the lookup has already been terminated, or the lookup id is wrong
		}
		
		
		synchronized (lookupData) {
		
			//update the parameters used by the node that returned the lookup result:
			lookupData.pmhApplied.put(sender.getNodeIdHash(), lookupParameters.isPMHApplied());
			lookupData.steinhausTransformApplied.put(sender.getNodeIdHash(), lookupParameters.isSteinhausTransformApplied());
			lookupData.steinhausPoints.put(sender.getNodeIdHash(), lookupParameters.getSteinhausPoint());			
			
			
			
			if ((! lookupParameters.finalLookup) && (! lookupData.finalLookup)) {
				if (lookupData.nodesRequested.contains(sender.getNodeIdHash())) {
					
					lookupData.nodesResponded.add(sender.getNodeIdHash());
					
					processNewNodes(lookupData, nodesFound, lookupParameters);
					
				}
				
			}
			if (lookupParameters.finalLookup && lookupData.finalLookup) {	//final lookup
				if (lookupData.nodesRequestedFinal.contains(sender.getNodeIdHash())) {
					
					lookupData.nodesRespondedFinal.add(sender.getNodeIdHash());
					
					processNewNodes(lookupData, nodesFound, lookupParameters);
					
					
				}
				
			}
			
			
			
			//and process the updated lookup data
			processLookup(lookupData);
			
		}


	}
	
	
	
	public void processNewNodes(HyCubeLookupData lookupData, NodePointer[] newNodes, HyCubeLookupNextHopSelectionParameters lookupParameters) {
		
		//update the lookup data structure with the response
		
		//assume optimistically that the nodes are closer and already sorted with the same metric and start inserting from the last element 
		
		lookupData.setNextNode(null);
		lookupData.setNextNodePmhApplied(false);
		lookupData.setNextNodeSteinhausTransformApplied(false);
		lookupData.setNextSteinhausPoint(null);
		double nextNodeDist = 0;
		
		for (int i = newNodes.length - 1; i >=0; i--) {
			
			NodePointer np = newNodes[i];
			
			if (! (np.getNodeId() instanceof HyCubeNodeId)) throw new IllegalArgumentException("The lookup node id should be an instance of: " + HyCubeNodeId.class.getName());
			double dist = HyCubeNodeId.calculateDistance((HyCubeNodeId)np.getNodeId(), lookupData.lookupNodeId, metric);
			double steinDist = 0;
			if (lookupParameters.isSteinhausTransformApplied()) {
				HyCubeNodeId.calculateDistance((HyCubeNodeId)np.getNodeId(), lookupData.lookupNodeId, lookupParameters.getSteinhausPoint(), metric);
			}
			else steinDist = dist;
			
			
			//set the next node to ask on this virtual route - the closest (Steinhaus metric) node found that was not yet requested
			if (lookupData.nextNode == null || steinDist < nextNodeDist) {
				if (! lookupData.finalLookup) {
					if (! lookupData.nodesRequested.contains(np.getNodeIdHash())) {
						lookupData.setNextNode(np);
						lookupData.setNextNodePmhApplied(lookupParameters.isPMHApplied());
						lookupData.setNextNodeSteinhausTransformApplied(lookupParameters.isSteinhausTransformApplied());
						lookupData.setNextSteinhausPoint(lookupParameters.getSteinhausPoint());
						nextNodeDist = steinDist;
					}
				}
				else {	//final
					if (! lookupData.nodesRequestedFinal.contains(np.getNodeIdHash())) {
						lookupData.setNextNode(np);
						lookupData.setNextNodePmhApplied(lookupParameters.isPMHApplied());
						lookupData.setNextNodeSteinhausTransformApplied(lookupParameters.isSteinhausTransformApplied());
						lookupData.setNextSteinhausPoint(lookupParameters.getSteinhausPoint());
						nextNodeDist = steinDist;
					}
				}
			}
			
			
			
			if (lookupData.closestNodesSet.contains(np.getNodeIdHash())) {
				//ignore that node, it is already in the closest nodes collection
				continue;
			}
			
			
			
			int insertIndex = 0;
			
			//ListIterator<NodePointer> nodeIter = lookupData.closestNodes.listIterator();
			ListIterator<Double> distIter = lookupData.distances.listIterator();
			
			//while (nodeIter.hasNext()) {
			while (distIter.hasNext()) {
				//NodePointer nextNode = nodeIter.next();
				double nextDist = distIter.next();
				if (dist < nextDist) break;
				else insertIndex++;
			}

			if (insertIndex == lookupData.gamma) {
				//skip this node - we store only gamma closest nodes found
				continue;
			}
			else {
				if (lookupData.closestNodes.size() == lookupData.gamma) {
					//the closest nodes collection already contains gamma (number of closed nodes stored) nodes, so we should remove the last element
					NodePointer nodeRemoved = lookupData.closestNodes.removeLast();
					lookupData.distances.removeLast();
					lookupData.closestNodesSet.remove(nodeRemoved.getNodeIdHash());
					lookupData.pmhApplied.remove(nodeRemoved.getNodeIdHash());
					lookupData.steinhausTransformApplied.remove(nodeRemoved.getNodeIdHash());
					lookupData.steinhausPoints.remove(nodeRemoved.getNodeIdHash());
				}
			}
			
			lookupData.closestNodes.add(insertIndex, np);				
			lookupData.distances.add(insertIndex, dist);
			
			lookupData.closestNodesSet.add(np.getNodeIdHash());
			
			//set the parameters for this virtual route
			lookupData.pmhApplied.put(np.getNodeIdHash(), lookupParameters.isPMHApplied());
			lookupData.steinhausTransformApplied.put(np.getNodeIdHash(), lookupParameters.isSteinhausTransformApplied());
			lookupData.steinhausPoints.put(np.getNodeIdHash(), lookupParameters.getSteinhausPoint());
			
			if (insertIndex == 0) {	//this is the closest node:
				lookupData.minDistance = dist;
				lookupData.closestNode = np;
			}
			
			

			
			
		}
	}
	
	
	public void requestTimedOut(int lookupId, long nodeIdHash, boolean finalLookup) {
		
		HyCubeLookupData lookupData;
		synchronized(lookupManagerLock) {
			if (lookupsData.containsKey(lookupId)) {
				lookupData = lookupsData.get(lookupId);
			}
			else return;	//the lookup has already been terminated
		}
		
		
		synchronized (lookupData) {
			
			//clear the next node
			lookupData.setNextNode(null);
			lookupData.setNextNodePmhApplied(false);
			lookupData.setNextNodeSteinhausTransformApplied(false);
			lookupData.setNextSteinhausPoint(null);
			
			//mark the node (for which the response time limit was reached) as "responded"
			if (! finalLookup) {
				if (lookupData.nodesRequested.contains(nodeIdHash)) {
					lookupData.nodesResponded.add(nodeIdHash);
				}
			}
			else {	//final lookup
				if (lookupData.nodesRequestedFinal.contains(nodeIdHash)) {
					lookupData.nodesRespondedFinal.add(nodeIdHash);
				}
			}
		
			
			//and process the lookup data:
			processLookup(lookupData);
			
		}
		
		
	}
	
	
	protected void processLookup(HyCubeLookupData lookupData) {
		
		//go through the nodes list and continue the lookup procedure
		
		synchronized (lookupData) {
			
			//if the closest element found is the lookup node id, return it
			if (lookupData.closestNode != null && lookupData.closestNode.getNodeId().equals(lookupData.lookupNodeId)) {

				//enqueue the callback event
				enqueueCallbackEvent(lookupData.lookupCallback, lookupData.callbackArg, lookupData.lookupId, lookupData.closestNode);
				
				//remove this lookup from the lookups list:
				synchronized (lookupManagerLock) {
					lookupsData.remove(lookupData.lookupId);
				}
				
				return;
				
			}
			

			
			if (! lookupData.finalLookup) {
				//if closest nodes contain self, mark it as already requested and responded (the local lookup was done in the initial step of the lookup procedure)
				ListIterator<NodePointer> nodeIter = lookupData.closestNodes.listIterator();
				while (nodeIter.hasNext() && nodeIter.nextIndex() < lookupData.gamma) { 
					NodePointer np = nodeIter.next();
					if (np.getNodeId().equals(nodeId)) {
						if (! lookupData.nodesResponded.contains(np.getNodeIdHash())) {
							lookupData.nodesRequested.add(np.getNodeIdHash());
							lookupData.nodesResponded.add(np.getNodeIdHash());
						}
						break;
					}
				}
			}
			else {	//final lookup
				//if closest nodes contain self and it is not marked as requested (and responded), perform a local lookup and mark it as already requested and responded
				ListIterator<NodePointer> nodeIter = lookupData.closestNodes.listIterator();
				while (nodeIter.hasNext() && nodeIter.nextIndex() < lookupData.gamma) { 
					NodePointer np = nodeIter.next();
					if (np.getNodeId().equals(nodeId)) {
						if (! lookupData.nodesRespondedFinal.contains(np.getNodeIdHash())) {
							
							//prepare the lookup parameters:
							HyCubeLookupNextHopSelectionParameters parameters = createDefaultLookupParameters(lookupData.beta);
							//set the request parameters (the values for this virtual route):
							parameters.setFinalLookup(true);
							parameters.setPMHApplied(true);
							parameters.setSteinhausTransformApplied(false);
							parameters.setSteinhausPoint(null);
							parameters.setSecureRoutingApplied(lookupData.secureLookup);
							parameters.setSkipRandomNumOfNodesApplied(lookupData.skipRandomNextHops);

							
							NodePointer[] newNodes = nextHopSelector.findNextHops(lookupData.lookupNodeId, parameters, lookupData.beta);
							
							processNewNodes(lookupData, newNodes, parameters);
							
							
							//mark the node as requested/responded in the final phase
							lookupData.nodesRequestedFinal.add(np.getNodeIdHash());
							lookupData.nodesRespondedFinal.add(np.getNodeIdHash());
							
						}
						break;
					}
				}
			}
			
			
			
			if (! lookupData.finalLookup) {
				
				boolean switchToFinalSearch = true;
				ListIterator<NodePointer> nodeIter = lookupData.closestNodes.listIterator();
				while (nodeIter.hasNext() && nodeIter.nextIndex() < lookupData.gamma) { 
					NodePointer np = nodeIter.next();
					if (! lookupData.nodesResponded.contains(np.getNodeIdHash())) {
						switchToFinalSearch = false;
						break;
					}
				}
				
				if (switchToFinalSearch) {
				
					//all nodes were already requested and responded

					
					//enable final lookup option:
					lookupData.finalLookup = true;
						
					//set all the nodes that were already requested with the settings used for final lookup
					for (long nodeIdHash : lookupData.closestNodesSet) {
						if (lookupData.nodesRequested.contains(nodeIdHash) 
								&& lookupData.steinhausTransformApplied.get(nodeIdHash) == false
								&& lookupData.pmhApplied.get(nodeIdHash) == true) {
							
							lookupData.nodesRequestedFinal.add(nodeIdHash);
							lookupData.nodesRespondedFinal.add(nodeIdHash);
							
						}
							
					}
				}
			}
			if (lookupData.finalLookup) {
								
				boolean searchFinished = true;
				ListIterator<NodePointer> nodeIter = lookupData.closestNodes.listIterator();
				while (nodeIter.hasNext() && nodeIter.nextIndex() < lookupData.gamma) { 
					NodePointer np = nodeIter.next();
					if (! lookupData.nodesRespondedFinal.contains(np.getNodeIdHash())) {
						searchFinished = false;
						break;
					}
				}
				
				if (searchFinished) {
				
					//all nodes were already requested and responded -> this lookup procedure is finished
					
					//enqueue the callback event
					enqueueCallbackEvent(lookupData.lookupCallback, lookupData.callbackArg, lookupData.lookupId, lookupData.closestNode);
					
					//remove this lookup from the lookups list:
					synchronized (lookupManagerLock) {
						lookupsData.remove(lookupData.lookupId);
					}
					
					return;
					
				}
					
			}
						
			
			
			
			if (! lookupData.finalLookup) {
				if (lookupData.nodesRequested.size() == lookupData.nodesResponded.size()) {
					//if the node previously requested already responded (or the request was discarded due to a timeout):
					
					if (lookupData.nextNode != null && (! lookupData.nodesRequested.contains(lookupData.nextNode.getNodeIdHash()))) {
						NodePointer reqNode = lookupData.nextNode;
						
						HyCubeLookupNextHopSelectionParameters parameters = createDefaultLookupParameters(lookupData.beta);
						
						//set the request parameters (the values for this virtual route):
						parameters.setPMHApplied(lookupData.nextNodePmhApplied);
						parameters.setSteinhausTransformApplied(lookupData.nextNodeSteinhausTransformApplied);
						if (parameters.isSteinhausTransformApplied()) parameters.setSteinhausPoint(lookupData.nextSteinhausPoint);
						parameters.setSecureRoutingApplied(lookupData.secureLookup);
						parameters.setSkipRandomNumOfNodesApplied(lookupData.skipRandomNextHops);
						
						//send the lookup request to this node:
						sendLookupRequest(lookupData.lookupId, reqNode, lookupData.lookupNodeId, parameters);
						
						//mark this node as requested:
						lookupData.nodesRequested.add(reqNode.getNodeIdHash());
					}
					else {
						//find first (closest) node to which the request has not yet been sent:
						NodePointer reqNode = null;
						ListIterator<NodePointer> nodeIter = lookupData.closestNodes.listIterator();
						while (nodeIter.hasNext()) {
							NodePointer np = nodeIter.next();
							if (! lookupData.nodesRequested.contains(np.getNodeIdHash())) {
								//this node is the closest that has not yet been requested"
								reqNode = np;
								break;
							}
						}
					
						if (reqNode != null) {
							
							//prepare the lookup parameters:
							HyCubeLookupNextHopSelectionParameters parameters = createDefaultLookupParameters(lookupData.beta);
							
							//set the request parameters (the values for this virtual route):
							parameters.setPMHApplied(lookupData.pmhApplied.get(reqNode.getNodeIdHash()));
							parameters.setSteinhausTransformApplied(lookupData.steinhausTransformApplied.get(reqNode.getNodeIdHash()));
							if (parameters.isSteinhausTransformApplied()) parameters.setSteinhausPoint(lookupData.steinhausPoints.get(reqNode.getNodeIdHash()));
							parameters.setSecureRoutingApplied(lookupData.secureLookup);
							parameters.setSkipRandomNumOfNodesApplied(lookupData.skipRandomNextHops);
							
							//send the lookup request to this node:
							sendLookupRequest(lookupData.lookupId, reqNode, lookupData.lookupNodeId, parameters);
							
							//mark this node as requested:
							lookupData.nodesRequested.add(reqNode.getNodeIdHash());
							
						}
					}
				}
			}
			
			else {	//final lookup:
				if (lookupData.nodesRequestedFinal.size() == lookupData.nodesRespondedFinal.size()) {
					//if the node previously requested already responded (or the request was discarded due to a timeout):
					
					if (lookupData.nextNode != null && (! lookupData.nodesRequestedFinal.contains(lookupData.nextNode.getNodeIdHash()))) {
						NodePointer reqNode = lookupData.nextNode;
					
						HyCubeLookupNextHopSelectionParameters parameters = createDefaultLookupParameters(lookupData.beta);
						
						//set the request parameters (the values for this virtual route):
						parameters.setFinalLookup(true);
						parameters.setPMHApplied(true);
						parameters.setSteinhausTransformApplied(false);
						parameters.setSteinhausPoint(null);
						parameters.setSecureRoutingApplied(lookupData.secureLookup);
						parameters.setSkipRandomNumOfNodesApplied(lookupData.skipRandomNextHops);
						
						//send the lookup request to this node:
						sendLookupRequest(lookupData.lookupId, reqNode, lookupData.lookupNodeId, parameters);
						
						//mark this node as requested:
						lookupData.nodesRequestedFinal.add(reqNode.getNodeIdHash());
					}
					else {
						//find first (closest) node to which the request has not yet been sent:
						NodePointer reqNode = null;
						ListIterator<NodePointer> nodeIter = lookupData.closestNodes.listIterator();
						while (nodeIter.hasNext()) {
							NodePointer np = nodeIter.next();
							if (! lookupData.nodesRequestedFinal.contains(np.getNodeIdHash())) {
								//this node is the closest that has not yet been requested"
								reqNode = np;
								break;
							}
						}
						
						if (reqNode != null) {
							//prepare the lookup parameters:
							HyCubeLookupNextHopSelectionParameters parameters = createDefaultLookupParameters(lookupData.beta);
							//set the request parameters (the values for this virtual route):
							parameters.setFinalLookup(true);
							parameters.setPMHApplied(true);
							parameters.setSteinhausTransformApplied(false);
							parameters.setSteinhausPoint(null);
							parameters.setSecureRoutingApplied(lookupData.secureLookup);
							parameters.setSkipRandomNumOfNodesApplied(lookupData.skipRandomNextHops);
							
							
							//send the lookup request to this node:
							sendLookupRequest(lookupData.lookupId, reqNode, lookupData.lookupNodeId, parameters);
							
							//mark this node as requested:
							lookupData.nodesRequestedFinal.add(reqNode.getNodeIdHash());
						}
					}
				}
			}
			
			
		}
		
		
	}
	
	
	protected void sendLookupRequest(int lookupId, NodePointer recipient, HyCubeNodeId lookupNodeId, HyCubeLookupNextHopSelectionParameters lookupParameters) {
		
		//prepare the message:
		
		int messageSerialNo = nodeAccessor.getNextMessageSerialNo();
		byte[] lookupMessageData = (new HyCubeLookupMessageData(lookupId, lookupNodeId, lookupParameters)).getBytes(nodeIdFactory.getDimensions(), nodeIdFactory.getDigitsCount());
		Message lookupMessage = messageFactory.newMessage(messageSerialNo, nodeAccessor.getNodeId(), recipient.getNodeId(), nodeAccessor.getNetworkAdapter().getPublicAddressBytes(), HyCubeMessageType.LOOKUP, nodeAccessor.getNodeParameterSet().getMessageTTL(), (short)0, false, false, (short)0, (short)0, lookupMessageData); 

		
		//send the message directly to the recipient:
		try {
			nodeAccessor.sendMessage(new MessageSendProcessInfo(lookupMessage, recipient.getNetworkNodePointer(), false), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
		} catch (NetworkAdapterException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a lookup request to a node.", e);
		} catch (ProcessMessageException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a lookup request to a node.", e);
		}
		
		
		//schedule the request time out event
		scheduleRequestTimeout(lookupId, recipient.getNodeIdHash(), lookupParameters.finalLookup);
		
		
	}
	

	protected void sendLookupResponse(int lookupId, NodePointer recipient, HyCubeLookupNextHopSelectionParameters lookupParameters, NodePointer[] nodesFound) {
		
		//prepare the message:
		
		int messageSerialNo = nodeAccessor.getNextMessageSerialNo();
		byte[] lookupReplyMessageData = (new HyCubeLookupReplyMessageData(lookupId, lookupParameters, nodesFound)).getBytes(nodeIdFactory.getDimensions(), nodeIdFactory.getDigitsCount()); 
		Message lookupReplyMessage = messageFactory.newMessage(messageSerialNo, nodeAccessor.getNodeId(), recipient.getNodeId(), nodeAccessor.getNetworkAdapter().getPublicAddressBytes(), HyCubeMessageType.LOOKUP_REPLY, nodeAccessor.getNodeParameterSet().getMessageTTL(), (short)0, false, false, (short)0, (short)0, lookupReplyMessageData); 

		
		//send the message directly to the recipient:
		try {
			nodeAccessor.sendMessage(new MessageSendProcessInfo(lookupReplyMessage, recipient.getNetworkNodePointer(), false), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
		} catch (NetworkAdapterException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a lookup response to a node.", e);
		} catch (ProcessMessageException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a lookup response to a node.", e);
		}

		
	}
	
	
	protected void scheduleRequestTimeout(int lookupId, long nodeIdHash, boolean finalLookup) {
		enqueueLookupRequestTimeoutEvent(lookupId, nodeIdHash, finalLookup);
		
		
	}
	
	protected void enqueueLookupRequestTimeoutEvent(int lookupId, long nodeIdHash, boolean finalLookup) {
		
		//create the event:
		Event event = new LookupRequestTimeoutEvent(this, lookupRequestTimeoutEventProxy, lookupId, nodeIdHash, finalLookup);
		
		//schedule the event:
		Queue<Event> queue = nodeAccessor.getEventQueue(lookupRequestTimeoutEventType);
		EventScheduler scheduler = nodeAccessor.getEventScheduler();
		scheduler.scheduleEventWithDelay(event, queue, requestTimeout);
		
		
	}
	
	
	public class LookupRequestTimeoutEventProxy implements ProcessEventProxy {
		@Override
		public void processEvent(Event event) throws EventProcessException {
			if (! (event instanceof LookupRequestTimeoutEvent)) throw new EventProcessException("Invalid event type scheduled to be processed by " + HyCubeLookupManager.class.getName() + ". The expected event class: " + LookupRequestTimeoutEvent.class.getName() + ".");
			LookupRequestTimeoutEvent lookupEvent = (LookupRequestTimeoutEvent)event;
			try {
				requestTimedOut(lookupEvent.lookupId, lookupEvent.nodeIdHash, lookupEvent.finalLookup);
			}
			catch (Exception e) {
				throw new EventProcessException("An exception was thrown while processing a lookup request timeout event.", e);
			}
		}
	}
	
	

	
	
	
	protected HyCubeLookupNextHopSelectionParameters createDefaultLookupParameters() {
		
		HyCubeLookupNextHopSelectionParameters parameters = new HyCubeLookupNextHopSelectionParameters();
		
		parameters.setIncludeMoreDistantNodes(false);
		parameters.setPMHApplied(false);
		parameters.setPreventPMH(false);
		parameters.setSecureRoutingApplied(false);
		parameters.setSkipRandomNumOfNodesApplied(false);
		parameters.setSkipTargetNode(false);
		parameters.setSteinhausTransformApplied(useSteinhausTransform);
		parameters.setSteinhausPoint(null);
		parameters.setFinalLookup(false);
		
		return parameters;
		
	}
	
	
	protected HyCubeLookupNextHopSelectionParameters createDefaultLookupParameters(short beta) {
		
		HyCubeLookupNextHopSelectionParameters parameters = createDefaultLookupParameters();
		
		parameters.setBeta(beta);
		
		return parameters;
		
	}




	@Override
	public EntryPoint getEntryPoint() {
		return null;
	}
	
	
	public static Object[] createLookupParameters(Short beta, Short gamma, Boolean secureLookup, Boolean skipRandomNextHops) {
		Object[] parameters = new Object[] {beta, gamma, secureLookup, skipRandomNextHops};
		return parameters;
	}

	public static short getLookupParameterBeta(Object[] parameters) {
		if (parameters == null) return 0;
		if (! (parameters.length > 0) || (!(parameters[0] instanceof Short))) return 0;
		return (Short)parameters[0];
	}

	public static short getLookupParameterGamma(Object[] parameters) {
		if (parameters == null) return 0;
		if (! (parameters.length > 1) || (!(parameters[1] instanceof Short))) return 0;
		return (Short)parameters[1];
	}
	
	public static boolean getLookupParameterSecureLookup(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 2) || (!(parameters[2] instanceof Boolean))) return false;
		return (Boolean)parameters[2];
	}
	
	public static boolean getLookupParameterSkipRandomNextHops(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 3) || (!(parameters[3] instanceof Boolean))) return false;
		return (Boolean)parameters[3];
	}
	


}

