package net.hycube.search;

import java.util.HashMap;
import java.util.LinkedList;
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
 * Search manager class.
 * @author Artur Olszak
 *
 */
public class HyCubeSearchManager implements SearchManager {
	
	protected static final String PROP_KEY_NEXT_HOP_SELECTOR_KEY = "NextHopSelectorKey";
	protected static final String PROP_KEY_SEARCH_CALLBACK_EVENT_KEY = "SearchCallbackEventKey";
	protected static final String PROP_KEY_SEARCH_REQUEST_TIMEOUT_EVENT_KEY = "SearchRequestTimeoutEventKey";
	protected static final String PROP_KEY_DEFAULT_ALPHA = "DefaultAlpha";
	protected static final String PROP_KEY_DEFAULT_BETA = "DefaultBeta";
	protected static final String PROP_KEY_DEFAULT_GAMMA = "DefaultGamma";
	protected static final String PROP_KEY_METRIC = "Metric";
	protected static final String PROP_KEY_USE_STEINHAUS_TRANSFORM = "UseSteinhausTransform";
	protected static final String PROP_KEY_REQUEST_TIMEOUT = "SearchRequestTimeout";
	
	protected static final boolean DEFAULT_IGNORE_TARGET_NODE = false;
	
	protected HyCubeNodeId nodeId;
	
	protected int nextSearchId;
	
	protected Object searchManagerLock = new Object();	//used top synchronize operations on the manager's data (like searches list)
	
	protected HashMap<Integer, HyCubeSearchData> searchesData;
	
	protected short defaultAlpha;
	protected short defaultBeta;
	protected short defaultGamma;
	
	protected Metric metric;
	protected boolean useSteinhausTransform;
	
	protected int requestTimeout;
	
	protected NodeAccessor nodeAccessor;
	protected NodeProperties properties;
	
	protected EventType searchCallbackEventType;
	
	protected String searchRequestTimeoutEventTypeKey;
	protected EventType searchRequestTimeoutEventType;
	protected ProcessEventProxy searchRequestTimeoutEventProxy;
	
	protected NextHopSelector nextHopSelector;
	protected NodeProperties nextHopSelectorProperties;
	
	protected HyCubeMessageFactory messageFactory;
	protected HyCubeNodeIdFactory nodeIdFactory;
	
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		nextSearchId = Integer.MIN_VALUE;

		this.nodeAccessor = nodeAccessor;
		this.properties = properties;
		
		
		if (nodeAccessor.getNodeId() instanceof HyCubeNodeId) {
			this.nodeId = (HyCubeNodeId)nodeAccessor.getNodeId();
		}
		else {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize the search manager instance. The node id is expected to be an instance of: " + HyCubeNodeId.class.getName());
		}
		
		
		if (! (nodeAccessor.getMessageFactory() instanceof HyCubeMessageFactory)) throw new UnrecoverableRuntimeException("The message factory is expected to be an instance of: " + HyCubeMessageFactory.class.getName() + ".");
		messageFactory = (HyCubeMessageFactory) nodeAccessor.getMessageFactory();
		
		if (! (nodeAccessor.getNodeIdFactory() instanceof HyCubeNodeIdFactory)) throw new UnrecoverableRuntimeException("The node id factory is expected to be an instance of: " + HyCubeNodeIdFactory.class.getName() + ".");
		nodeIdFactory = (HyCubeNodeIdFactory) nodeAccessor.getNodeIdFactory();
		
		
		String nextHopSelectorKey = properties.getProperty(PROP_KEY_NEXT_HOP_SELECTOR_KEY);
		if (nextHopSelectorKey == null || nextHopSelectorKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_NEXT_HOP_SELECTOR_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_NEXT_HOP_SELECTOR_KEY));
		nextHopSelector = (NextHopSelector) nodeAccessor.getNextHopSelector(nextHopSelectorKey);
		
		try {
			
			this.defaultAlpha = (Short) properties.getProperty(PROP_KEY_DEFAULT_ALPHA, MappedType.SHORT);
			this.defaultBeta = (Short) properties.getProperty(PROP_KEY_DEFAULT_BETA, MappedType.SHORT);
			this.defaultGamma = (Short) properties.getProperty(PROP_KEY_DEFAULT_GAMMA, MappedType.SHORT);
			
			this.metric = (Metric) properties.getEnumProperty(PROP_KEY_METRIC, Metric.class);
			
			this.useSteinhausTransform = (Boolean) properties.getProperty(PROP_KEY_USE_STEINHAUS_TRANSFORM, MappedType.BOOLEAN);
			
			this.requestTimeout = (Integer) properties.getProperty(PROP_KEY_REQUEST_TIMEOUT, MappedType.INT);
			
			
			
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize the search manager instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		

		
		
		searchesData = new HashMap<Integer, HyCubeSearchData>(HashMapUtils.getHashMapCapacityForElementsNum(GlobalConstants.INITIAL_SEARCHES_DATA_COLLECTION_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		

		String searchCallbackEventTypeKey = properties.getProperty(PROP_KEY_SEARCH_CALLBACK_EVENT_KEY);
		searchCallbackEventType = new EventType(EventCategory.extEvent, searchCallbackEventTypeKey);
		
		String searchRequestTimeoutEventTypeKey = properties.getProperty(PROP_KEY_SEARCH_REQUEST_TIMEOUT_EVENT_KEY);
		searchRequestTimeoutEventType = new EventType(EventCategory.extEvent, searchRequestTimeoutEventTypeKey);
		
		searchRequestTimeoutEventProxy = new SearchRequestTimeoutEventProxy();
		
		
		
	}

	
	
	@Override
	public SearchCallback search(NodeId searchNodeId, short k, SearchCallback searchCallback, Object callbackArg) {
		return search(searchNodeId, null, k, searchCallback, callbackArg);
	}
	
	@Override
	public SearchCallback search(NodeId searchNodeId, NodePointer[] initialNodes, short k, SearchCallback searchCallback, Object callbackArg) {
		return search(searchNodeId, initialNodes, k, DEFAULT_IGNORE_TARGET_NODE, searchCallback, callbackArg);
	}
	

	@Override
	public SearchCallback search(NodeId searchNodeId, short k, boolean ignoreTargetNode, SearchCallback searchCallback, Object callbackArg) {
		return search(searchNodeId, null, k, ignoreTargetNode, searchCallback, callbackArg);
	}
	
	@Override
	public SearchCallback search(NodeId searchNodeId, NodePointer[] initialNodes, short k, boolean ignoreTargetNode, SearchCallback searchCallback, Object callbackArg) {
		return search(searchNodeId, initialNodes, k, ignoreTargetNode, searchCallback, callbackArg, defaultAlpha, defaultBeta, defaultGamma, false, false);
	}
	
	@Override
	public SearchCallback search(NodeId searchNodeId, short k, SearchCallback searchCallback, Object callbackArg, Object[] parameters) {
		return search(searchNodeId, null, k, searchCallback, callbackArg, parameters);
	}
	
	@Override
	public SearchCallback search(NodeId searchNodeId, NodePointer[] initialNodes, short k, SearchCallback searchCallback, Object callbackArg, Object[] parameters) {
		return search(searchNodeId, initialNodes, k, DEFAULT_IGNORE_TARGET_NODE, searchCallback, callbackArg, parameters);
	}
	

	@Override
	public SearchCallback search(NodeId searchNodeId, short k, boolean ignoreTargetNode, SearchCallback searchCallback, Object callbackArg, Object[] parameters) {
		return search(searchNodeId, null, k, ignoreTargetNode, searchCallback, callbackArg, parameters);
	}
	
	@Override
	public SearchCallback search(NodeId searchNodeId, NodePointer[] initialNodes, short k, boolean ignoreTargetNode, SearchCallback searchCallback, Object callbackArg, Object[] parameters) {
		
		short alpha = defaultAlpha;
		short beta = defaultBeta;
		short gamma = defaultGamma;

		boolean secureSearch = false;
		boolean skipRandomNextHops = false;
		
		if (parameters != null) {

			short paramAlpha = getSearchParameterAlpha(parameters);
			short paramBeta = getSearchParameterBeta(parameters);
			short paramGamma = getSearchParameterGamma(parameters);
			
			if (paramAlpha != 0) alpha = paramAlpha;
			if (paramBeta != 0) beta = paramBeta;
			if (paramGamma != 0) gamma = paramGamma;

			secureSearch = getSearchParameterSecureSearch(parameters);
			
			skipRandomNextHops = getSearchParameterSkipRandomNextHops(parameters);
			
			
		}
		
		return search(searchNodeId, initialNodes, k, ignoreTargetNode, searchCallback, callbackArg, alpha, beta, gamma, secureSearch, skipRandomNextHops);
		
	}
	
	public SearchCallback search(NodeId searchNodeId, short k, SearchCallback searchCallback, Object callbackArg, short alpha, short beta, short gamma, boolean secureSearch, boolean skipRandomNextHops) {
		return search(searchNodeId, null, k, searchCallback, callbackArg, alpha, beta, gamma, secureSearch, skipRandomNextHops);
	}
	
	public SearchCallback search(NodeId searchNodeId, NodePointer[] initialNodes, short k, SearchCallback searchCallback, Object callbackArg, short alpha, short beta, short gamma, boolean secureSearch, boolean skipRandomNextHops) {
		return search(searchNodeId, initialNodes, k, DEFAULT_IGNORE_TARGET_NODE, searchCallback, callbackArg, alpha, beta, gamma, secureSearch, skipRandomNextHops);
	}
	
	public SearchCallback search(NodeId searchNodeId, short k, boolean ignoreTargetNode, SearchCallback searchCallback, Object callbackArg, short alpha, short beta, short gamma, boolean secureSearch, boolean skipRandomNextHops) {
		return search(searchNodeId, null, k, ignoreTargetNode, searchCallback, callbackArg, alpha, beta, gamma, secureSearch, skipRandomNextHops);
	}
	
	public SearchCallback search(NodeId searchNodeId, NodePointer[] initialNodes, short k, boolean ignoreTargetNode, SearchCallback searchCallback, Object callbackArg, short alpha, short beta, short gamma, boolean secureSearch, boolean skipRandomNextHops) {
		if (searchNodeId instanceof HyCubeNodeId) {
			return search((HyCubeNodeId)searchNodeId, initialNodes, k, ignoreTargetNode, searchCallback, callbackArg, alpha, beta, gamma, secureSearch, skipRandomNextHops);
		}
		else {
			throw new IllegalArgumentException("The search node id should be an instance of: " + HyCubeNodeId.class.getName());
		}
		
	}

	public SearchCallback search(HyCubeNodeId searchNodeId, short k, SearchCallback searchCallback, Object callbackArg, short alpha, short beta, short gamma, boolean secureSearch, boolean skipRandomNextHops) {
		return search(searchNodeId, null, k, searchCallback, callbackArg, alpha, beta, gamma, secureSearch, skipRandomNextHops);
	}
	
	public SearchCallback search(HyCubeNodeId searchNodeId, NodePointer[] initialNodes, short k, SearchCallback searchCallback, Object callbackArg, short alpha, short beta, short gamma, boolean secureSearch, boolean skipRandomNextHops) {
		return search(searchNodeId, initialNodes, k, DEFAULT_IGNORE_TARGET_NODE, searchCallback, callbackArg, alpha, beta, gamma, secureSearch, skipRandomNextHops);
	}
	
	public SearchCallback search(HyCubeNodeId searchNodeId, short k, boolean ignoreTargetNode, SearchCallback searchCallback, Object callbackArg, short alpha, short beta, short gamma, boolean secureSearch, boolean skipRandomNextHops) {
		return search(searchNodeId, null, k, ignoreTargetNode, searchCallback, callbackArg, alpha, beta, gamma, secureSearch, skipRandomNextHops);
	}
	
	public SearchCallback search(HyCubeNodeId searchNodeId, NodePointer[] initialNodes, short k, boolean ignoreTargetNode, SearchCallback searchCallback, Object callbackArg, short alpha, short beta, short gamma, boolean secureSearch, boolean skipRandomNextHops) {
		
		synchronized(searchManagerLock) {
		
			//generate the search id
			int searchId = getNextSearchId();
			
			
			//if exists, discard previous search with the same id:   (the range of int is big enough, so it will not limit the number of concurrent search operations)
			HyCubeSearchData oldSearchData = null;
			synchronized (searchManagerLock) {
				if (searchesData.containsKey(searchId)) {
					oldSearchData = searchesData.get(searchId);
					searchesData.remove(searchId);
				}
			}
			if (oldSearchData != null) oldSearchData.discard();
			
			
			
			//if k is greater than beta or gamma, update
			if (k > beta) beta = k;
			if (k > gamma) gamma = k;
			
			
			//prepare the search structure
			HyCubeSearchData searchData = new HyCubeSearchData(searchId, searchNodeId, k, ignoreTargetNode, alpha, beta, gamma, secureSearch, skipRandomNextHops);
			
			searchesData.put(searchId, searchData);
			
			
			//registerSearchCallback
			searchData.setSearchCallback(searchCallback);
			searchData.setCallbackArg(callbackArg);
			
			
			//send the search request:
			
			
			HyCubeSearchNextHopSelectionParameters parameters = createDefaultSearchParameters(ignoreTargetNode, beta);
			
			parameters.setSteinhausPoint(null);
			
			if (searchNodeId.equals(nodeId)) {
				//this is necessary, otherwise the Steinhaus point would equal the searchNodeId, which would cause all the distances to the destination to equal 1
				parameters.setSteinhausTransformApplied(false);
			}
			
			if (parameters.isSteinhausTransformApplied()) {
				parameters.setSteinhausPoint(nodeId);
			}

			parameters.setSecureRoutingApplied(searchData.secureSearch);
			parameters.setSkipRandomNumOfNodesApplied(searchData.skipRandomNextHops);
			
			
			
			
			if (initialNodes == null || initialNodes.length == 0) {
				
				//just for initial local search, include self in results (self might be one of the k closest)
				parameters.setIncludeSelf(true);
				
				initialNodes = nextHopSelector.findNextHops(searchNodeId, parameters, searchData.closestNodesStoredNum);
				
				//the nodes requested should not return self:
				parameters.setIncludeSelf(false);
				
			}
			
			
			
			processNewNodes(searchData, initialNodes, parameters);						
			
			//and process the search list:
			processSearch(searchData);
		
			
		}
		
		return searchCallback;
		
	}
	
	
	protected int getNextSearchId() {
		synchronized (searchManagerLock) {
			int searchId = nextSearchId;
			nextSearchId++;
			return searchId;
		}
	}




	public void discard() {
		synchronized (searchManagerLock) {
			for (Entry<Integer, HyCubeSearchData> entry : searchesData.entrySet()) {
				int searchId = entry.getKey();
				HyCubeSearchData searchData = entry.getValue();
				enqueueCallbackEvent(searchData.searchCallback, searchData.callbackArg, searchId, null);
				searchData.discard();
			}
		}
		
	}


	@Override
	public EventType getSearchCallbackEventType() {
		return searchCallbackEventType;
	}

	
	@Override
	public EventType getSearchRequestTimeoutEventType() {
		return searchRequestTimeoutEventType;
	}
	

	protected void enqueueCallbackEvent(SearchCallback searchCallback, Object callbackArg, int searchId, NodePointer[] result) {
		//create the event
		Event event = new SearchCallbackEvent(this, searchCallback, callbackArg, searchId, result);
		
		//insert to the appropriate event queue
		try {
			nodeAccessor.getEventQueue(searchCallbackEventType).put(event);
		} catch (InterruptedException e) {
			//this should never happen
			throw new UnrecoverableRuntimeException("An exception was thrown while inserting an event to an event queue.");
		}
		
	}
	
	
	
	
	public void processSearchRequest(NodePointer sender, int searchId, NodeId searchNodeId, HyCubeSearchNextHopSelectionParameters searchParameters) {
		
		//find best nodes according to the specified parameters

		//use the search parameters specified by the requestor:
		HyCubeSearchNextHopSelectionParameters parameters = searchParameters;
		
		short beta = searchParameters.getBeta();
		
		if (searchNodeId.equals(nodeId)) {
			//this is necessary, otherwise the Steinhaus point would equal the searchNodeId, which would cause all the distances to the destination to equal 1
			parameters.setSteinhausTransformApplied(false);
			parameters.setSteinhausPoint(null);
		}
		
		NodePointer[] nodesFound = nextHopSelector.findNextHops(searchNodeId, parameters, beta);
				
		
		//send response to the sender:
		sendSearchResponse(searchId, sender, parameters, nodesFound);
		
		
	}
	
	
	public void processSearchResponse(NodePointer sender, int searchId, HyCubeSearchNextHopSelectionParameters searchParameters, NodePointer[] nodesFound) {
		
		HyCubeSearchData searchData;
		synchronized(searchManagerLock) {
			if (searchesData.containsKey(searchId)) {
				searchData = searchesData.get(searchId);
			}
			else return;	//the search has already been terminated, or the search id is wrong
		}
		
		
		synchronized (searchData) {
		
			//update the parameters used by the node returning the search results:
			searchData.pmhApplied.put(sender.getNodeIdHash(), searchParameters.isPMHApplied());
			searchData.steinhausTransformApplied.put(sender.getNodeIdHash(), searchParameters.isSteinhausTransformApplied());
			searchData.steinhausPoints.put(sender.getNodeIdHash(), searchParameters.getSteinhausPoint());

			
			
			if ((! searchParameters.finalSearch) && (! searchData.finalSearch)) {
				if (searchData.nodesRequested.contains(sender.getNodeIdHash())) {
					
					searchData.nodesResponded.add(sender.getNodeIdHash());
					
					processNewNodes(searchData, nodesFound, searchParameters);
					
				}
				
			}
			if (searchParameters.finalSearch && searchData.finalSearch) {	//final search
				if (searchData.nodesRequestedFinal.contains(sender.getNodeIdHash())) {
					
					searchData.nodesRespondedFinal.add(sender.getNodeIdHash());
					
					processNewNodes(searchData, nodesFound, searchParameters);
					
					
				}
				
			}
			
			
			
			//and process the updated search data
			processSearch(searchData);
			
		}


	}
	
	
	
	public void processNewNodes(HyCubeSearchData searchData, NodePointer[] newNodes, HyCubeSearchNextHopSelectionParameters searchParameters) {
		//update the search data structure with the response
		//assume optimistically that the nodes are closer and already sorted with the same metric and start inserting from the last element 
		for (int i = newNodes.length - 1; i >=0; i--) {
			NodePointer np = newNodes[i];
			
			if (searchData.closestNodesSet.contains(np.getNodeIdHash())) {
				//ignore that node, it is already in the closest nodes collection
				continue;
			}
			
			if (! (np.getNodeId() instanceof HyCubeNodeId)) throw new IllegalArgumentException("The search node id should be an instance of: " + HyCubeNodeId.class.getName());
			double dist = HyCubeNodeId.calculateDistance((HyCubeNodeId)np.getNodeId(), searchData.searchNodeId, metric);
			
			int insertIndex = 0;
			
			//ListIterator<NodePointer> nodeIter = searchData.closestNodes.listIterator();
			ListIterator<Double> distIter = searchData.distances.listIterator();
			
			//while (nodeIter.hasNext()) {
			while (distIter.hasNext()) {
				//NodePointer nextNode = nodeIter.next();
				double nextDist = distIter.next();
				if (dist < nextDist) break;
				else insertIndex++;
			}

			if (insertIndex == searchData.closestNodesStoredNum) {
				//skip this node - we store only closestNodesStoredNum closest nodes found
				continue;
			}
			else {
				if (searchData.closestNodes.size() == searchData.closestNodesStoredNum) {
					//the closest nodes collection already contains closestNodesStoredNum (number of closed nodes stored) nodes, so we should remove the last element
					NodePointer nodeRemoved = searchData.closestNodes.removeLast();
					searchData.distances.removeLast();
					searchData.closestNodesSet.remove(nodeRemoved.getNodeIdHash());
					searchData.pmhApplied.remove(nodeRemoved.getNodeIdHash());
					searchData.steinhausTransformApplied.remove(nodeRemoved.getNodeIdHash());
					searchData.steinhausPoints.remove(nodeRemoved.getNodeIdHash());
				}
			}
			
			searchData.closestNodes.add(insertIndex, np);
			searchData.distances.add(insertIndex, dist);
			
			searchData.closestNodesSet.add(np.getNodeIdHash());
			
			//set the parameters for this virtual route
			searchData.pmhApplied.put(np.getNodeIdHash(), searchParameters.isPMHApplied());
			searchData.steinhausTransformApplied.put(np.getNodeIdHash(), searchParameters.isSteinhausTransformApplied());
			searchData.steinhausPoints.put(np.getNodeIdHash(), searchParameters.getSteinhausPoint());
			
			
			
			
		}
	}
	
	
	public void requestTimedOut(int searchId, long nodeIdHash, boolean finalSearch) {
		
		HyCubeSearchData searchData;
		synchronized(searchManagerLock) {
			if (searchesData.containsKey(searchId)) {
				searchData = searchesData.get(searchId);
			}
			else return;	//the search has already been terminated
		}
		
		
		synchronized (searchData) {
			
			//mark the node (for which the response time limit was reached) as "responded"
			if (! finalSearch) {
				if (searchData.nodesRequested.contains(nodeIdHash)) {
					searchData.nodesResponded.add(nodeIdHash);
				}
			}
			else {	//final search
				if (searchData.nodesRequestedFinal.contains(nodeIdHash)) {
					searchData.nodesRespondedFinal.add(nodeIdHash);
				}
			}
		
			
			//and process the search data:
			processSearch(searchData);
			
		}
		
		
	}
	
	
	protected void processSearch(HyCubeSearchData searchData) {
		
		//go through the nodes list and continue the search procedure
	
		synchronized (searchData) {
		
			if (! searchData.finalSearch) {
				//if alpha closest nodes contain self, mark it as already requested and responded (the local search was done in the initial step of the search procedure)

				ListIterator<NodePointer> nodeIter = searchData.closestNodes.listIterator();
				while (nodeIter.hasNext() && nodeIter.nextIndex() < searchData.alpha) { 
					NodePointer np = nodeIter.next();
					if (np.getNodeId().equals(nodeId)) {
						if (! searchData.nodesResponded.contains(np.getNodeIdHash())) {
							searchData.nodesRequested.add(np.getNodeIdHash());
							searchData.nodesResponded.add(np.getNodeIdHash());
						}
						break;
					}
				}
			}
			else {	//final search
				//if gamma closest nodes contain self and it is not marked as requested (and responded), perform a local search and mark it as already requested and responded
				
				ListIterator<NodePointer> nodeIter = searchData.closestNodes.listIterator();
				while (nodeIter.hasNext() && nodeIter.nextIndex() < searchData.gamma) { 
					NodePointer np = nodeIter.next();
					if (np.getNodeId().equals(nodeId)) {
						if (! searchData.nodesRespondedFinal.contains(np.getNodeIdHash())) {
							
							//prepare the search parameters:
							HyCubeSearchNextHopSelectionParameters parameters = createDefaultSearchParameters(searchData.ignoreTargetNode, searchData.beta);
							//set the request parameters (the values for this virtual route):
							parameters.setFinalSearch(true);
							parameters.setPMHApplied(true);
							parameters.setSteinhausTransformApplied(false);
							parameters.setSteinhausPoint(null);
							parameters.setSecureRoutingApplied(searchData.secureSearch);
							parameters.setSkipRandomNumOfNodesApplied(searchData.skipRandomNextHops);
							
							NodePointer[] newNodes = nextHopSelector.findNextHops(searchData.searchNodeId, parameters, searchData.beta);
							
							processNewNodes(searchData, newNodes, parameters);
							
							
							//mark the node as requested/responded in the final phase
							searchData.nodesRequestedFinal.add(np.getNodeIdHash());
							searchData.nodesRespondedFinal.add(np.getNodeIdHash());
							
						}
						break;
					}
				}
				
			}
			
			
				
			if (! searchData.finalSearch) {
				
				boolean switchToFinalSearch = true;
				ListIterator<NodePointer> nodeIter = searchData.closestNodes.listIterator();
				while (nodeIter.hasNext() && nodeIter.nextIndex() < searchData.alpha) { 
					NodePointer np = nodeIter.next();
					if (! searchData.nodesResponded.contains(np.getNodeIdHash())) {
						switchToFinalSearch = false;
						break;
					}
				}

				if (switchToFinalSearch) {
				
					//enable final search option:
					searchData.finalSearch = true;
	
					//set all the nodes that were already requested with the settings used for final search
					//if the Steinhaus transform is enabled, there should be no such nodes
					// - because nodes may return more distant nodes than themselves, there would never be a situation that no next hop is returned 
					//   (Steinhaus transform disabling condition) - only when the routing tables are completely empty,
					//   in which case next hop selection according to Euclidean metric would also not return any nodes
					for (long nodeIdHash : searchData.closestNodesSet) {
						if (searchData.nodesRequested.contains(nodeIdHash) 
								&& searchData.steinhausTransformApplied.get(nodeIdHash) == false
								&& searchData.pmhApplied.get(nodeIdHash) == true) {
	
							searchData.nodesRequestedFinal.add(nodeIdHash);
							searchData.nodesRespondedFinal.add(nodeIdHash);
						}
					}

				}
				

			}
			if (searchData.finalSearch) {
				
				boolean searchFinished = true;
				ListIterator<NodePointer> nodeIter = searchData.closestNodes.listIterator();
				while (nodeIter.hasNext() && nodeIter.nextIndex() < searchData.gamma) { 
					NodePointer np = nodeIter.next();
					if (! searchData.nodesRespondedFinal.contains(np.getNodeIdHash())) {
						searchFinished = false;
						break;
					}
				}
				
				if (searchFinished) {
				
					//this search procedure is finished
	
					//prepare the result - get an array containing first k nodes found
					int nodesFoundNum = 0;
					if (searchData.k <= searchData.closestNodes.size()) nodesFoundNum = searchData.k;
					else nodesFoundNum = searchData.closestNodes.size();
					NodePointer[] nodesFound = new NodePointer[nodesFoundNum];
					nodesFound = searchData.closestNodes.subList(0, nodesFoundNum).toArray(nodesFound);
	
	
					//enqueue the callback event
					enqueueCallbackEvent(searchData.searchCallback, searchData.callbackArg, searchData.searchId, nodesFound);
	
					//remove this search from the searches list:
					synchronized (searchManagerLock) {
						searchesData.remove(searchData.searchId);
					}
					
					return;
					
				}
				

			}

			
			
			if (! searchData.finalSearch) {
				//find closest nodes to which the request has not yet been sent (within first alpha nodes):
				LinkedList<NodePointer> reqNodes = new LinkedList<NodePointer>();
				ListIterator<NodePointer> nodeIter = searchData.closestNodes.listIterator();
				while (nodeIter.hasNext() && nodeIter.nextIndex() < searchData.alpha) {		//in first phase (finalSearch = false), a request should be sent to max. alpha closest nodes 
					NodePointer np = nodeIter.next();
					if (! searchData.nodesRequested.contains(np.getNodeIdHash())) {
						//this node is within the first alpha closest nodes and has not yet been requested
						reqNodes.add(np);
					}
				}
				
				for (NodePointer reqNode : reqNodes) {
					
					//prepare the search parameters:
					HyCubeSearchNextHopSelectionParameters parameters = createDefaultSearchParameters(searchData.ignoreTargetNode, searchData.beta);
					
					//set the request parameters (the values for this virtual route):
					parameters.setPMHApplied(searchData.pmhApplied.get(reqNode.getNodeIdHash()));
					parameters.setSteinhausTransformApplied(searchData.steinhausTransformApplied.get(reqNode.getNodeIdHash()));
					
//					//if PMH is applied, do not use Steinhaus transform anymore:
//					if (parameters.isPMHApplied()) parameters.setSteinhausTransformApplied(false);
//					else parameters.setSteinhausTransformApplied(searchData.steinhausTransformApplied.get(reqNode.getNodeIdHash()));
					
					
					if (parameters.isSteinhausTransformApplied()) parameters.setSteinhausPoint(searchData.steinhausPoints.get(reqNode.getNodeIdHash()));
					
					parameters.setSecureRoutingApplied(searchData.secureSearch);
					parameters.setSkipRandomNumOfNodesApplied(searchData.skipRandomNextHops);
					
					//send the search request to this node:
					sendSearchRequest(searchData.searchId, reqNode, searchData.searchNodeId, parameters);
					
					//mark this node as requested:
					searchData.nodesRequested.add(reqNode.getNodeIdHash());
					
				}
				
			}
			
			else {	//final search:
				//find first (closest) nodes to which the request has not yet been sent:
				LinkedList<NodePointer> reqNodes = new LinkedList<NodePointer>();
				ListIterator<NodePointer> nodeIter = searchData.closestNodes.listIterator();
				while (nodeIter.hasNext() && nodeIter.nextIndex() < searchData.gamma) {		//in second phase (finalSearch = true), a request should be sent to max. gamma closest nodes
					NodePointer np = nodeIter.next();
					if (! searchData.nodesRequestedFinal.contains(np.getNodeIdHash())) {
						//this node is withing the first alpha closest nodes and has not yet been requested
						reqNodes.add(np);
					}
				}
				
				for (NodePointer reqNode : reqNodes) {
					//prepare the search parameters:
					HyCubeSearchNextHopSelectionParameters parameters = createDefaultSearchParameters(searchData.ignoreTargetNode, searchData.beta);
					//set the request parameters (the values for this virtual route):
					parameters.setFinalSearch(true);
					parameters.setPMHApplied(true);
					parameters.setSteinhausTransformApplied(false);
					parameters.setSteinhausPoint(null);
					parameters.setSecureRoutingApplied(searchData.secureSearch);
					parameters.setSkipRandomNumOfNodesApplied(searchData.skipRandomNextHops);
					
					
					//send the search request to this node:
					sendSearchRequest(searchData.searchId, reqNode, searchData.searchNodeId, parameters);
					
					//mark this node as requested:
					searchData.nodesRequestedFinal.add(reqNode.getNodeIdHash());
				}
			}
			
			
		}
		
		
	}
	
	
	protected void sendSearchRequest(int searchId, NodePointer recipient, HyCubeNodeId searchNodeId, HyCubeSearchNextHopSelectionParameters searchParameters) {
		
		//prepare the message:
		
		int messageSerialNo = nodeAccessor.getNextMessageSerialNo();
		byte[] searchMessageData = (new HyCubeSearchMessageData(searchId, searchNodeId, searchParameters)).getBytes(nodeIdFactory.getDimensions(), nodeIdFactory.getDigitsCount());
		Message searchMessage = messageFactory.newMessage(messageSerialNo, nodeAccessor.getNodeId(), recipient.getNodeId(), nodeAccessor.getNetworkAdapter().getPublicAddressBytes(), HyCubeMessageType.SEARCH, nodeAccessor.getNodeParameterSet().getMessageTTL(), (short)0, false, false, (short)0, (short)0, searchMessageData); 

		
		//send the message directly to the recipient:
		try {
			nodeAccessor.sendMessage(new MessageSendProcessInfo(searchMessage, recipient.getNetworkNodePointer(), false), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
		} catch (NetworkAdapterException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a search request to a node.", e);
		} catch (ProcessMessageException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a search request to a node.", e);
		}
		
		
		//schedule the request time out event
		scheduleRequestTimeout(searchId, recipient.getNodeIdHash(), searchParameters.finalSearch);
		
		
	}
	

	protected void sendSearchResponse(int searchId, NodePointer recipient, HyCubeSearchNextHopSelectionParameters searchParameters, NodePointer[] nodesFound) {
		
		//prepare the message:
		
		int messageSerialNo = nodeAccessor.getNextMessageSerialNo();
		byte[] searchReplyMessageData = (new HyCubeSearchReplyMessageData(searchId, searchParameters, nodesFound)).getBytes(nodeIdFactory.getDimensions(), nodeIdFactory.getDigitsCount()); 
		Message searchReplyMessage = messageFactory.newMessage(messageSerialNo, nodeAccessor.getNodeId(), recipient.getNodeId(), nodeAccessor.getNetworkAdapter().getPublicAddressBytes(), HyCubeMessageType.SEARCH_REPLY, nodeAccessor.getNodeParameterSet().getMessageTTL(), (short)0, false, false, (short)0, (short)0, searchReplyMessageData); 

		
		//send the message directly to the recipient:
		try {
			nodeAccessor.sendMessage(new MessageSendProcessInfo(searchReplyMessage, recipient.getNetworkNodePointer(), false), GlobalConstants.WAIT_ON_BKG_MSG_SEND);
		} catch (NetworkAdapterException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a search response to a node.", e);
		} catch (ProcessMessageException e) {
			throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a search response to a node.", e);
		}

		
	}
	
	
	protected void scheduleRequestTimeout(int searchId, long nodeIdHash, boolean finalSearch) {
		enqueueSearchRequestTimeoutEvent(searchId, nodeIdHash, finalSearch);
		
		
	}
	
	protected void enqueueSearchRequestTimeoutEvent(int searchId, long nodeIdHash, boolean finalSearch) {
		
		//create the event:
		Event event = new SearchRequestTimeoutEvent(this, searchRequestTimeoutEventProxy, searchId, nodeIdHash, finalSearch);
		
		//schedule the event:
		Queue<Event> queue = nodeAccessor.getEventQueue(searchRequestTimeoutEventType);
		EventScheduler scheduler = nodeAccessor.getEventScheduler();
		scheduler.scheduleEventWithDelay(event, queue, requestTimeout);
		
		
	}
	
	
	public class SearchRequestTimeoutEventProxy implements ProcessEventProxy {
		@Override
		public void processEvent(Event event) throws EventProcessException {
			if (! (event instanceof SearchRequestTimeoutEvent)) throw new EventProcessException("Invalid event type scheduled to be processed by " + HyCubeSearchManager.class.getName() + ". The expected event class: " + SearchRequestTimeoutEvent.class.getName() + ".");
			SearchRequestTimeoutEvent searchEvent = (SearchRequestTimeoutEvent)event;
			try {
				requestTimedOut(searchEvent.searchId, searchEvent.nodeIdHash, searchEvent.finalSearch);
			}
			catch (Exception e) {
				throw new EventProcessException("An exception was thrown while processing a search request timeout event.", e);
			}
		}
	}
	
	

	
	
	
	protected HyCubeSearchNextHopSelectionParameters createDefaultSearchParameters() {
		
		HyCubeSearchNextHopSelectionParameters parameters = new HyCubeSearchNextHopSelectionParameters();
		
		parameters.setIncludeMoreDistantNodes(true);
		parameters.setPMHApplied(false);
		parameters.setPreventPMH(false);
		parameters.setSecureRoutingApplied(false);
		parameters.setSkipRandomNumOfNodesApplied(false);
		parameters.setSkipTargetNode(DEFAULT_IGNORE_TARGET_NODE);
		parameters.setSteinhausTransformApplied(useSteinhausTransform);
		parameters.setSteinhausPoint(null);
		parameters.setFinalSearch(false);
		
		return parameters;
		
	}
	
	
	protected HyCubeSearchNextHopSelectionParameters createDefaultSearchParameters(boolean ignoreTargetNode, short beta) {
		
		HyCubeSearchNextHopSelectionParameters parameters = createDefaultSearchParameters();
		
		parameters.setBeta(beta);
		
		parameters.setSkipTargetNode(ignoreTargetNode);
		
		return parameters;
		
	}



	@Override
	public EntryPoint getEntryPoint() {
		return null;
	}
	
	
	
	public static Object[] createSearchParameters(Short alpha, Short beta, Short gamma, Boolean secureSearch, Boolean skipRandomNextHops) {
		Object[] parameters = new Object[] {alpha, beta, gamma, secureSearch, skipRandomNextHops};
		return parameters;
	}
	
	public static short getSearchParameterAlpha(Object[] parameters) {
		if (parameters == null) return 0;
		if (! (parameters.length > 0) || (!(parameters[0] instanceof Short))) return 0;
		return (Short)parameters[0];
	}
	
	public static short getSearchParameterBeta(Object[] parameters) {
		if (parameters == null) return 0;
		if (! (parameters.length > 1) || (!(parameters[1] instanceof Short))) return 0;
		return (Short)parameters[1];
	}

	public static short getSearchParameterGamma(Object[] parameters) {
		if (parameters == null) return 0;
		if (! (parameters.length > 2) || (!(parameters[2] instanceof Short))) return 0;
		return (Short)parameters[2];
	}
	
	public static boolean getSearchParameterSecureSearch(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 3) || (!(parameters[3] instanceof Boolean))) return false;
		return (Boolean)parameters[3];
	}
	
	public static boolean getSearchParameterSkipRandomNextHops(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 4) || (!(parameters[4] instanceof Boolean))) return false;
		return (Boolean)parameters[4];
	}
	
	
}
