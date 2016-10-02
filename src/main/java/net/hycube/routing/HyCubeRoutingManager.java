package net.hycube.routing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Random;

import net.hycube.common.EntryPoint;
import net.hycube.configuration.GlobalConstants;
import net.hycube.core.HyCubeNodeId;
import net.hycube.core.HyCubeRoutingTable;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodeId;
import net.hycube.core.NodePointer;
import net.hycube.core.RoutingTable;
import net.hycube.core.RoutingTableEntry;
import net.hycube.environment.NodeProperties;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.logging.LogHelper;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.Message;
import net.hycube.messaging.processing.MessageSendProcessInfo;
import net.hycube.messaging.processing.ProcessMessageException;
import net.hycube.metric.Metric;
import net.hycube.nexthopselection.HyCubeNextHopSelectionParameters;
import net.hycube.nexthopselection.NextHopSelector;
import net.hycube.transport.NetworkAdapterException;
import net.hycube.transport.NetworkNodePointer;
import net.hycube.utils.HashMapUtils;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public class HyCubeRoutingManager implements RoutingManager {

	
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeRoutingManager.class);
	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
	
	protected static final String PROP_KEY_NEXT_HOP_SELECTOR_KEY = "NextHopSelectorKey";
	protected static final String PROP_KEY_USE_STEINHAUS_TRANSFORM = "UseSteinhausTransform";
	protected static final String PROP_KEY_REGISTERED_ROUTES_RETENTION_TIME = "RegisteredRoutesRetentionTime";
	protected static final String PROP_KEY_ALLOW_REGISTERED_ROUTES = "AllowRegisteredRoutes";
	protected static final String PROP_KEY_ALLOW_ANONYMOUS_ROUTES = "AllowAnonymousRoutes";
	protected static final String PROP_KEY_CONCEAL_TTL = "ConcealTTL";
	protected static final String PROP_KEY_DECREASE_TTL_PROBABILITY = "DecreaseTTLProbability";
	protected static final String PROP_KEY_INCREASE_TTL_BY_RANDOM_NUM = "IncreaseTTLByRandomNum";
	protected static final String PROP_KEY_INCREASE_TTL_RANDOM_MEAN = "IncreaseTTLRandomMean";
	protected static final String PROP_KEY_INCREASE_TTL_RANDOM_STD_DEV = "IncreaseTTLRandomStdDev";
	protected static final String PROP_KEY_INCREASE_TTL_RANDOM_ABSOLUTE = "IncreaseTTLRandomAbsolute";
	protected static final String PROP_KEY_INCREASE_TTL_RANDOM_MODULO = "IncreaseTTLRandomModulo";
	protected static final String PROP_KEY_CONCEAL_HOP_COUNT = "ConcealHopCount";
	protected static final String PROP_KEY_ENSURE_STEINHAUS_POINT_ANONYMITY = "EnsureSteinhausPointAnonymity";
	protected static final String PROP_KEY_STEINHAUS_POINT_ANONYMITY_DISTANCE_FACTOR = "SteinhausPointAnonymityDistanceFactor";
	
	
	public static final short HOP_COUNT_MASK_VALUE = Short.MAX_VALUE;
	
	
	protected boolean initialized = false;

	
	protected NodeAccessor nodeAccessor;
	protected NodeProperties properties;
	
	
	protected NodeId nodeId;
	protected RoutingTable routingTable;
	
	
	protected String nextHopSelectorKey;
	protected NextHopSelector nextHopSelector;
	
	protected boolean useSteinhausTransform;
	
	
	protected Random rand;
	
	
	protected boolean allowRegisteredRoutes;
	protected boolean allowAnonymousRoutes;
	
	protected HashMap<Integer, HyCubeRegisteredRouteInfo> registeredRoutesByOutRouteId;
	protected LinkedList<HyCubeRegisteredRouteInfo> registeredRoutesByOutRouteIdOrdered;
	
	
	protected int registeredRoutesRetentionTime;
	
	protected boolean concealTTL;
	protected double decreaseTTLProbability;
	protected boolean increaseTTLByRandomNum;
	protected double increaseTTLRandomMean;
	protected double increaseTTLRandomStdDev;
	protected boolean increaseTTLRandomAbsolute;
	protected int increaseTTLRandomModulo;
	
	protected boolean concealHopCount;
	
	protected boolean ensureSteinhausPointAnonymity;
	protected double steinhausPointAnonymityDistanceFactor;
	
	
	
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		this.nodeAccessor = nodeAccessor;
		this.properties = properties;
		
		this.nodeId = nodeAccessor.getNodeId();
		this.routingTable = nodeAccessor.getRoutingTable();
		
		this.properties = properties;
		
		
		this.initialized = true;
		
		
		this.nextHopSelectorKey = properties.getProperty(PROP_KEY_NEXT_HOP_SELECTOR_KEY);
		if (nextHopSelectorKey == null || nextHopSelectorKey.trim().isEmpty()) {
			throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_NEXT_HOP_SELECTOR_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_NEXT_HOP_SELECTOR_KEY) + ". Unable to initialize the routing manager instance.");
		}
		this.nextHopSelector = (NextHopSelector) nodeAccessor.getNextHopSelector(nextHopSelectorKey);
		
		try {
			
			if (properties.containsKey(PROP_KEY_USE_STEINHAUS_TRANSFORM)) {
				this.useSteinhausTransform = (Boolean) properties.getProperty(PROP_KEY_USE_STEINHAUS_TRANSFORM, MappedType.BOOLEAN);
			}
			else {
				this.useSteinhausTransform = false;
			}
			
			
			this.allowRegisteredRoutes = (Boolean) properties.getProperty(PROP_KEY_ALLOW_REGISTERED_ROUTES, MappedType.BOOLEAN);
			
			this.registeredRoutesRetentionTime = (Integer) properties.getProperty(PROP_KEY_REGISTERED_ROUTES_RETENTION_TIME, MappedType.INT);
			
			
			this.allowAnonymousRoutes= (Boolean) properties.getProperty(PROP_KEY_ALLOW_ANONYMOUS_ROUTES, MappedType.BOOLEAN);
			
			
			concealTTL = (Boolean) properties.getProperty(PROP_KEY_CONCEAL_TTL, MappedType.BOOLEAN);
			
			decreaseTTLProbability = (Double)  properties.getProperty(PROP_KEY_DECREASE_TTL_PROBABILITY, MappedType.DOUBLE);
			
			increaseTTLByRandomNum = (Boolean) properties.getProperty(PROP_KEY_INCREASE_TTL_BY_RANDOM_NUM, MappedType.BOOLEAN);
			increaseTTLRandomMean = (Double) properties.getProperty(PROP_KEY_INCREASE_TTL_RANDOM_MEAN, MappedType.DOUBLE);
			increaseTTLRandomStdDev = (Double) properties.getProperty(PROP_KEY_INCREASE_TTL_RANDOM_STD_DEV, MappedType.DOUBLE);
			increaseTTLRandomAbsolute = (Boolean) properties.getProperty(PROP_KEY_INCREASE_TTL_RANDOM_ABSOLUTE, MappedType.BOOLEAN);
			increaseTTLRandomModulo = (Integer) properties.getProperty(PROP_KEY_INCREASE_TTL_RANDOM_MODULO, MappedType.INT);
			
			concealHopCount = (Boolean) properties.getProperty(PROP_KEY_CONCEAL_HOP_COUNT, MappedType.BOOLEAN);
			
			ensureSteinhausPointAnonymity = (Boolean) properties.getProperty(PROP_KEY_ENSURE_STEINHAUS_POINT_ANONYMITY, MappedType.BOOLEAN);
			steinhausPointAnonymityDistanceFactor = (Double) properties.getProperty(PROP_KEY_STEINHAUS_POINT_ANONYMITY_DISTANCE_FACTOR, MappedType.DOUBLE);
			
			
			
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, e.getKey(), "An error occured while reading a node parameter. The property could not be converted: " + e.getKey(), e);
		}
		
		
		
		this.rand = new Random();
		

		
		this.registeredRoutesByOutRouteId = new HashMap<Integer, HyCubeRegisteredRouteInfo>(HashMapUtils.getHashMapCapacityForElementsNum(GlobalConstants.INITIAL_REGISTERED_ROUTES_COLLECTION_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		this.registeredRoutesByOutRouteIdOrdered = new LinkedList<HyCubeRegisteredRouteInfo>();
		
		
	}

	
	
	
	
	
	@Override
	public NodePointer findNextHop(Message msg) {
		return findNextHop((HyCubeMessage)msg);		
		
	}
	
	
	public NodePointer findNextHop(HyCubeMessage msg) {
		
		if (devLog.isTraceEnabled()) {
			devLog.trace("Finding next hop for message #" + msg.getSerialNoAndSenderString());
		}
		
		HyCubeNextHopSelectionParameters parameters = new HyCubeNextHopSelectionParameters();
		parameters.setPMHApplied(msg.isPMHApplied());
		parameters.setSteinhausTransformApplied(msg.isSteinhausTransformApplied());
		parameters.setSkipRandomNumOfNodesApplied(msg.isSkipRandomNumOfNodesApplied());
		parameters.setSecureRoutingApplied(msg.isSecureRoutingApplied());
		parameters.setSteinhausPoint(msg.getSteinhausPointId());
		parameters.setIncludeMoreDistantNodes(false);
		
		NodePointer res = nextHopSelector.findNextHop(msg.getRecipientId(), parameters);
		
		msg.setPMHApplied(parameters.isPMHApplied());
		msg.setSteinhausTransformApplied(parameters.isSteinhausTransformApplied());
		msg.setSkipRandomNumOfNodesApplied(parameters.isSkipRandomNumOfNodesApplied());
		msg.setSecureRoutingApplied(parameters.isSecureRoutingApplied());
		msg.setSteinhausPoint(parameters.getSteinhausPoint());
		
		return res;
		
		
	}
	
	
	public NodePointer[] findNextHops(HyCubeMessage msg, int numNextHops) {
		
		if (devLog.isTraceEnabled()) {
			devLog.trace("Finding next hop for message #" + msg.getSerialNoAndSenderString());
		}
		
		HyCubeNextHopSelectionParameters parameters = new HyCubeNextHopSelectionParameters();
		parameters.setPMHApplied(msg.isPMHApplied());
		parameters.setSteinhausTransformApplied(msg.isSteinhausTransformApplied());
		parameters.setSkipRandomNumOfNodesApplied(msg.isSkipRandomNumOfNodesApplied());
		parameters.setSecureRoutingApplied(msg.isSecureRoutingApplied());
		parameters.setSteinhausPoint(msg.getSteinhausPointId());
		parameters.setIncludeMoreDistantNodes(false);
		
		NodePointer[] res = nextHopSelector.findNextHops(msg.getRecipientId(), parameters, numNextHops);
		
		msg.setPMHApplied(parameters.isPMHApplied());
		msg.setSteinhausTransformApplied(parameters.isSteinhausTransformApplied());
		msg.setSkipRandomNumOfNodesApplied(parameters.isSkipRandomNumOfNodesApplied());
		msg.setSecureRoutingApplied(parameters.isSecureRoutingApplied());
		msg.setSteinhausPoint(parameters.getSteinhausPoint());
		
		return res;
		
		
	}


	
	
	@Override
	public boolean routeMessage(MessageSendProcessInfo info, boolean wait) throws NetworkAdapterException, ProcessMessageException {
		
    	HyCubeMessage msg = (HyCubeMessage) info.getMsg();


    	if (devLog.isDebugEnabled()) {
			devLog.debug("Routing message #" + msg.getSerialNoAndSenderString() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.info("Routing message #" + msg.getSerialNoAndSenderString() + ".");
		}


		
		if (info.getMsg().getTtl() == 0) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("Message #" + info.getMsg().getSerialNoAndSenderString() + " dropped. TTL = 0.");
			}
			if (msgLog.isInfoEnabled()) {
				msgLog.info("Message #" + info.getMsg().getSerialNoAndSenderString() + " dropped. TTL = 0.");
			}
			return false;
		}
		
		
		
		
		//update the hop count
		
		if (info.getMsg().getHopCount() != HOP_COUNT_MASK_VALUE) {
			info.getMsg().setHopCount((short) (info.getMsg().getHopCount() + 1));
		}
		
		
		
		
		
		if (msg.getHopCount() == 1) {
			
			//set the default routing parameters:
			msg.setPMHApplied(false);
			msg.setSteinhausTransformApplied(this.useSteinhausTransform);
			msg.setSteinhausPoint(msg.getSenderId());
			
			
			
			if (info.getRoutingParameters() != null) {
				
				
				//apply the routing parameters (not setting the default values from parameters, if not default values have been specified in the message fields)
				
				Object[] routingParameters = info.getRoutingParameters();
				
				boolean secureRouting = getRoutingParameterSecureRouting(routingParameters);
				if (secureRouting) msg.setSecureRoutingApplied(secureRouting);
				
				boolean skipRandomNextHops = getRoutingParameterSkipRandomNextHops(routingParameters);
				if (skipRandomNextHops) msg.setSkipRandomNumOfNodesApplied(skipRandomNextHops);
				
				boolean registerRoute = getRoutingParameterRegisterRoute(routingParameters);
				if (registerRoute) msg.setRegisterRoute(registerRoute);
				
				boolean routeBack = getRoutingParameterRouteBack(routingParameters);
				if (routeBack) msg.setRouteBack(routeBack);
				
				int routeId = getRoutingParameterRouteId(routingParameters);
				if (routeId != 0) msg.setRouteId(routeId);
				
				boolean anonymousRoute = getRoutingParameterAnonymousRoute(routingParameters);
				if (anonymousRoute) msg.setAnonymousRoute(anonymousRoute);
							
				
				
			}
			
			
			
			//anonymity - update of the Steinhaus point
			
			if (info.getDirectRecipient() == null	//the message is being routed 
					&& msg.isSteinhausTransformApplied() && (ensureSteinhausPointAnonymity || msg.isAnonymousRoute())) {
				
				double dist = HyCubeNodeId.calculateDistance((HyCubeNodeId) this.nodeId, msg.getRecipientId(), Metric.EUCLIDEAN);
				
				int numCloserNSNodes = 0;
				
				HyCubeRoutingTable rt = (HyCubeRoutingTable)nodeAccessor.getRoutingTable();
				
				//acquire the ns lock:
		        rt.getNsLock().readLock().lock();
		        
		        //copy the ns:
		        ArrayList<RoutingTableEntry> nsCopy = new ArrayList<RoutingTableEntry>(rt.getNeighborhoodSet());
		        
		        //release the ns lock
		        rt.getNsLock().readLock().unlock();
				
				for (RoutingTableEntry rte : nsCopy) {
					double distRteDest = HyCubeNodeId.calculateDistance((HyCubeNodeId) rte.getNode().getNodeId(), msg.getRecipientId(), Metric.EUCLIDEAN);
					if (distRteDest <= dist) {
						numCloserNSNodes++;
					}
				}
				
				//if (((HyCubeNodeId)this.nodeId).getBigInteger().not().equals(msg.getRecipientId().getBigInteger())) {
				if (numCloserNSNodes >= nsCopy.size() * steinhausPointAnonymityDistanceFactor) {
					
					NodePointer[] nextHops = findNextHops(msg, 2);
					if (nextHops.length < 2) {
						if (devLog.isDebugEnabled()) {
							devLog.debug("Message #" + msg.getSerialNoAndSenderString() + " dropped. Could not change the Steinhaus point.");
						}
						return false;
					}
					msg.setSteinhausPoint((HyCubeNodeId) nextHops[1].getNodeId());
					
					//and enable the prefix mismatch heuristic:
					msg.setPMHApplied(true);
					
				}
			}
			
			
		
		}
		
		
		
		//update the TTL
		
		if ((concealTTL || msg.isAnonymousRoute()) && decreaseTTLProbability < 1) {
			double randomNum = rand.nextDouble();
			if (decreaseTTLProbability > randomNum) {
				info.getMsg().setTtl((short) (info.getMsg().getTtl() - 1));
			}
		}
		else {
			info.getMsg().setTtl((short) (info.getMsg().getTtl() - 1));
		}
		
		
		
		//conceal the TTL:
		
		if ((concealTTL || msg.isAnonymousRoute()) && increaseTTLByRandomNum) {
			long randomTTLIncrease = 0;
			if (increaseTTLRandomAbsolute) {
				randomTTLIncrease = (long) Math.floor(Math.abs(rand.nextGaussian() * increaseTTLRandomStdDev + increaseTTLRandomMean));
			}
			else {
				randomTTLIncrease = (long) Math.floor(Math.max(rand.nextGaussian() * increaseTTLRandomStdDev + increaseTTLRandomMean, 0));
			}
			if (increaseTTLRandomModulo > 0) randomTTLIncrease = randomTTLIncrease % increaseTTLRandomModulo;
			msg.setTtl((short) (msg.getTtl() + (short)randomTTLIncrease));
			
		}
		
		
		
		
		
		
		if ((! msg.isRouteBack()) && info.getDirectRecipient() == null) {
			NodePointer nextHop;
			if (NodeId.compareIds(this.nodeId, msg.getRecipientId())) {
				nextHop = nodeAccessor.getNodePointer();
			}
			else {
				nextHop = findNextHop(msg);
			}
			
			if (nextHop == null) {
				if (devLog.isDebugEnabled()) {
					devLog.debug("Message #" + msg.getSerialNoAndSenderString() + " dropped. Next hop not found.");
				}
				if (msgLog.isInfoEnabled()) {
					msgLog.info("Message #" + msg.getSerialNoAndSenderString() + " dropped. Next hop not found.");
				}
				return false;
			}
			else {
				if (devLog.isDebugEnabled()) {
					devLog.debug("Next hop found. Sending message #" + msg.getSerialNoAndSenderString() + ". Next hop: " + nextHop.getNodeId().toHexString() + ".");
				}
				if (msgLog.isInfoEnabled()) {
					msgLog.debug("Next hop found. Sending message #" + msg.getSerialNoAndSenderString() + ". Next hop: " + nextHop.getNodeId().toHexString() + ".");
				}
				info.setDirectRecipient(nextHop.getNetworkNodePointer());
				
			}
		}
		
		
		
		
		
		
		
		if (msg.isRouteBack() && info.getDirectRecipient() == null) {
			
			if (devLog.isDebugEnabled()) {
				devLog.debug("Routing back");
			}
			
			int routeId = msg.getRouteId();
			
			HyCubeRegisteredRouteInfo registeredRoute = registeredRoutesByOutRouteId.get(routeId);
			
			if (registeredRoute == null) {
				//the route does not exist or was purged after the retention time
				return false;
			}
			
			//validate if the node sending the message is the one for which the route was registered
			if (! Arrays.equals(registeredRoute.getOutNetworkNodePointer().getAddressBytes(), msg.getSenderNetworkAddress())) {
				return false;
			}
			
			msg.setRouteId(registeredRoute.getInRouteId());
			
			msg.setSenderId(nodeAccessor.getNodeId());
			msg.setSenderNetworkAddress(nodeAccessor.getNetworkAdapter().getPublicAddressBytes());
			
			msg.setSteinhausPoint((HyCubeNodeId) nodeAccessor.getNodeId());
			
			msg.setRecipientId(registeredRoute.getInNodePointer().getNodeId());
			
			info.setDirectRecipient(registeredRoute.getInNodePointer().getNetworkNodePointer());
			
			
		}
		
		
		
		
		
		
		
		//if the route should be registered
		if (msg.isRegisterRoute()) {

			if (! allowRegisteredRoutes) {
				if (devLog.isDebugEnabled()) {
					devLog.debug("Registered routes not allowed. Dropping the message.");
				}
				return false;
			}
			
			
			if (devLog.isDebugEnabled()) {
				devLog.debug("Registering route");
			}

			NodePointer inNodePointer = null;
			NetworkNodePointer outNetworkNodePointer;

			boolean routeStart = (msg.getHopCount() == 1);

			if (! routeStart) {
				inNodePointer = new NodePointer(nodeAccessor.getNetworkAdapter(), msg.getSenderNetworkAddress(), msg.getSenderId());
			}
			outNetworkNodePointer = info.getDirectRecipient();

			long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();

			int inRouteId = msg.getRouteId();
			int outRouteId = getAndReserveNextRandomUnusedRouteId(currTime);

			msg.setRouteId(outRouteId);

			synchronized(registeredRoutesByOutRouteId) {
				HyCubeRegisteredRouteInfo registeredRoute = registeredRoutesByOutRouteId.get(outRouteId);

				if (registeredRoute == null) {
					//the route if is no longer reserved or was not reserved, should only happen if the delay between reservation and the update is longer than the route retention time (should not happen) 
					return false;
				}

				registeredRoute.setValues(inNodePointer, inRouteId, outNetworkNodePointer, outRouteId, routeStart, currTime);

			}

			
			
			//the route is now registered

			msg.setSenderId(nodeAccessor.getNodeId());
			msg.setSenderNetworkAddress(nodeAccessor.getNetworkAdapter().getPublicAddressBytes());

		}
				
		
		
		
		
		//if the route should be registered
		if (msg.isAnonymousRoute()) {

			if (! allowAnonymousRoutes) {
				if (devLog.isDebugEnabled()) {
					devLog.debug("Anonymous routes not allowed. Dropping the message.");
				}
				return false;
			}
			
			
			if (devLog.isDebugEnabled()) {
				devLog.debug("Sending message through an anonymous route");
			}
		
			
			
			
			//hide the original sender:
			
			msg.setSenderId(nodeAccessor.getNodeId());
			msg.setSenderNetworkAddress(nodeAccessor.getNetworkAdapter().getPublicAddressBytes());
			
			
		}
		
		
		
		
		
			
		if (msg.getHopCount() == 1) {
			if (concealHopCount || msg.isAnonymousRoute()) {
				msg.setHopCount((short) HOP_COUNT_MASK_VALUE);
			}
			
		}
		
		
		
		
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Sending message #" + msg.getSerialNoAndSenderString() + " to " + info.getDirectRecipient().getAddressString() + ".");
		}
		if (msgLog.isInfoEnabled()) {
			msgLog.debug("Sending message #" + msg.getSerialNoAndSenderString() + " to " + info.getDirectRecipient().getAddressString() + ".");
		}
		
		nodeAccessor.sendMessageToNode(info, wait);
		
		return true;
		
		
	}


	
	public int getAndReserveNextRandomUnusedRouteId() {
		long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
		return getAndReserveNextRandomUnusedRouteId(currTime);
	}
	
	public int getAndReserveNextRandomUnusedRouteId(long currTime) {
		
		synchronized (registeredRoutesByOutRouteId) {
			
			//purge
			purgeRegisteredRoutes();
			
			int randInt;
			
			do {
				randInt = rand.nextInt(); 
			}
			while (registeredRoutesByOutRouteId.containsKey(randInt));
			
			HyCubeRegisteredRouteInfo registeredRouteEmpty = new HyCubeRegisteredRouteInfo(currTime);
			
			registeredRoutesByOutRouteId.put(randInt, registeredRouteEmpty);
			registeredRoutesByOutRouteIdOrdered.add(registeredRouteEmpty);
			
			return randInt;
		}
		
	}
	
	
	public HyCubeRegisteredRouteInfo getRegisteredRoute(int routeId) {
		
		synchronized (registeredRoutesByOutRouteId) {
			//purge
			purgeRegisteredRoutes();
			
			return registeredRoutesByOutRouteId.get(routeId);
			
		}
		
	}
	

	
	public void purgeRegisteredRoutes() {
		long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
		purgeRegisteredRoutes(currTime);
	}
	
	public void purgeRegisteredRoutes(long purgeTime) {
		
		synchronized(registeredRoutesByOutRouteId) {
			ListIterator<HyCubeRegisteredRouteInfo> iter = registeredRoutesByOutRouteIdOrdered.listIterator();
			while (iter.hasNext()) {
				HyCubeRegisteredRouteInfo registeredRoute = iter.next();
				if (purgeTime >= registeredRoute.getTime() + registeredRoutesRetentionTime) {
					iter.remove();
					registeredRoutesByOutRouteId.remove(registeredRoute.getOutRouteId());
				}
				else break;
				
			}
			
		}
		
	}
	
	
	

	@Override
	public EntryPoint getEntryPoint() {
		return null;
	}
	
	
	
	@Override
	public void discard() {	
	}
	
	
	
	
	public static Object[] createRoutingParameters(Boolean secureRouting, Boolean skipRandomNextHops, Boolean registerRoute, Boolean routeBack, Integer routeId, Boolean anonymousRoute) {
		Object[] routingParameters = new Object[] {secureRouting, skipRandomNextHops, registerRoute, routeBack, routeId, anonymousRoute};
		return routingParameters;
	}

	public static boolean getRoutingParameterSecureRouting(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 0) || (!(parameters[0] instanceof Boolean))) return false;
		return (Boolean)parameters[0];
	}
	
	public static boolean getRoutingParameterSkipRandomNextHops(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 1) || (!(parameters[1] instanceof Boolean))) return false;
		return (Boolean)parameters[1];
	}
	
	public static boolean getRoutingParameterRegisterRoute(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 2) || (!(parameters[2] instanceof Boolean))) return false;
		return (Boolean)parameters[2];
	}
	
	public static boolean getRoutingParameterRouteBack(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 3) || (!(parameters[3] instanceof Boolean))) return false;
		return (Boolean)parameters[3];
	}
	
	public static int getRoutingParameterRouteId(Object[] parameters) {
		if (parameters == null) return 0;
		if (! (parameters.length > 4) || (!(parameters[4] instanceof Integer))) return 0;
		return (Integer)parameters[4];
	}
	
	public static boolean getRoutingParameterAnonymousRoute(Object[] parameters) {
		if (parameters == null) return false;
		if (! (parameters.length > 5) || (!(parameters[5] instanceof Boolean))) return false;
		return (Boolean)parameters[5];
	}
	
	
}

