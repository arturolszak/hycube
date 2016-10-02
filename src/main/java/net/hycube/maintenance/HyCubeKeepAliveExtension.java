package net.hycube.maintenance;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.ListIterator;

import net.hycube.common.EntryPoint;
import net.hycube.configuration.GlobalConstants;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodePointer;
import net.hycube.environment.NodeProperties;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.extensions.Extension;
import net.hycube.utils.HashMapUtils;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public class HyCubeKeepAliveExtension implements Extension {

	public static final String PROP_KEY_PING_INTERVAL = "PingInterval";
	public static final String PROP_KEY_PONG_TIMEOUT = "PongTimeout";
	public static final String PROP_KEY_PROCESS_PONG_INTERVAL = "ProcessPongInterval";
	public static final String PROP_KEY_PING_RESPONSE_INDICATOR_RTE_KEY = "PingResponseIndicatorRteKey";
	public static final String PROP_KEY_INITIAL_PING_RESPONSE_INDICATOR_VALUE = "InitialPingResponseIndicatorValue";
	public static final String PROP_KEY_MAX_PING_RESPONSE_INDICATOR_VALUE = "MaxPingResponseIndicatorValue";
	public static final String PROP_KEY_PING_RESPONSE_INDICATOR_UPDATE_COEFFICIENT = "PingResponseIndicatorUpdateCoefficient";
	public static final String PROP_KEY_PING_RESPONSE_INDICATOR_DEACTIVATE_THRESHOLD = "PingResponseIndicatorDeactivateThreshold";
	public static final String PROP_KEY_PING_RESPONSE_INDICATOR_REMOVE_THRESHOLD = "PingResponseIndicatorRemoveThreshold";
	public static final String PROP_KEY_PING_RESPONSE_INDICATOR_RETENTION_TIME = "PingResponseIndicatorRetentionTime";

	
	//maximum ping interval (ms):
	public static final int MAX_PING_INTERVAL = 3600000;
	
	//maximum pong timeout (ms):
	public static final int MAX_PONG_TIMEOUT = 60000;
	
	//maximum process pong interval (ms):
	public static final int MAX_PROCESS_PONG_INTERVAL = 600000;
	
	
	
	
	protected NodeAccessor nodeAccessor;
	
	protected final Object pongAwaitingLock = new Object();				//any structural read/update to the pongAwaitingMap should be synchronized on this lock, pongProc objects access should be synchronized on themselves
    protected HashMap<Integer, HyCubePongProcessInfo> pongAwaitingMap;
    
    
    //hash map of cached ping response indicators
    protected HashMap<Long, HyCubePingResponseIndicatorInfo> priCache;
    
    //ordered list (by discard time) of cached ping response indicators
    protected LinkedList<HyCubePingResponseIndicatorInfo> priCacheOrdered;
    
    
    
	protected int pingInterval;
	protected int pongTimeout;
	protected int processPongInterval;
	protected String pingResponseIndicatorRteKey;
	protected double initialPingResponseIndicatorValue;
	protected double maxPingResponseIndicatorValue;
	protected double pingResponseIndicatorUpdateCoefficient;
	protected double pingResponseIndicatorDeactivateThreshold;
	protected double pingResponseIndicatorRemoveThreshold;
	protected int pingResponseIndicatorRetentionTime;
	

	
    
    
	public int getPingInterval() {
		return pingInterval;
	}
	
	public void setPingInterval(int pingInterval) {
		this.pingInterval = pingInterval;
	}

	public int getPongTimeout() {
		return pongTimeout;
	}

	public void setPongTimeout(int pongTimeout) {
		this.pongTimeout = pongTimeout;
	}

	public int getProcessPongInterval() {
		return processPongInterval;
	}

	public void setProcessPongInterval(int processPongInterval) {
		this.processPongInterval = processPongInterval;
	}
	
	public String getPingResponseIndicatorRteKey() {
		return pingResponseIndicatorRteKey;
	}
	
	public void setPingResponseIndicatorRteKey(String pingResponseIndicatorRteKey) {
		this.pingResponseIndicatorRteKey = pingResponseIndicatorRteKey;
	}
	
	public double getInitialPingResponseIndicatorValue() {
		return initialPingResponseIndicatorValue;
	}
	
	public void setInitialPingResponseIndicatorValue(double initialPingResponseIndicatorValue) {
		this.initialPingResponseIndicatorValue = initialPingResponseIndicatorValue;
	}
	
	public double getMaxPingResponseIndicatorValue() {
		return maxPingResponseIndicatorValue;
	}
	
	public void setMaxPingResponseIndicatorValue(double maxPingResponseIndicatorValue) {
		this.maxPingResponseIndicatorValue = maxPingResponseIndicatorValue;
	}
	
	public double getPingResponseIndicatorUpdateCoefficient() {
		return pingResponseIndicatorUpdateCoefficient;
	}
	
	public void setPingResponseIndicatorUpdateCoefficient(double pingResponseIndicatorUpdateCoefficient) {
		this.pingResponseIndicatorUpdateCoefficient = pingResponseIndicatorUpdateCoefficient;
	}
	
	public double getPingResponseIndicatorDeactivateThreshold() {
		return pingResponseIndicatorDeactivateThreshold;
	}
	
	public void setPingIndicatorDeactivateThreshold(double pingResponseIndicatorDeactivateThreshold) {
		this.pingResponseIndicatorDeactivateThreshold = pingResponseIndicatorDeactivateThreshold;
	}
	
	public double getPingResponseIndicatorRemoveThreshold() {
		return pingResponseIndicatorRemoveThreshold;
	}
	
	public void setPingResponseIndicatorRemoveThreshold(double pingResponseIndicatorRemoveThreshold) {
		this.pingResponseIndicatorRemoveThreshold = pingResponseIndicatorRemoveThreshold;
	}
	
	public int getPingResponseIndicatorRetentionTime() {
		return pingResponseIndicatorRetentionTime;
	}
	
	public void setPingResponseIndicatorRetentionTime(int pingResponseIndicatorRetentionTime) {
		this.pingResponseIndicatorRetentionTime = pingResponseIndicatorRetentionTime;
	}
	
	
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		this.nodeAccessor = nodeAccessor;
		
		this.pongAwaitingMap = new HashMap<Integer, HyCubePongProcessInfo>();
		
		try {
			this.pingInterval = (Integer) properties.getProperty(PROP_KEY_PING_INTERVAL, MappedType.INT);
			if (this.pingInterval <= 0 || this.pingInterval > MAX_PING_INTERVAL) {
				throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, PROP_KEY_PING_INTERVAL, "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_PING_INTERVAL));
			}
			
			this.pongTimeout = (Integer) properties.getProperty(PROP_KEY_PONG_TIMEOUT, MappedType.INT);
			if (this.pongTimeout <= 0 || this.pongTimeout > MAX_PONG_TIMEOUT) {
				throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, PROP_KEY_PONG_TIMEOUT, "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_PONG_TIMEOUT));
			}
			
			this.processPongInterval = (Integer) properties.getProperty(PROP_KEY_PROCESS_PONG_INTERVAL, MappedType.INT);
			if (this.processPongInterval <= 0 || this.processPongInterval > MAX_PROCESS_PONG_INTERVAL) {
				throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, PROP_KEY_PROCESS_PONG_INTERVAL, "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_PROCESS_PONG_INTERVAL));
			}
			
			this.pingResponseIndicatorRteKey = properties.getProperty(PROP_KEY_PING_RESPONSE_INDICATOR_RTE_KEY);
			if (pingResponseIndicatorRteKey == null || pingResponseIndicatorRteKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_PING_RESPONSE_INDICATOR_RTE_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_PING_RESPONSE_INDICATOR_RTE_KEY));
			
			this.initialPingResponseIndicatorValue = (Double) properties.getProperty(PROP_KEY_INITIAL_PING_RESPONSE_INDICATOR_VALUE, MappedType.DOUBLE);
			this.maxPingResponseIndicatorValue = (Double) properties.getProperty(PROP_KEY_MAX_PING_RESPONSE_INDICATOR_VALUE, MappedType.DOUBLE);
			this.pingResponseIndicatorUpdateCoefficient = (Double) properties.getProperty(PROP_KEY_PING_RESPONSE_INDICATOR_UPDATE_COEFFICIENT, MappedType.DOUBLE);
			this.pingResponseIndicatorDeactivateThreshold = (Double) properties.getProperty(PROP_KEY_PING_RESPONSE_INDICATOR_DEACTIVATE_THRESHOLD, MappedType.DOUBLE);
			this.pingResponseIndicatorRemoveThreshold = (Double) properties.getProperty(PROP_KEY_PING_RESPONSE_INDICATOR_REMOVE_THRESHOLD, MappedType.DOUBLE);
			this.pingResponseIndicatorRetentionTime = (Integer) properties.getProperty(PROP_KEY_PING_RESPONSE_INDICATOR_RETENTION_TIME, MappedType.INT);
		
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, e.getKey(), "An error occured while reading a parameter. The property could not be converted: " + e.getKey(), e);
		}
		
		
		this.priCache = new HashMap<Long, HyCubePingResponseIndicatorInfo>(HashMapUtils.getHashMapCapacityForElementsNum(GlobalConstants.DEFAULT_INITIAL_COLLECTION_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
	    this.priCacheOrdered = new LinkedList<HyCubePingResponseIndicatorInfo>();
		
	}
	
	
	
	@Override
	public void postInitialize() {
		
	}
	
	
	@Override
	public EntryPoint getExtensionEntryPoint() {
		return null;
	}
	

	public Object getPongAwaitingLock() {
		return pongAwaitingLock;
	}

	public HashMap<Integer, HyCubePongProcessInfo> getPongAwaitingMap() {
		return pongAwaitingMap;
	}

	@Override
	public void discard() {

	}


	

	public void cachePingResponseIndicator(NodePointer np, double pingResponseIndicator) {
		
		clearExpiredCachedPingResponseIndicators();
		
		if (pingResponseIndicatorRetentionTime > 0) {
			
			synchronized (priCache) {

				long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
				long discardTime = currTime + pingResponseIndicatorRetentionTime;
				HyCubePingResponseIndicatorInfo priInfo = new HyCubePingResponseIndicatorInfo(np.getNodeId(), np.getNodeIdHash(), np.getNetworkNodePointer(), pingResponseIndicator, discardTime);

				HyCubePingResponseIndicatorInfo prev = priCache.put(np.getNodeIdHash(), priInfo);
				if (prev != null) {
					prev.setRemoved(true);
					priCacheOrdered.addLast(priInfo);
				}
				
			}

		}
		
	}
	
	
	
	public double retrieveCachedPingResponseIndicator(NodePointer np) {
		return retrieveCachedPingResponseIndicator(np.getNodeIdHash());
	}
	
	public double retrieveCachedPingResponseIndicator(long nodeIdHash) {
		
		clearExpiredCachedPingResponseIndicators();
		
		long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
		
		synchronized (priCache) {
			HyCubePingResponseIndicatorInfo pri = priCache.get(nodeIdHash);
			if (pri != null && (!pri.isRemoved()) && (currTime >= pri.getDiscardTime())) {
				return pri.getPingResponseIndicator();
			}
			else return Double.NaN;
		}
		
	}
	
	public void removeCachedPingResponseIndicator(NodePointer np) {
		removeCachedPingResponseIndicator(np.getNodeIdHash());
	}
	
	public void removeCachedPingResponseIndicator(long nodeIdHash) {
		
		clearExpiredCachedPingResponseIndicators();
		
		synchronized (priCache) {
			HyCubePingResponseIndicatorInfo pri = priCache.remove(nodeIdHash);
			if (pri != null) {
				pri.setRemoved(true);
			}
		}
		
	}
	
	public void clearExpiredCachedPingResponseIndicators() {
		
		long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
		
		synchronized (priCache) {
			ListIterator<HyCubePingResponseIndicatorInfo> iter = priCacheOrdered.listIterator();
			while (iter.hasNext()) {
				HyCubePingResponseIndicatorInfo pri = iter.next();
				if (pri.isRemoved() || ((!pri.isRemoved()) && pri.getDiscardTime() <= currTime)) {
					//discard
					pri.setRemoved(true);
					priCache.remove(pri.getNodeIdHash());
					iter.remove();
				}
				else {
					break;
				}
			}
		}
		
	}
	
	

}

