package net.hycube.rtnodeselection;

import java.util.HashMap;
import java.util.List;

import net.hycube.core.HyCubeRoutingTableSlotInfo;
import net.hycube.core.HyCubeRoutingTableType;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodeId;
import net.hycube.core.NodePointer;
import net.hycube.core.RoutingTableEntry;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.environment.NodeProperties;
import net.hycube.maintenance.HyCubeKeepAliveExtension;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public class HyCubeLnsRTNodeSelector extends HyCubeRTNodeSelector {

	protected static final String PROP_KEY_LNS_INDICATOR_RTE_KEY = "LnsIndicatorRteKey";
	protected static final String PROP_KEY_INITIAL_LNS_INDICATOR_VALUE = "InitialLnsIndicatorValue";
	protected static final String PROP_KEY_LNS_INDICATOR_REPLACE_THRESHOLD = "LnsIndicatorReplaceThreshold";
	protected static final String PROP_KEY_KEEP_ALIVE_EXTENSION_KEY = "KeepAliveExtensionKey";
	protected static final String PROP_KEY_USE_KEEP_ALIVE_EXTENSION_LNS_INDICATOR_CACHE = "UseKeepAliveExtensionLnsIndicatorCache";
	
	
	protected String lnsIndicatorRteKey;
	protected double initialLnsIndicatorValue;
	protected double lnsIndicatorReplaceThreshold;
	protected String keepAliveExtensionKey;
	protected boolean useKeepAliveExtensionLnsIndicatorCache;
	
	protected HyCubeKeepAliveExtension keepAliveExtension; 
	
	
	
	@Override
	public void initialize(NodeId nodeId, NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		super.initialize(nodeId, nodeAccessor, properties);
		
		//parameters
		try {
			
			lnsIndicatorRteKey = properties.getProperty(PROP_KEY_LNS_INDICATOR_RTE_KEY);
			if (lnsIndicatorRteKey == null || lnsIndicatorRteKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_LNS_INDICATOR_RTE_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_LNS_INDICATOR_RTE_KEY));
			
			initialLnsIndicatorValue = (Double) properties.getProperty(PROP_KEY_INITIAL_LNS_INDICATOR_VALUE, MappedType.DOUBLE);
			
			lnsIndicatorReplaceThreshold = (Double) properties.getProperty(PROP_KEY_LNS_INDICATOR_REPLACE_THRESHOLD, MappedType.DOUBLE);
			
			keepAliveExtensionKey = properties.getProperty(PROP_KEY_KEEP_ALIVE_EXTENSION_KEY);
			
			try {
				this.keepAliveExtension = (HyCubeKeepAliveExtension) nodeAccessor.getExtension(this.keepAliveExtensionKey);
				if (this.keepAliveExtension == null) throw new InitializationException(InitializationException.Error.MISSING_EXTENSION_ERROR, this.keepAliveExtensionKey, "The KeepAliveExtension is missing at the specified key: " + this.keepAliveExtensionKey + ".");
			} catch (ClassCastException e) {
				throw new InitializationException(InitializationException.Error.MISSING_EXTENSION_ERROR, this.keepAliveExtensionKey, "The KeepAliveExtension is missing at the specified key: " + this.keepAliveExtensionKey + ".");
			}
			
			
			useKeepAliveExtensionLnsIndicatorCache = (Boolean) properties.getProperty(PROP_KEY_USE_KEEP_ALIVE_EXTENSION_LNS_INDICATOR_CACHE, MappedType.BOOLEAN);
			
			
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize rt node selector instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
	}
	
	
	@Override
	public void processNode(NodePointer newNode, 
			List<RoutingTableEntry> routingTableSlot, HashMap<Long, RoutingTableEntry> rtMap, 
			HyCubeRoutingTableType rtType, int level, int slotNo, 
			int routingTableSlotSize,
			double dist, long currTimestamp) {
		
		double newRteLnsIndicatorValue;
		if (useKeepAliveExtensionLnsIndicatorCache) {
			double cachedValue = keepAliveExtension.retrieveCachedPingResponseIndicator(newNode.getNodeIdHash());
			if (! Double.isNaN(cachedValue)) newRteLnsIndicatorValue = cachedValue;
			else newRteLnsIndicatorValue = initialLnsIndicatorValue;
		}
		else {
			newRteLnsIndicatorValue = initialLnsIndicatorValue;
		}
		
		if (routingTableSlot.size() < routingTableSlotSize) {
			HyCubeRoutingTableSlotInfo slotInfo = new HyCubeRoutingTableSlotInfo(rtType, rtMap, routingTableSlot);
			RoutingTableEntry rte = initializeRoutingTableEntry(newNode, dist, currTimestamp, slotInfo);
			
        	rte.setData(lnsIndicatorRteKey, newRteLnsIndicatorValue);
        	rtMap.put(newNode.getNodeIdHash(), rte);
        	routingTableSlot.add(rte);
        	return;
		}
		else {
			int worstIndex = -1;
			double worstPingIndicator = 0;
			for (int i = 0; i < routingTableSlot.size(); i++) {
				//find the worst node, according to the ping indicator value:
				double rtePingIndicator = ((Double)(routingTableSlot.get(i).getData(lnsIndicatorRteKey)));
				if (worstIndex == -1 || rtePingIndicator < worstPingIndicator) {
					worstIndex = i;
					worstPingIndicator = rtePingIndicator;
				}
			}
			if (worstPingIndicator <= lnsIndicatorReplaceThreshold && worstPingIndicator < newRteLnsIndicatorValue) {
				//replace the worst node with the new node:
	        	HyCubeRoutingTableSlotInfo slotInfo = new HyCubeRoutingTableSlotInfo(rtType, rtMap, routingTableSlot);
	        	RoutingTableEntry rte = initializeRoutingTableEntry(newNode, dist, currTimestamp, slotInfo);
	        	rte.setData(lnsIndicatorRteKey, newRteLnsIndicatorValue);
	        	rtMap.remove(routingTableSlot.get(worstIndex).getNodeIdHash());
	        	routingTableSlot.set(worstIndex, rte);
	        	rtMap.put(newNode.getNodeIdHash(), rte);
	            
			}
			
		}
		
	}

}
