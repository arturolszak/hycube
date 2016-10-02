package net.hycube.pastry.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.HyCubeRoutingTable;
import net.hycube.core.HyCubeRoutingTableImpl;
import net.hycube.core.InitializationException;
import net.hycube.core.RoutingTableEntry;
import net.hycube.environment.NodeProperties;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.utils.HashMapUtils;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public class PastryHyCubeRoutingTableImpl extends HyCubeRoutingTableImpl implements PastryRoutingTable, HyCubeRoutingTable {

	
	protected static final String PROP_KEY_DIMENSIONS = "Dimensions";
	protected static final String PROP_KEY_LEVELS = "Levels";
	protected static final String PROP_KEY_LS_SIZE = "LSSize";
	protected static final String PROP_KEY_ROUTING_TABLE_SLOT_SIZE = "RoutingTableSlotSize";
	protected static final String PROP_KEY_USE_SECURE_ROUTING = "UseSecureRouting";
	
	
	
	//initial size of the (id_hash -> node) map for routing table 1 
	public static int RT_MAP_INITIAL_SIZE = 10;
	
	public static int ROUTING_TABLES_COUNT = 3;
	
	
	
    
    
	/* (non-Javadoc)
	 * @see net.hycube.pastry.core.PastryRoutingTable#getRoutingTable()
	 */
	@Override
	public List<RoutingTableEntry>[][] getRoutingTable() {
		return routingTable1;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.pastry.core.PastryRoutingTable#setRoutingTable(java.util.List)
	 */
	@Override
	public void setRoutingTable(List<RoutingTableEntry>[][] routingTable) {
		this.routingTable1 = routingTable;
	}
	
	
	/* (non-Javadoc)
	 * @see net.hycube.pastry.core.PastryRoutingTable#getSecRoutingTable()
	 */
	@Override
	public List<RoutingTableEntry>[][] getSecRoutingTable() {
		return secRoutingTable1;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.pastry.core.PastryRoutingTable#setSecRoutingTable(java.util.List)
	 */
	@Override
	public void setSecRoutingTable(List<RoutingTableEntry>[][] secRoutingTable) {
		this.secRoutingTable1 = secRoutingTable;
	}
	
	
	/* (non-Javadoc)
	 * @see net.hycube.pastry.core.PastryRoutingTable#getLeafSet()
	 */
	@Override
	public List<RoutingTableEntry> getLeafSet() {
		return neighborhoodSet;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.pastry.core.PastryRoutingTable#setNeighborhoodSet(java.util.List)
	 */
	@Override
	public void setLeafSet(List<RoutingTableEntry> leafSet) {
		this.neighborhoodSet = leafSet;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.pastry.core.PastryRoutingTable#getLsMap()
	 */
	@Override
	public HashMap<Long, RoutingTableEntry> getLsMap() {
		return nsMap;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.pastry.core.PastryRoutingTable#setLsMap(java.util.HashMap)
	 */
	@Override
	public void setLsMap(HashMap<Long, RoutingTableEntry> lsMap) {
		this.nsMap = lsMap;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.pastry.core.PastryRoutingTable#getRtMap()
	 */
	@Override
	public HashMap<Long, RoutingTableEntry> getRtMap() {
		return rt1Map;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.pastry.core.PastryRoutingTable#setRtMap(java.util.HashMap)
	 */
	@Override
	public void setRtMap(HashMap<Long, RoutingTableEntry> rtMap) {
		this.rt1Map = rtMap;
	}
	
	
	/* (non-Javadoc)
	 * @see net.hycube.pastry.core.PastryRoutingTable#getSecRtMap()
	 */
	@Override
	public HashMap<Long, RoutingTableEntry> getSecRtMap() {
		return secRt1Map;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.pastry.core.PastryRoutingTable#setSecRtMap(java.util.HashMap)
	 */
	@Override
	public void setSecRtMap(HashMap<Long, RoutingTableEntry> secRtMap) {
		this.secRt1Map = secRtMap;
	}
	
	
	/* (non-Javadoc)
	 * @see net.hycube.pastry.core.PastryRoutingTable#getRteMaps()
	 */
	@Override
	public List<Map<Long, RoutingTableEntry>> getRteMaps() {
		return rteMaps;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.pastry.core.PastryRoutingTable#setRteMaps(java.util.List)
	 */
	@Override
	public void setRteMaps(List<Map<Long, RoutingTableEntry>> rteMaps) {
		this.rteMaps = rteMaps;
	}
    

	
	
	/* (non-Javadoc)
	 * @see net.hycube.pastry.core.PastryRoutingTable#getLsLock()
	 */
	@Override
	public ReentrantReadWriteLock getLsLock() {
		return nsLock;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.pastry.core.PastryRoutingTable#getRtLock()
	 */
	@Override
	public ReentrantReadWriteLock getRtLock() {
		return rt1Lock;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.pastry.core.PastryRoutingTable#getSecRtLock()
	 */
	@Override
	public ReentrantReadWriteLock getSecRtLock() {
		return secRt1Lock;
	}
	
	
	
	
	/* (non-Javadoc)
	 * @see net.hycube.pastry.core.PastryRoutingTable#getDimensions()
	 */
	@Override
	public int getDimensions() {
		return dimensions;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.pastry.core.PastryRoutingTable#getDigitsCount()
	 */
	@Override
	public int getDigitsCount() {
		return digitsCount;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.pastry.core.PastryRoutingTable#getLSSize()
	 */
	@Override
	public int getLSSize() {
		return nsSize;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.pastry.core.PastryRoutingTable#getRoutingTableSlotSize()
	 */
	@Override
	public int getRoutingTableSlotSize() {
		return routingTableSlotSize;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.pastry.core.PastryRoutingTable#isUseSecureRouting()
	 */
	@Override
	public boolean isUseSecureRouting() {
		return useSecureRouting;
	}
	

	
	
	
	@SuppressWarnings("unchecked")
	public void initialize(NodeProperties properties) throws InitializationException {

		this.nodeProperties = properties;
		
		try {
			this.dimensions = (Integer) properties.getProperty(PROP_KEY_DIMENSIONS, MappedType.INT);
			this.digitsCount = (Integer) properties.getProperty(PROP_KEY_LEVELS, MappedType.INT);
			if (this.dimensions <= 0) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize PastryRoutingTable instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_DIMENSIONS) + ".");
			}
			
			if (this.digitsCount <= 0) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize PastryRoutingTable instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_LEVELS) + ".");
			}
			
			this.nsSize = (Integer) properties.getProperty(PROP_KEY_LS_SIZE, MappedType.INT);
			if (this.nsSize <= 0) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize PastryRoutingTable instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_LS_SIZE) + ".");
			}
			
			this.routingTableSlotSize = (Integer) properties.getProperty(PROP_KEY_ROUTING_TABLE_SLOT_SIZE, MappedType.INT);
			if (this.routingTableSlotSize <= 0) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize PastryRoutingTable instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_ROUTING_TABLE_SLOT_SIZE) + ".");
			}
			
			this.useSecureRouting = (Boolean) properties.getProperty(PROP_KEY_USE_SECURE_ROUTING, MappedType.BOOLEAN);

					
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize PastryRoutingTable instance. Invalid parameter value: " + e.getKey() + ".", e);
		}		
		
		//initialize the routing tables:
        routingTable1 = (ArrayList<RoutingTableEntry>[][]) new ArrayList<?>[digitsCount][];
        for (int i = 0; i < digitsCount; i++) {
            routingTable1[i] = (ArrayList<RoutingTableEntry>[]) new ArrayList<?>[Integer.rotateLeft(2, dimensions)];
            for (int ii = 0; ii < Integer.rotateLeft(2, dimensions); ii++) {
                routingTable1[i][ii] = new ArrayList<RoutingTableEntry>(routingTableSlotSize);
            }
        }

        routingTable2 = (ArrayList<RoutingTableEntry>[][]) new ArrayList<?>[digitsCount][];
        for (int i = 0; i < digitsCount; i++)
        {
            routingTable2[i] = (ArrayList<RoutingTableEntry>[]) new ArrayList<?>[dimensions];
            for (int ii = 0; ii < dimensions; ii++)
            {
                routingTable2[i][ii] = new ArrayList<RoutingTableEntry>(routingTableSlotSize);
            }
        }

        if (useSecureRouting) {
	        secRoutingTable1 = (ArrayList<RoutingTableEntry>[][]) new ArrayList<?>[digitsCount][];
	        for (int i = 0; i < digitsCount; i++) {
	            secRoutingTable1[i] = (ArrayList<RoutingTableEntry>[]) new ArrayList<?>[Integer.rotateLeft(2, dimensions)];
	            for (int ii = 0; ii < Integer.rotateLeft(2, dimensions); ii++) {
	                secRoutingTable1[i][ii] = new ArrayList<RoutingTableEntry>(routingTableSlotSize);
	            }
	        }
	        
	        secRoutingTable2 = (ArrayList<RoutingTableEntry>[][]) new ArrayList<?>[digitsCount][];
	        for (int i = 0; i < digitsCount; i++)
	        {
	            secRoutingTable2[i] = (ArrayList<RoutingTableEntry>[]) new ArrayList<?>[dimensions];
	            for (int ii = 0; ii < dimensions; ii++)
	            {
	                secRoutingTable2[i][ii] = new ArrayList<RoutingTableEntry>(routingTableSlotSize);
	            }
	        }
	
        }
        
        neighborhoodSet = new ArrayList<RoutingTableEntry>(nsSize);
        
		
        nsMap = new HashMap<Long, RoutingTableEntry>(HashMapUtils.getHashMapCapacityForElementsNum(nsSize, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
        rt1Map = new HashMap<Long, RoutingTableEntry>(HashMapUtils.getHashMapCapacityForElementsNum(RT_MAP_INITIAL_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
        secRt1Map = new HashMap<Long, RoutingTableEntry>(HashMapUtils.getHashMapCapacityForElementsNum(RT_MAP_INITIAL_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
        
        rteMaps = new ArrayList<Map<Long, RoutingTableEntry>>(3);
    	rteMaps.add(nsMap);
    	rteMaps.add(rt1Map);
    	if (useSecureRouting) {
	    	rteMaps.add(secRt1Map);
    	}
    	
    	rt2Map = new HashMap<Long, RoutingTableEntry>(HashMapUtils.getHashMapCapacityForElementsNum(0, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
        secRt2Map = new HashMap<Long, RoutingTableEntry>(HashMapUtils.getHashMapCapacityForElementsNum(0, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);

        
		
	}
	
    

	@Override
	public List<RoutingTableEntry> getAllRoutingTableEntries() {
		
		int neighborsCount = getNeighborsCount();
		List<RoutingTableEntry> rteList = new ArrayList<RoutingTableEntry>(neighborsCount);
		
		synchronized (this) {
			rteList.addAll(nsMap.values());
	    	rteList.addAll(rt1Map.values());
	    	if (useSecureRouting) {
	    		rteList.addAll(secRt1Map.values());
	    	}
		}
    	
    	return rteList;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.pastry.core.PastryRoutingTable#getNeighborsCount()
	 */
	@Override
	public int getNeighborsCount() {
		synchronized (this) {
			int neighborsCount = nsMap.size() + rt1Map.size();
	    	if (useSecureRouting) neighborsCount = neighborsCount + secRt1Map.size();
	    	return neighborsCount;
		}
	}

	@Override
	public List<RoutingTableEntry> getRoutingTableEntriesByNodeIdHash(long nodeIdHash) {
		List<RoutingTableEntry> entries = new ArrayList<RoutingTableEntry>(ROUTING_TABLES_COUNT);
		if (nsMap.containsKey(nodeIdHash)) entries.add(nsMap.get(nodeIdHash));
		if (rt1Map.containsKey(nodeIdHash)) entries.add(rt1Map.get(nodeIdHash));
				if (useSecureRouting) {
			if (secRt1Map.containsKey(nodeIdHash)) entries.add(secRt1Map.get(nodeIdHash));
    	}
		return entries;
	}


	
	
	@Override
	public void discard() {	
	}


	

}
