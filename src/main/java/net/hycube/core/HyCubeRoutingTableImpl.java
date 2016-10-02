package net.hycube.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.hycube.configuration.GlobalConstants;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.environment.NodeProperties;
import net.hycube.utils.HashMapUtils;
import net.hycube.utils.ObjectToStringConverter.MappedType;


public class HyCubeRoutingTableImpl implements RoutingTable, HyCubeRoutingTable {

	protected static final String PROP_KEY_DIMENSIONS = "Dimensions";
	protected static final String PROP_KEY_LEVELS = "Levels";
	protected static final String PROP_KEY_NS_SIZE = "NSSize";
	protected static final String PROP_KEY_ROUTING_TABLE_SLOT_SIZE = "RoutingTableSlotSize";
	protected static final String PROP_KEY_USE_SECURE_ROUTING = "UseSecureRouting";
	
	
	//initial size of the (id_hash -> node) map for routing table 1 
	public static int RT1_MAP_INITIAL_SIZE = 10;
	
	//initial size of the (id_hash -> node) map for routing table 1
	public static int RT2_MAP_INITIAL_SIZE = 10;
	
	public static int ROUTING_TABLES_COUNT = 5;
	
	
	protected boolean initialized = false;
	
	protected NodeProperties nodeProperties;
	
	protected int dimensions;
	protected int digitsCount;
	protected int nsSize;
	protected int routingTableSlotSize;
	protected boolean useSecureRouting;
	
	
	/**
	 * Primary routing table
	 */
	protected List<RoutingTableEntry>[][] routingTable1;


    /**
     * Secondary routing table
     */
	protected List<RoutingTableEntry>[][] routingTable2;


    /**
     * Secure primary routing table
     */
	protected List<RoutingTableEntry>[][] secRoutingTable1;


    /**
     * Secure secondary routing table
     */
	protected List<RoutingTableEntry>[][] secRoutingTable2;

    
    /**
     * Neighborhood set
     */
	protected List<RoutingTableEntry> neighborhoodSet;

    
    //maps that maps node hashes to routing table entries in routing tables:
    protected HashMap<Long, RoutingTableEntry> nsMap;
    protected HashMap<Long, RoutingTableEntry> rt1Map;
    protected HashMap<Long, RoutingTableEntry> rt2Map;
    protected HashMap<Long, RoutingTableEntry> secRt1Map;
    protected HashMap<Long, RoutingTableEntry> secRt2Map;
    protected List<Map<Long, RoutingTableEntry>> rteMaps;
    
    
    
    
    //locks
    protected final ReentrantReadWriteLock nsLock = new ReentrantReadWriteLock(false);
    protected final ReentrantReadWriteLock rt1Lock = new ReentrantReadWriteLock(false);
    protected final ReentrantReadWriteLock rt2Lock = new ReentrantReadWriteLock(false);
    protected final ReentrantReadWriteLock secRt1Lock = new ReentrantReadWriteLock(false);
    protected final ReentrantReadWriteLock secRt2Lock = new ReentrantReadWriteLock(false);
    
    
    
    
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#getRoutingTable1()
	 */
	@Override
	public List<RoutingTableEntry>[][] getRoutingTable1() {
		return routingTable1;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#setRoutingTable1(java.util.List)
	 */
	@Override
	public void setRoutingTable1(List<RoutingTableEntry>[][] routingTable1) {
		this.routingTable1 = routingTable1;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#getRoutingTable2()
	 */
	@Override
	public List<RoutingTableEntry>[][] getRoutingTable2() {
		return routingTable2;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#setRoutingTable2(java.util.List)
	 */
	@Override
	public void setRoutingTable2(List<RoutingTableEntry>[][] routingTable2) {
		this.routingTable2 = routingTable2;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#getSecRoutingTable1()
	 */
	@Override
	public List<RoutingTableEntry>[][] getSecRoutingTable1() {
		return secRoutingTable1;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#setSecRoutingTable(java.util.List)
	 */
	@Override
	public void setSecRoutingTable(List<RoutingTableEntry>[][] secRoutingTable1) {
		this.secRoutingTable1 = secRoutingTable1;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#getSecRoutingTable2()
	 */
	@Override
	public List<RoutingTableEntry>[][] getSecRoutingTable2() {
		return secRoutingTable2;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#setSecRoutingTable2(java.util.List)
	 */
	@Override
	public void setSecRoutingTable2(List<RoutingTableEntry>[][] secRoutingTable2) {
		this.secRoutingTable2 = secRoutingTable2;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#getNeighborhoodSet()
	 */
	@Override
	public List<RoutingTableEntry> getNeighborhoodSet() {
		return neighborhoodSet;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#setLeafSet(java.util.List)
	 */
	@Override
	public void setLeafSet(List<RoutingTableEntry> neighborhoodSet) {
		this.neighborhoodSet = neighborhoodSet;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#getNsMap()
	 */
	@Override
	public HashMap<Long, RoutingTableEntry> getNsMap() {
		return nsMap;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#setNsMap(java.util.HashMap)
	 */
	@Override
	public void setNsMap(HashMap<Long, RoutingTableEntry> nsMap) {
		this.nsMap = nsMap;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#getRt1Map()
	 */
	@Override
	public HashMap<Long, RoutingTableEntry> getRt1Map() {
		return rt1Map;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#setRt1Map(java.util.HashMap)
	 */
	@Override
	public void setRt1Map(HashMap<Long, RoutingTableEntry> rt1Map) {
		this.rt1Map = rt1Map;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#getRt2Map()
	 */
	@Override
	public HashMap<Long, RoutingTableEntry> getRt2Map() {
		return rt2Map;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#setRt2Map(java.util.HashMap)
	 */
	@Override
	public void setRt2Map(HashMap<Long, RoutingTableEntry> rt2Map) {
		this.rt2Map = rt2Map;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#getSecRt1Map()
	 */
	@Override
	public HashMap<Long, RoutingTableEntry> getSecRt1Map() {
		return secRt1Map;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#setSecRt1Map(java.util.HashMap)
	 */
	@Override
	public void setSecRt1Map(HashMap<Long, RoutingTableEntry> secRt1Map) {
		this.secRt1Map = secRt1Map;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#getSecRt2Map()
	 */
	@Override
	public HashMap<Long, RoutingTableEntry> getSecRt2Map() {
		return secRt2Map;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#setSecRt2Map(java.util.HashMap)
	 */
	@Override
	public void setSecRt2Map(HashMap<Long, RoutingTableEntry> secRt2Map) {
		this.secRt2Map = secRt2Map;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#getRteMaps()
	 */
	@Override
	public List<Map<Long, RoutingTableEntry>> getRteMaps() {
		return rteMaps;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#setRteMaps(java.util.List)
	 */
	@Override
	public void setRteMaps(List<Map<Long, RoutingTableEntry>> rteMaps) {
		this.rteMaps = rteMaps;
	}
    
	
	
	
	
	
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#getNsLock()
	 */
	@Override
	public ReentrantReadWriteLock getNsLock() {
		return nsLock;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#getRt1Lock()
	 */
	@Override
	public ReentrantReadWriteLock getRt1Lock() {
		return rt1Lock;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#getRt2Lock()
	 */
	@Override
	public ReentrantReadWriteLock getRt2Lock() {
		return rt2Lock;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#getSecRt1Lock()
	 */
	@Override
	public ReentrantReadWriteLock getSecRt1Lock() {
		return secRt1Lock;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#getSecRt2Lock()
	 */
	@Override
	public ReentrantReadWriteLock getSecRt2Lock() {
		return secRt2Lock;
	}

	
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#getLockByRtType(net.hycube.core.HyCubeRoutingTableType)
	 */
	@Override
	public ReentrantReadWriteLock getLockByRtType(HyCubeRoutingTableType type) {
		switch(type) {
			case NS:
				return nsLock;
			case RT1:
				return rt1Lock;
			case RT2:
				return rt2Lock;
			case SecureRT1:
				return secRt1Lock;
			case SecureRT2:
				return secRt2Lock;
		}
		return null;
	}
	
	
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#getDimensions()
	 */
	@Override
	public int getDimensions() {
		return dimensions;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#getDigitsCount()
	 */
	@Override
	public int getDigitsCount() {
		return digitsCount;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#getNSSize()
	 */
	@Override
	public int getNSSize() {
		return nsSize;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#getRoutingTableSlotSize()
	 */
	@Override
	public int getRoutingTableSlotSize() {
		return routingTableSlotSize;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#isUseSecureRouting()
	 */
	@Override
	public boolean isUseSecureRouting() {
		return useSecureRouting;
	}

	
	
	
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#initialize(net.hycube.environment.NodeProperties)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public void initialize(NodeProperties properties) throws InitializationException {

		this.nodeProperties = properties;
		
		try {
			this.dimensions = (Integer) properties.getProperty(PROP_KEY_DIMENSIONS, MappedType.INT);
			this.digitsCount = (Integer) properties.getProperty(PROP_KEY_LEVELS, MappedType.INT);
			if (this.dimensions <= 0) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize HyCubeRoutingTable instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_DIMENSIONS) + ".");
			}
			
			if (this.digitsCount <= 0) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize HyCubeRoutingTable instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_LEVELS) + ".");
			}
			
			this.nsSize = (Integer) properties.getProperty(PROP_KEY_NS_SIZE, MappedType.INT);
			if (this.nsSize <= 0) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize HyCubeRoutingTable instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_NS_SIZE) + ".");
			}
			
			this.routingTableSlotSize = (Integer) properties.getProperty(PROP_KEY_ROUTING_TABLE_SLOT_SIZE, MappedType.INT);
			if (this.routingTableSlotSize <= 0) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize HyCubeRoutingTable instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_ROUTING_TABLE_SLOT_SIZE) + ".");
			}
			
			this.useSecureRouting = (Boolean) properties.getProperty(PROP_KEY_USE_SECURE_ROUTING, MappedType.BOOLEAN);

					
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize HyCubeRoutingTable instance. Invalid parameter value: " + e.getKey() + ".", e);
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
        rt1Map = new HashMap<Long, RoutingTableEntry>(HashMapUtils.getHashMapCapacityForElementsNum(RT1_MAP_INITIAL_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
        rt2Map = new HashMap<Long, RoutingTableEntry>(HashMapUtils.getHashMapCapacityForElementsNum(RT2_MAP_INITIAL_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
        secRt1Map = new HashMap<Long, RoutingTableEntry>(HashMapUtils.getHashMapCapacityForElementsNum(RT1_MAP_INITIAL_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
        secRt2Map = new HashMap<Long, RoutingTableEntry>(HashMapUtils.getHashMapCapacityForElementsNum(RT2_MAP_INITIAL_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
        
        rteMaps = new ArrayList<Map<Long, RoutingTableEntry>>(5);
    	rteMaps.add(nsMap);
    	rteMaps.add(rt1Map);
    	rteMaps.add(rt2Map);
    	if (useSecureRouting) {
	    	rteMaps.add(secRt1Map);
	    	rteMaps.add(secRt2Map);
    	}

    	
    	this.initialized = true;
		
	}
	
    
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#lockRoutingTableForRead()
	 */
	@Override
	public void lockRoutingTableForRead() {
		nsLock.readLock().lock();
		rt1Lock.readLock().lock();
		rt2Lock.readLock().lock();
		secRt1Lock.readLock().lock();
		secRt2Lock.readLock().lock();
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#unlockRoutingTableForRead()
	 */
	@Override
	public void unlockRoutingTableForRead() {
		secRt2Lock.readLock().unlock();
		secRt1Lock.readLock().unlock();
		rt2Lock.readLock().unlock();
		rt1Lock.readLock().unlock();
		nsLock.readLock().unlock();
		
	}
	
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#lockRoutingTableForWrite()
	 */
	@Override
	public void lockRoutingTableForWrite() {
		nsLock.writeLock().lock();
		rt1Lock.writeLock().lock();
		rt2Lock.writeLock().lock();
		secRt1Lock.writeLock().lock();
		secRt2Lock.writeLock().lock();
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#unlockRoutingTableForWrite()
	 */
	@Override
	public void unlockRoutingTableForWrite() {
		secRt2Lock.writeLock().unlock();
		secRt1Lock.writeLock().unlock();
		rt2Lock.writeLock().unlock();
		rt1Lock.writeLock().unlock();
		nsLock.writeLock().unlock();
		
	}
	
	
	

	@Override
	public List<RoutingTableEntry> getAllRoutingTableEntries() {
		
		int neighborsCount = getNeighborsCount();
		List<RoutingTableEntry> rteList = new ArrayList<RoutingTableEntry>(neighborsCount);
		
		rteList.addAll(nsMap.values());
	    rteList.addAll(rt1Map.values());
	    rteList.addAll(rt2Map.values());
	    if (useSecureRouting) {
	    	rteList.addAll(secRt1Map.values());
	    	rteList.addAll(secRt2Map.values());
	    }
    	
    	return rteList;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.core.HyCubeRoutingTableI#getNeighborsCount()
	 */
	@Override
	public int getNeighborsCount() {
		int neighborsCount = nsMap.size() + rt1Map.size() + rt2Map.size();
	    if (useSecureRouting) neighborsCount = neighborsCount + secRt1Map.size() + secRt2Map.size();
	    return neighborsCount;
	}
	

	@Override
	public List<RoutingTableEntry> getRoutingTableEntriesByNodeIdHash(long nodeIdHash) {
		List<RoutingTableEntry> entries = new ArrayList<RoutingTableEntry>(ROUTING_TABLES_COUNT);
		if (nsMap.containsKey(nodeIdHash)) entries.add(nsMap.get(nodeIdHash));
		if (rt1Map.containsKey(nodeIdHash)) entries.add(rt1Map.get(nodeIdHash));
		if (rt2Map.containsKey(nodeIdHash)) entries.add(rt2Map.get(nodeIdHash));
		if (useSecureRouting) {
			if (secRt1Map.containsKey(nodeIdHash)) entries.add(secRt1Map.get(nodeIdHash));
			if (secRt2Map.containsKey(nodeIdHash)) entries.add(secRt2Map.get(nodeIdHash));
    	}
		return entries;
		
	}

	

	
	
	@Override
	public void discard() {	
	}


	
}
