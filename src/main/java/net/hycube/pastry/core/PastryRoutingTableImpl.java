package net.hycube.pastry.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.InitializationException;
import net.hycube.core.RoutingTableEntry;
import net.hycube.environment.NodeProperties;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.utils.HashMapUtils;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public class PastryRoutingTableImpl implements PastryRoutingTable {

	
	protected static final String PROP_KEY_DIMENSIONS = "Dimensions";
	protected static final String PROP_KEY_LEVELS = "Levels";
	protected static final String PROP_KEY_LS_SIZE = "LSSize";
	protected static final String PROP_KEY_ROUTING_TABLE_SLOT_SIZE = "RoutingTableSlotSize";
	protected static final String PROP_KEY_USE_SECURE_ROUTING = "UseSecureRouting";
	
	
	
	//initial size of the (id_hash -> node) map for routing table 1 
	public static int RT_MAP_INITIAL_SIZE = 10;
	
	public static int ROUTING_TABLES_COUNT = 3;
	
	
	
	protected boolean initialized = false;
	
	protected NodeProperties properties;
	
	protected int dimensions;
	protected int digitsCount;
	protected int lsSize;
	protected int routingTableSlotSize;
	protected boolean useSecureRouting;
	
	protected final ReentrantReadWriteLock lsLock = new ReentrantReadWriteLock(false);
    protected final ReentrantReadWriteLock rtLock = new ReentrantReadWriteLock(false);
    protected final ReentrantReadWriteLock secRtLock = new ReentrantReadWriteLock(false);
    
	
	/**
	 * Primary routing table
	 */
	protected List<RoutingTableEntry>[][] routingTable;


    /**
     * Secure primary routing table
     */
	protected List<RoutingTableEntry>[][] secRoutingTable;


    
    /**
     * Leaf set
     */
	protected List<RoutingTableEntry> leafSet;

    
    //maps that maps node hashes to routing table entries in routing tables:
    protected HashMap<Long, RoutingTableEntry> lsMap;
    protected HashMap<Long, RoutingTableEntry> rtMap;
    protected HashMap<Long, RoutingTableEntry> secRtMap;
    protected List<Map<Long, RoutingTableEntry>> rteMaps;
    
    
	/**
	 * @return the routingTable
	 */
	public List<RoutingTableEntry>[][] getRoutingTable() {
		return routingTable;
	}
	
	/**
	 * @param routingTable the routingTable to set
	 */
	public void setRoutingTable(List<RoutingTableEntry>[][] routingTable) {
		this.routingTable = routingTable;
	}
	
	
	/**
	 * @return the secRoutingTable
	 */
	public List<RoutingTableEntry>[][] getSecRoutingTable() {
		return secRoutingTable;
	}
	
	/**
	 * @param secRoutingTable the secRoutingTable to set
	 */
	public void setSecRoutingTable(List<RoutingTableEntry>[][] secRoutingTable) {
		this.secRoutingTable = secRoutingTable;
	}
	
	
	/**
	 * @return the leafSet
	 */
	public List<RoutingTableEntry> getLeafSet() {
		return leafSet;
	}
	
	/**
	 * @param leafSet the leafSet to set
	 */
	public void setLeafSet(List<RoutingTableEntry> leafSet) {
		this.leafSet = leafSet;
	}
	
	/**
	 * @return the lsMap
	 */
	public HashMap<Long, RoutingTableEntry> getLsMap() {
		return lsMap;
	}
	
	/**
	 * @param lsMap the lsMap to set
	 */
	public void setLsMap(HashMap<Long, RoutingTableEntry> lsMap) {
		this.lsMap = lsMap;
	}
	
	/**
	 * @return the rtMap
	 */
	public HashMap<Long, RoutingTableEntry> getRtMap() {
		return rtMap;
	}
	
	/**
	 * @param rtMap the rtMap to set
	 */
	public void setRtMap(HashMap<Long, RoutingTableEntry> rtMap) {
		this.rtMap = rtMap;
	}
	
	
	/**
	 * @return the secRtMap
	 */
	public HashMap<Long, RoutingTableEntry> getSecRtMap() {
		return secRtMap;
	}
	
	/**
	 * @param secRtMap the secRtMap to set
	 */
	public void setSecRtMap(HashMap<Long, RoutingTableEntry> secRtMap) {
		this.secRtMap = secRtMap;
	}
	
	
	/**
	 * @return the rteMaps
	 */
	public List<Map<Long, RoutingTableEntry>> getRteMaps() {
		return rteMaps;
	}
	
	/**
	 * @param rteMaps the rteMaps to set
	 */
	public void setRteMaps(List<Map<Long, RoutingTableEntry>> rteMaps) {
		this.rteMaps = rteMaps;
	}
    

	
	
	public ReentrantReadWriteLock getLsLock() {
		return lsLock;
	}
	
	public ReentrantReadWriteLock getRtLock() {
		return rtLock;
	}
	
	public ReentrantReadWriteLock getSecRtLock() {
		return secRtLock;
	}
	
	
	
	public ReentrantReadWriteLock getLockByRtType(PastryRoutingTableType type) {
		switch(type) {
			case LS:
				return lsLock;
			case RT:
				return rtLock;
			case SecureRT:
				return secRtLock;
		}
		return null;
	}

	
	
	
	
	public int getDimensions() {
		return dimensions;
	}
	
	public int getDigitsCount() {
		return digitsCount;
	}
	
	public int getLSSize() {
		return lsSize;
	}
	
	public int getRoutingTableSlotSize() {
		return routingTableSlotSize;
	}
	
	public boolean isUseSecureRouting() {
		return useSecureRouting;
	}
	

	
	
	
	@SuppressWarnings("unchecked")
	public void initialize(NodeProperties properties) throws InitializationException {

		this.properties = properties;
		
		try {
			this.dimensions = (Integer) properties.getProperty(PROP_KEY_DIMENSIONS, MappedType.INT);
			this.digitsCount = (Integer) properties.getProperty(PROP_KEY_LEVELS, MappedType.INT);
			if (this.dimensions <= 0) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize PastryRoutingTable instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_DIMENSIONS) + ".");
			}
			
			if (this.digitsCount <= 0) {
				throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize PastryRoutingTable instance. Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_LEVELS) + ".");
			}
			
			this.lsSize = (Integer) properties.getProperty(PROP_KEY_LS_SIZE, MappedType.INT);
			if (this.lsSize <= 0) {
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
        routingTable = (ArrayList<RoutingTableEntry>[][]) new ArrayList<?>[digitsCount][];
        for (int i = 0; i < digitsCount; i++) {
            routingTable[i] = (ArrayList<RoutingTableEntry>[]) new ArrayList<?>[Integer.rotateLeft(2, dimensions)];
            for (int ii = 0; ii < Integer.rotateLeft(2, dimensions); ii++) {
                routingTable[i][ii] = new ArrayList<RoutingTableEntry>(routingTableSlotSize);
            }
        }


        if (useSecureRouting) {
	        secRoutingTable = (ArrayList<RoutingTableEntry>[][]) new ArrayList<?>[digitsCount][];
	        for (int i = 0; i < digitsCount; i++) {
	            secRoutingTable[i] = (ArrayList<RoutingTableEntry>[]) new ArrayList<?>[Integer.rotateLeft(2, dimensions)];
	            for (int ii = 0; ii < Integer.rotateLeft(2, dimensions); ii++) {
	                secRoutingTable[i][ii] = new ArrayList<RoutingTableEntry>(routingTableSlotSize);
	            }
	        }
	
        }
        
        leafSet = new ArrayList<RoutingTableEntry>(lsSize);
        
		
        lsMap = new HashMap<Long, RoutingTableEntry>(HashMapUtils.getHashMapCapacityForElementsNum(lsSize, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
        rtMap = new HashMap<Long, RoutingTableEntry>(HashMapUtils.getHashMapCapacityForElementsNum(RT_MAP_INITIAL_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
        secRtMap = new HashMap<Long, RoutingTableEntry>(HashMapUtils.getHashMapCapacityForElementsNum(RT_MAP_INITIAL_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
        
        rteMaps = new ArrayList<Map<Long, RoutingTableEntry>>(5);
    	rteMaps.add(lsMap);
    	rteMaps.add(rtMap);
    	if (useSecureRouting) {
	    	rteMaps.add(secRtMap);
    	}

		
	}
	
    
	
	
	
	@Override
	public void lockRoutingTableForRead() {
		lsLock.readLock().lock();
		rtLock.readLock().lock();
		secRtLock.readLock().lock();
	}
	
	
	@Override
	public void unlockRoutingTableForRead() {
		secRtLock.readLock().unlock();
		rtLock.readLock().unlock();
		lsLock.readLock().unlock();
		
	}
	
	
	@Override
	public void lockRoutingTableForWrite() {
		lsLock.writeLock().lock();
		rtLock.writeLock().lock();
		secRtLock.writeLock().lock();
	}
	
	
	@Override
	public void unlockRoutingTableForWrite() {
		secRtLock.writeLock().unlock();
		rtLock.writeLock().unlock();
		lsLock.writeLock().unlock();
		
	}

	
	
	
	
	
	

	@Override
	public List<RoutingTableEntry> getAllRoutingTableEntries() {
		
		int neighborsCount = getNeighborsCount();
		List<RoutingTableEntry> rteList = new ArrayList<RoutingTableEntry>(neighborsCount);
		
		synchronized (this) {
			rteList.addAll(lsMap.values());
	    	rteList.addAll(rtMap.values());
	    	if (useSecureRouting) {
	    		rteList.addAll(secRtMap.values());
	    	}
		}
    	
    	return rteList;
	}
	
	public int getNeighborsCount() {
		synchronized (this) {
			int neighborsCount = lsMap.size() + rtMap.size();
	    	if (useSecureRouting) neighborsCount = neighborsCount + secRtMap.size();
	    	return neighborsCount;
		}
	}

	@Override
	public List<RoutingTableEntry> getRoutingTableEntriesByNodeIdHash(long nodeIdHash) {
		List<RoutingTableEntry> entries = new ArrayList<RoutingTableEntry>(ROUTING_TABLES_COUNT);
		if (lsMap.containsKey(nodeIdHash)) entries.add(lsMap.get(nodeIdHash));
		if (rtMap.containsKey(nodeIdHash)) entries.add(rtMap.get(nodeIdHash));
				if (useSecureRouting) {
			if (secRtMap.containsKey(nodeIdHash)) entries.add(secRtMap.get(nodeIdHash));
    	}
		return entries;
	}


	
	
	@Override
	public void discard() {	
	}




	

}
