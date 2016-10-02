package net.hycube.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public interface HyCubeRoutingTable extends RoutingTable {

	/**
	 * @return the routingTable1
	 */
	public List<RoutingTableEntry>[][] getRoutingTable1();

	/**
	 * @param routingTable1 the routingTable1 to set
	 */
	public void setRoutingTable1(List<RoutingTableEntry>[][] routingTable1);

	/**
	 * @return the routingTable2
	 */
	public List<RoutingTableEntry>[][] getRoutingTable2();

	/**
	 * @param routingTable2 the routingTable2 to set
	 */
	public void setRoutingTable2(List<RoutingTableEntry>[][] routingTable2);

	/**
	 * @return the secRoutingTable1
	 */
	public List<RoutingTableEntry>[][] getSecRoutingTable1();

	/**
	 * @param secRoutingTable1 the secRoutingTable1 to set
	 */
	public void setSecRoutingTable(List<RoutingTableEntry>[][] secRoutingTable1);

	/**
	 * @return the secRoutingTable2
	 */
	public List<RoutingTableEntry>[][] getSecRoutingTable2();

	/**
	 * @param secRoutingTable2 the secRoutingTable2 to set
	 */
	public void setSecRoutingTable2(List<RoutingTableEntry>[][] secRoutingTable2);

	/**
	 * @return the neighborhoodSet
	 */
	public List<RoutingTableEntry> getNeighborhoodSet();

	/**
	 * @param neighborhoodSet the neighborhoodSet to set
	 */
	public void setLeafSet(List<RoutingTableEntry> neighborhoodSet);

	/**
	 * @return the nsMap
	 */
	public HashMap<Long, RoutingTableEntry> getNsMap();

	/**
	 * @param nsMap the nsMap to set
	 */
	public void setNsMap(HashMap<Long, RoutingTableEntry> nsMap);

	/**
	 * @return the rt1Map
	 */
	public HashMap<Long, RoutingTableEntry> getRt1Map();

	/**
	 * @param rt1Map the rt1Map to set
	 */
	public void setRt1Map(HashMap<Long, RoutingTableEntry> rt1Map);

	/**
	 * @return the rt2Map
	 */
	public HashMap<Long, RoutingTableEntry> getRt2Map();

	/**
	 * @param rt2Map the rt2Map to set
	 */
	public void setRt2Map(HashMap<Long, RoutingTableEntry> rt2Map);

	/**
	 * @return the secRt1Map
	 */
	public HashMap<Long, RoutingTableEntry> getSecRt1Map();

	/**
	 * @param secRt1Map the secRt1Map to set
	 */
	public void setSecRt1Map(HashMap<Long, RoutingTableEntry> secRt1Map);

	/**
	 * @return the secRt2Map
	 */
	public HashMap<Long, RoutingTableEntry> getSecRt2Map();

	/**
	 * @param secRt2Map the secRt2Map to set
	 */
	public void setSecRt2Map(HashMap<Long, RoutingTableEntry> secRt2Map);

	/**
	 * @return the rteMaps
	 */
	public List<Map<Long, RoutingTableEntry>> getRteMaps();

	/**
	 * @param rteMaps the rteMaps to set
	 */
	public void setRteMaps(List<Map<Long, RoutingTableEntry>> rteMaps);

	public ReentrantReadWriteLock getNsLock();

	public ReentrantReadWriteLock getRt1Lock();

	public ReentrantReadWriteLock getRt2Lock();

	public ReentrantReadWriteLock getSecRt1Lock();

	public ReentrantReadWriteLock getSecRt2Lock();

	public ReentrantReadWriteLock getLockByRtType(HyCubeRoutingTableType type);

	public int getDimensions();

	public int getDigitsCount();

	public int getNSSize();

	public int getRoutingTableSlotSize();

	public boolean isUseSecureRouting();

	public void lockRoutingTableForRead();

	public void unlockRoutingTableForRead();

	public void lockRoutingTableForWrite();

	public void unlockRoutingTableForWrite();

	public int getNeighborsCount();

}