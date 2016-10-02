package net.hycube.pastry.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.hycube.core.RoutingTable;
import net.hycube.core.RoutingTableEntry;

public interface PastryRoutingTable extends RoutingTable {

	/**
	 * @return the routingTable
	 */
	public List<RoutingTableEntry>[][] getRoutingTable();

	/**
	 * @param routingTable the routingTable to set
	 */
	public void setRoutingTable(List<RoutingTableEntry>[][] routingTable);

	/**
	 * @return the secRoutingTable
	 */
	public List<RoutingTableEntry>[][] getSecRoutingTable();

	/**
	 * @param secRoutingTable the secRoutingTable to set
	 */
	public void setSecRoutingTable(List<RoutingTableEntry>[][] secRoutingTable);

	/**
	 * @return the leafSet
	 */
	public List<RoutingTableEntry> getLeafSet();

	/**
	 * @param leafSet the leafSet to set
	 */
	public void setLeafSet(List<RoutingTableEntry> leafSet);

	/**
	 * @return the lsMap
	 */
	public HashMap<Long, RoutingTableEntry> getLsMap();

	/**
	 * @param lsMap the lsMap to set
	 */
	public void setLsMap(HashMap<Long, RoutingTableEntry> lsMap);

	/**
	 * @return the rtMap
	 */
	public HashMap<Long, RoutingTableEntry> getRtMap();

	/**
	 * @param rtMap the rtMap to set
	 */
	public void setRtMap(HashMap<Long, RoutingTableEntry> rtMap);

	/**
	 * @return the secRtMap
	 */
	public HashMap<Long, RoutingTableEntry> getSecRtMap();

	/**
	 * @param secRtMap the secRtMap to set
	 */
	public void setSecRtMap(HashMap<Long, RoutingTableEntry> secRtMap);

	/**
	 * @return the rteMaps
	 */
	public List<Map<Long, RoutingTableEntry>> getRteMaps();

	/**
	 * @param rteMaps the rteMaps to set
	 */
	public void setRteMaps(List<Map<Long, RoutingTableEntry>> rteMaps);

	public ReentrantReadWriteLock getLsLock();

	public ReentrantReadWriteLock getRtLock();

	public ReentrantReadWriteLock getSecRtLock();

	public int getDimensions();

	public int getDigitsCount();

	public int getLSSize();

	public int getRoutingTableSlotSize();

	public boolean isUseSecureRouting();
	
	public void lockRoutingTableForRead();

	public void unlockRoutingTableForRead();

	public void lockRoutingTableForWrite();

	public void unlockRoutingTableForWrite();

	public int getNeighborsCount();

}
