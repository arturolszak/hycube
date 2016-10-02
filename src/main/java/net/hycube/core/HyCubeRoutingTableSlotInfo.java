package net.hycube.core;

import java.util.HashMap;
import java.util.List;

public class HyCubeRoutingTableSlotInfo {

	protected HyCubeRoutingTableType type;
	protected HashMap<Long, RoutingTableEntry> rteMap;
	protected List<RoutingTableEntry> slot;
	
	
	public HyCubeRoutingTableSlotInfo(HyCubeRoutingTableType type, HashMap<Long, RoutingTableEntry> rteMap, List<RoutingTableEntry> slot) {
		this.type = type;
		this.rteMap = rteMap;
		this.slot = slot;
		
	}
	
	
	public HyCubeRoutingTableType getType() {
		return type;
	}
	public void setType(HyCubeRoutingTableType type) {
		this.type = type;
	}
	
	
	public HashMap<Long, RoutingTableEntry> getRteMap() {
		return rteMap;
	}
	public void setRteMap(HashMap<Long, RoutingTableEntry> rteMap) {
		this.rteMap = rteMap;
	}
	
	


	public List<RoutingTableEntry> getSlot() {
		return slot;
	}
	public void setSlot(List<RoutingTableEntry> slot) {
		this.slot = slot;
	}
	
	
}
