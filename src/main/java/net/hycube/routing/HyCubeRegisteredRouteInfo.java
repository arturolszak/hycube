package net.hycube.routing;

import net.hycube.core.NodePointer;
import net.hycube.transport.NetworkNodePointer;

public class HyCubeRegisteredRouteInfo {

	
	public HyCubeRegisteredRouteInfo(NodePointer inNodePointer, int inRouteId, NetworkNodePointer outNetworkNodePointer, int outRouteId, boolean routeStart, long time) {
		
		setValues(inNodePointer, inRouteId, outNetworkNodePointer, outRouteId, routeStart, time);
		
	}
	

	//creates an empty route info with only the time specified
	public HyCubeRegisteredRouteInfo(long time) {
		
		this.time = time;
		
	}
	
	public void setValues(NodePointer inNodePointer, int inRouteId, NetworkNodePointer outNetworkNodePointer, int outRouteId, boolean routeStart, long time) {
		
		this.inNodePointer = inNodePointer;
		this.inRouteId = inRouteId;
		
		this.outNetworkNodePointer = outNetworkNodePointer;
		this.outRouteId = outRouteId;
		
		this.routeStart = routeStart;
		
		this.time = time;
		
	}
	
	
	protected NodePointer inNodePointer;
	protected int inRouteId;
	
	protected NetworkNodePointer outNetworkNodePointer;
	protected int outRouteId;
	
	protected boolean routeStart;
	
	protected long time;

	
	
	
	public NodePointer getInNodePointer() {
		return inNodePointer;
	}

	public void setInNodePointer(NodePointer inNodePointer) {
		this.inNodePointer = inNodePointer;
	}

	public int getInRouteId() {
		return inRouteId;
	}

	public void setInRouteId(int inRouteId) {
		this.inRouteId = inRouteId;
	}

	public NetworkNodePointer getOutNetworkNodePointer() {
		return outNetworkNodePointer;
	}

	public void setOutNetworkNodePointer(NetworkNodePointer outNetworkNodePointer) {
		this.outNetworkNodePointer = outNetworkNodePointer;
	}

	public int getOutRouteId() {
		return outRouteId;
	}

	public void setOutRouteId(int outRouteId) {
		this.outRouteId = outRouteId;
	}

	public boolean isRouteStart() {
		return routeStart;
	}

	public void setRouteStart(boolean routeStart) {
		this.routeStart = routeStart;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}
	
	
	
	
}
