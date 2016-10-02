package net.hycube.maintenance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.hycube.backgroundprocessing.AbstractBackgroundProcess;
import net.hycube.backgroundprocessing.BackgroundProcessException;
import net.hycube.core.HyCubeRoutingTable;
import net.hycube.core.HyCubeRoutingTableSlotInfo;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.RoutingTableEntry;
import net.hycube.environment.NodeProperties;
import net.hycube.logging.LogHelper;

public class HyCubeAwaitingPongsBackgroundProcess extends AbstractBackgroundProcess {

	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeAwaitingPongsBackgroundProcess.class); 
	
	
	protected static final String PROP_KEY_KEEP_ALIVE_EXTENSION_KEY = "KeepAliveExtensionKey";
	
	protected HyCubeRoutingTable routingTable;
	
	protected String keepAliveExtensionKey;
	protected HyCubeKeepAliveExtension keepAliveExtension;
	
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		if (userLog.isDebugEnabled()) {
			userLog.debug("Initializing backround processing of awaiting pong messages list...");
		}
		if (devLog.isDebugEnabled()) {
			devLog.debug("Initializing backround processing of awaiting pong messages list...");
		}
		
		
		super.initialize(nodeAccessor, properties);
		
		
		if (!(nodeAccessor.getRoutingTable() instanceof HyCubeRoutingTable)) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "The routing table is expected to be an instance of: " + HyCubeRoutingTable.class.getName());
		}
		this.routingTable = (HyCubeRoutingTable) nodeAccessor.getRoutingTable();

		
			
		keepAliveExtensionKey = properties.getProperty(PROP_KEY_KEEP_ALIVE_EXTENSION_KEY);
		if (keepAliveExtensionKey == null || keepAliveExtensionKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_KEEP_ALIVE_EXTENSION_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_KEEP_ALIVE_EXTENSION_KEY));
		
		
		try {
			this.keepAliveExtension = (HyCubeKeepAliveExtension) nodeAccessor.getExtension(this.keepAliveExtensionKey);
			if (this.keepAliveExtension == null) throw new InitializationException(InitializationException.Error.MISSING_EXTENSION_ERROR, this.keepAliveExtensionKey, "The KeepAliveExtension is missing at the specified key: " + this.keepAliveExtensionKey + ".");
		} catch (ClassCastException e) {
			throw new InitializationException(InitializationException.Error.MISSING_EXTENSION_ERROR, this.keepAliveExtensionKey, "The KeepAliveExtension is missing at the specified key: " + this.keepAliveExtensionKey + ".");
		}

		
		
	}

	@Override
	public void doProcess() {
		
		try {
			processAwaitingPongs();
		} catch (Exception e) {
			throw new BackgroundProcessException("An exception thrown while pprocessing awaiting pongs.", e);
		}
		
	}
	

	public void processAwaitingPongs() {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Processing awaiting pongs.");
		}
		
    	long currTimestamp = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
    	List<Integer> pongPrToRemove = null;
    	List<HyCubePongProcessInfo> pongPrList;
    	
    	//check awaiting pong map
    	synchronized (keepAliveExtension.getPongAwaitingLock()) {
	    	pongPrToRemove = new ArrayList<Integer>(keepAliveExtension.getPongAwaitingMap().size());
	    	pongPrList = new ArrayList<HyCubePongProcessInfo>(keepAliveExtension.getPongAwaitingMap().size());
	    	pongPrList.addAll(keepAliveExtension.getPongAwaitingMap().values());
    	}
	    for (HyCubePongProcessInfo pongPr : pongPrList) {
	    	synchronized (pongPr) {
	    		if (! pongPr.isProcessed()) {	//for thread safety check if processed (another thread could have processed it meanwhile)
			    	if (pongPr.getDiscardTimestamp() <= currTimestamp) {
			    		
			    		routingTable.lockRoutingTableForRead();
			    		List<RoutingTableEntry> rtes = nodeAccessor.getRoutingTable().getRoutingTableEntriesByNodeIdHash(pongPr.getNodeIdHash());
			    		routingTable.unlockRoutingTableForRead();
			    		
			    		for (RoutingTableEntry rte : rtes) {
			    			
			    			if (rte != null && Arrays.equals(rte.getNode().getNetworkNodePointer().getAddressBytes(), pongPr.getNodeNetworkAddress())) {

			    				HyCubeRoutingTableSlotInfo slotInfo = (HyCubeRoutingTableSlotInfo) rte.getOuterRef();
			    				
			    				//acquire the write lock
		    					routingTable.getLockByRtType(slotInfo.getType()).writeLock().lock();
			    				
			    				double rtePingResponseIndicator = (Double) rte.getData(keepAliveExtension.getPingResponseIndicatorRteKey(), keepAliveExtension.getInitialPingResponseIndicatorValue());
			    				rtePingResponseIndicator = rtePingResponseIndicator * (1 - keepAliveExtension.getPingResponseIndicatorUpdateCoefficient());
			    				rte.setData(keepAliveExtension.getPingResponseIndicatorRteKey(), rtePingResponseIndicator);
			    				if (rtePingResponseIndicator <= keepAliveExtension.getPingResponseIndicatorDeactivateThreshold()) {
			    					
			    					//deactivate the node
			    					//set disabled
		    						rte.setEnabled(false);
		    						if (devLog.isDebugEnabled()) {
		    							devLog.debug("Deactivating node: " + rte.getNode().getNetworkNodePointer().getAddressString());
		    						}
			    					
			    				}
			    				if (rtePingResponseIndicator <= keepAliveExtension.getPingResponseIndicatorRemoveThreshold()) {
			    					
			    					//remove the node
			    					
			    					//cache the pingResponseIndicator value
			    					keepAliveExtension.cachePingResponseIndicator(rte.getNode(), rtePingResponseIndicator);
			    					
			    					//remove from the slot:
			    					rte.setDiscarded(true);
			    					if (devLog.isDebugEnabled()) {
			    						devLog.debug("Discarding node: " + rte.getNode().getNetworkNodePointer().getAddressString());
			    					}
			    					
			    					slotInfo.getSlot().remove(rte);
			    					
			    					//remove from the rt map:
			    					slotInfo.getRteMap().remove(rte.getNodeIdHash());
			    					
			    					
			    					
			    				}
			    				
		    					//release the write lock
		    					routingTable.getLockByRtType(slotInfo.getType()).writeLock().unlock();
			    				
			    			}
			    		}
			    		pongPr.discard();
			    		pongPrToRemove.add(pongPr.getPingSerialNo());
			    	}
	    		}
	    	}
	    }
	    synchronized (keepAliveExtension.getPongAwaitingLock()) {
	    	for (int msgSerialNo : pongPrToRemove) {
	    		keepAliveExtension.getPongAwaitingMap().remove(msgSerialNo);
	    	}
    	}
		
    	
	}
		
	
}
