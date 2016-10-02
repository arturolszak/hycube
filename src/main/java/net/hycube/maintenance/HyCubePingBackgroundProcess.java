package net.hycube.maintenance;

import java.util.HashMap;
import java.util.List;

import net.hycube.backgroundprocessing.AbstractBackgroundProcess;
import net.hycube.backgroundprocessing.BackgroundProcessException;
import net.hycube.configuration.GlobalConstants;
import net.hycube.core.HyCubeRoutingTable;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.RoutingTableEntry;
import net.hycube.environment.NodeProperties;
import net.hycube.logging.LogHelper;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.HyCubeMessageFactory;
import net.hycube.messaging.messages.HyCubeMessageType;
import net.hycube.messaging.processing.ProcessMessageException;
import net.hycube.transport.NetworkAdapterException;
import net.hycube.utils.HashMapUtils;

public class HyCubePingBackgroundProcess extends AbstractBackgroundProcess {

	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubePingBackgroundProcess.class);
	
	
	protected static final String PROP_KEY_KEEP_ALIVE_EXTENSION_KEY = "KeepAliveExtensionKey";
	
 
	protected HyCubeRoutingTable routingTable;
	
	protected String keepAliveExtensionKey;
	protected HyCubeKeepAliveExtension keepAliveExtension;
	
	protected HyCubeMessageFactory messageFactory;
	
	
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		if (userLog.isDebugEnabled()) {
			userLog.debug("Initializing ping backround process...");
		}
		if (devLog.isDebugEnabled()) {
			devLog.debug("Initializing ping backround process...");
		}
		
		
		super.initialize(nodeAccessor, properties);

		
		if (!(nodeAccessor.getRoutingTable() instanceof HyCubeRoutingTable)) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "The routing table is expected to be an instance of: " + HyCubeRoutingTable.class.getName());
		}
		this.routingTable = (HyCubeRoutingTable) nodeAccessor.getRoutingTable();

		this.messageFactory = (HyCubeMessageFactory) nodeAccessor.getMessageFactory();

		
		
		this.keepAliveExtensionKey = properties.getProperty(PROP_KEY_KEEP_ALIVE_EXTENSION_KEY);
		if (keepAliveExtensionKey == null || keepAliveExtensionKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_KEEP_ALIVE_EXTENSION_KEY), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_KEEP_ALIVE_EXTENSION_KEY));

		try {
			this.keepAliveExtension = (HyCubeKeepAliveExtension) nodeAccessor.getExtension(this.keepAliveExtensionKey);
			if (this.keepAliveExtension == null) throw new InitializationException(InitializationException.Error.MISSING_EXTENSION_ERROR, this.keepAliveExtensionKey, "The KeepAliveExtension is missing at the specified key: " + this.keepAliveExtensionKey + ".");
		} catch (ClassCastException e) {
			throw new InitializationException(InitializationException.Error.MISSING_EXTENSION_ERROR, this.keepAliveExtensionKey, "The KeepAliveExtension is missing at the specified key: " + this.keepAliveExtensionKey + ".");
		}
		
				
	}

	@Override
	public void doProcess() throws BackgroundProcessException {

		try {
			pingNeighbours();
		} catch (Exception e) {
			throw new BackgroundProcessException("An exception thrown while pinging neighbours.", e);
		}
				
	}
	
	
	   /*
     * Send ping messages to all neighbors
     */
    protected void pingNeighbours() throws NetworkAdapterException, ProcessMessageException {

    	if (devLog.isDebugEnabled()) {
			devLog.debug("Pinging neighbors.");
		}
    	
    	List<RoutingTableEntry> rteList;
    	int neighborsCount;
    	
    	
    	routingTable.lockRoutingTableForRead();
    	rteList = routingTable.getAllRoutingTableEntries();
    	routingTable.unlockRoutingTableForRead();
    	
    	neighborsCount = rteList.size();
    	
    	HashMap<Long, Object> pinged = new HashMap<Long, Object>(HashMapUtils.getHashMapCapacityForElementsNum(neighborsCount, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
    	
    	for (RoutingTableEntry rte : rteList) {
    		
    		if (rte.isDiscarded()) continue;
    		
    		long neighHash = rte.getNodeIdHash();
        	//check if the node was already pinged in this iteration and if so, skip it
    		if (!pinged.containsKey(neighHash)) {
   				pingNeighbor(rte);
    			pinged.put(neighHash, rte.getNode());
    		}

    	}

    }

    protected void pingNeighbor(RoutingTableEntry rte) throws NetworkAdapterException, ProcessMessageException {    	
    	if (devLog.isDebugEnabled()) {
			devLog.debug("Pinging node: " + rte.getNode().getNodeId().toHexString());
		}
    	HyCubeMessage pingMsg = messageFactory.newMessage(nodeAccessor.getNextMessageSerialNo(), nodeAccessor.getNodeId(), rte.getNode().getNodeId(), nodeAccessor.getNetworkAdapter().getPublicAddressBytes(), HyCubeMessageType.PING, nodeAccessor.getNodeParameterSet().getMessageTTL(), (short)0, false, false, (short)0, (short)0, null); 
    	HyCubePingMessageSendProcessInfo pmspi = new HyCubePingMessageSendProcessInfo(HyCubeMessageType.PING, pingMsg, rte.getNode().getNetworkNodePointer(), rte.getNode(), true);
    	nodeAccessor.sendMessage(pmspi, GlobalConstants.WAIT_ON_BKG_MSG_SEND);
    	
    }

        
}
