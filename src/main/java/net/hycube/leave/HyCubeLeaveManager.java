package net.hycube.leave;

import java.util.List;

import net.hycube.core.HyCubeNodeIdFactory;
import net.hycube.core.HyCubeRoutingTable;
import net.hycube.core.HyCubeRoutingTableSlotInfo;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodePointer;
import net.hycube.core.RoutingTableEntry;
import net.hycube.core.UnrecoverableRuntimeException;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.environment.NodeProperties;
import net.hycube.messaging.messages.HyCubeMessageFactory;
import net.hycube.messaging.messages.HyCubeMessageType;
import net.hycube.messaging.messages.Message;
import net.hycube.messaging.processing.MessageSendProcessInfo;
import net.hycube.messaging.processing.ProcessMessageException;
import net.hycube.transport.NetworkAdapterException;
import net.hycube.transport.NetworkNodePointer;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public class HyCubeLeaveManager implements LeaveManager {

	
	protected static final String PROP_KEY_BLOCKING_SEND_LEAVE = "BlockingSendLeave";
	protected static final String PROP_KEY_WAIT_AFTER_SEND_LEAVE_TIME = "WaitAfterSendLeaveTime";
	protected static final String PROP_KEY_PROCESS_RECEIVED_NODES = "ProcessReceivedNodes";
	
	
	protected NodeAccessor nodeAccessor;
	protected NodeProperties properties;
	
	protected HyCubeMessageFactory messageFactory;
	protected HyCubeNodeIdFactory nodeIdFactory;
	protected HyCubeRoutingTable routingTable;
	
	protected boolean blockingSendLeave;
	protected int waitAfterSendLeaveTime;
	
	protected boolean processReceivedNodes;
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		this.nodeAccessor = nodeAccessor;
		this.properties = properties;
		
		if (! (nodeAccessor.getMessageFactory() instanceof HyCubeMessageFactory)) throw new UnrecoverableRuntimeException("The message factory is expected to be an instance of: " + HyCubeMessageFactory.class.getName() + ".");
		messageFactory = (HyCubeMessageFactory) nodeAccessor.getMessageFactory();
		
		if (! (nodeAccessor.getNodeIdFactory() instanceof HyCubeNodeIdFactory)) throw new UnrecoverableRuntimeException("The node id factory is expected to be an instance of: " + HyCubeNodeIdFactory.class.getName() + ".");
		nodeIdFactory = (HyCubeNodeIdFactory) nodeAccessor.getNodeIdFactory();
		
		
		if (! (nodeAccessor.getRoutingTable() instanceof HyCubeRoutingTable)) throw new UnrecoverableRuntimeException("The routing table is expected to be an instance of: " + HyCubeRoutingTable.class.getName() + ".");
		routingTable = (HyCubeRoutingTable) nodeAccessor.getRoutingTable();
		
		try {
			
			this.blockingSendLeave = (Boolean) properties.getProperty(PROP_KEY_BLOCKING_SEND_LEAVE, MappedType.BOOLEAN);
			this.waitAfterSendLeaveTime = (Integer) properties.getProperty(PROP_KEY_WAIT_AFTER_SEND_LEAVE_TIME, MappedType.INT);
			
			this.processReceivedNodes = (Boolean) properties.getProperty(PROP_KEY_PROCESS_RECEIVED_NODES, MappedType.BOOLEAN);
			
			
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize the join manager instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		
	}

	@Override
	public void leave() {
		
		//prepare the nodes to be included in the leave message
		
		NodePointer[] nsNodes;
		synchronized (routingTable) {
			nsNodes = new NodePointer[routingTable.getNeighborhoodSet().size()];
			for (int i = 0; i < routingTable.getNeighborhoodSet().size(); i++) {
				nsNodes[i] = routingTable.getNeighborhoodSet().get(i).getNode();
			}
		}
		
		
		//send the leave message to all nodes in NS:
		for (NodePointer np : nsNodes) {
		
			//prepare the message:
			
			int messageSerialNo = nodeAccessor.getNextMessageSerialNo();
			byte[] leaveMessageData = (new HyCubeLeaveMessageData(nsNodes)).getBytes(nodeIdFactory.getDimensions(), nodeIdFactory.getDigitsCount());
			Message leaveMessage = messageFactory.newMessage(messageSerialNo, nodeAccessor.getNodeId(), np.getNodeId(), nodeAccessor.getNetworkAdapter().getPublicAddressBytes(), HyCubeMessageType.LEAVE, nodeAccessor.getNodeParameterSet().getMessageTTL(), (short)0, false, false, (short)0, (short)0, leaveMessageData); 
				
	
			//send the message directly to the recipient:
			try {
				nodeAccessor.sendMessage(new MessageSendProcessInfo(leaveMessage, np.getNetworkNodePointer(), false), this.blockingSendLeave);
			} catch (NetworkAdapterException e) {
				throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a leave message to a node.", e);
			} catch (ProcessMessageException e) {
				throw new UnrecoverableRuntimeException("An exception has been thrown while trying to send a leave message to a node.", e);
			}

		}
	
		if (this.waitAfterSendLeaveTime > 0) {
			try {
				Thread.sleep(this.waitAfterSendLeaveTime);
			} catch (InterruptedException e) {
				//ignore
			}
		}
		
		
	}
	
	
	public void processLeaveMessage(NodePointer sender, NetworkNodePointer directSender, NodePointer[] nodePointers) {
		
		//the sender should be removed from the routing tables (if the local routing tables contain a reference to sender)
		
		long senderIdHash = sender.getNodeIdHash();
		
		routingTable.lockRoutingTableForWrite();
		
		List<RoutingTableEntry> rtes = routingTable.getRoutingTableEntriesByNodeIdHash(senderIdHash);
		
		if (rtes != null) {
			
			for (RoutingTableEntry rte : rtes) {
				
				HyCubeRoutingTableSlotInfo slotInfo = (HyCubeRoutingTableSlotInfo) rte.getOuterRef();
				
				//remove the reference
				slotInfo.getSlot().remove(rte);
				
				//remove from the rt map:
				slotInfo.getRteMap().remove(rte.getNodeIdHash());
				
			}
			
		}
		
		
		routingTable.unlockRoutingTableForWrite();
		
		

		if (processReceivedNodes) {
			for (NodePointer np : nodePointers) {
				nodeAccessor.getNotifyProcessor().processNotify(np, nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime());
			}
		
		}
		
		
	}

	
	
	@Override
	public void discard() {	
	}
	
	
	
}
