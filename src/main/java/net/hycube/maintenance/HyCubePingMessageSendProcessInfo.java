package net.hycube.maintenance;

import net.hycube.core.NodePointer;
import net.hycube.core.RoutingTableEntry;
import net.hycube.messaging.messages.HyCubeMessageType;
import net.hycube.messaging.messages.Message;
import net.hycube.messaging.processing.MessageSendProcessInfo;
import net.hycube.transport.NetworkNodePointer;

public class HyCubePingMessageSendProcessInfo extends MessageSendProcessInfo {

	protected RoutingTableEntry rte;
	protected NodePointer nodePointer;
	
	
	public HyCubePingMessageSendProcessInfo(HyCubeMessageType messageType, Message msg, NetworkNodePointer directRecipient, NodePointer pingedNodePointer, boolean processSend) {
		super(msg, directRecipient, processSend);
		
		this.nodePointer = pingedNodePointer; 
		
	}
	
	
	public NodePointer getNodePointer() {
		return nodePointer;
	}


	
}
