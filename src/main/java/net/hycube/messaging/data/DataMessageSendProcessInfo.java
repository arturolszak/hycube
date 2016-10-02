package net.hycube.messaging.data;

import net.hycube.messaging.ack.AckProcessInfo;
import net.hycube.messaging.messages.Message;
import net.hycube.messaging.processing.MessageSendProcessInfo;
import net.hycube.transport.NetworkNodePointer;

public class DataMessageSendProcessInfo extends MessageSendProcessInfo {

//	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
//	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(DataMessageSendProcessInfo.class); 
	
	
	protected AckProcessInfo ackProcessInfo;
	
	protected boolean resent; 
	

	
	
	public DataMessageSendProcessInfo(Message msg, NetworkNodePointer directRecipient, AckProcessInfo ackProcessInfo, boolean processBeforeSend, boolean resent, Object[] routingParameters) {

		super(msg, directRecipient, processBeforeSend, routingParameters);
		
		this.ackProcessInfo = ackProcessInfo;
		
		this.resent = resent;
		
	}
	
	public AckProcessInfo getAckProcessInfo() {
		return ackProcessInfo;
	}

	public boolean isResent() {
		return resent;
	}
	
	public Object[] getRoutingParameters() {
		return routingParameters;
	}
	

	
}
