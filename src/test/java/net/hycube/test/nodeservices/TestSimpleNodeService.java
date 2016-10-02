package net.hycube.test.nodeservices;

import java.util.concurrent.LinkedBlockingQueue;

import net.hycube.HyCubeSimpleNodeService;
import net.hycube.NodeService;
import net.hycube.NodeServiceException;
import net.hycube.SimpleNodeService;
import net.hycube.core.HyCubeNodeId;
import net.hycube.core.InitializationException;
import net.hycube.environment.DirectEnvironment;
import net.hycube.environment.Environment;
import net.hycube.join.JoinWaitCallback;
import net.hycube.messaging.ack.MessageAckCallback;
import net.hycube.messaging.ack.WaitMessageAckCallback;
import net.hycube.messaging.data.DataMessage;
import net.hycube.messaging.data.ReceivedDataMessage;
import net.hycube.messaging.processing.MessageSendInfo;

public class TestSimpleNodeService {

	public static final String address = "127.0.0.1:10013";
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		try {
			
			Environment environment = DirectEnvironment.initialize();
			JoinWaitCallback joinWaitCallback = new JoinWaitCallback();
			SimpleNodeService sns = HyCubeSimpleNodeService.initialize(environment, HyCubeNodeId.generateRandomNodeId(4, 32), address, null, joinWaitCallback, null, 0, true, null, null);
			
			LinkedBlockingQueue<ReceivedDataMessage> inMsgQueue = sns.registerPort((short)0);
			
			
			try {
				Thread.sleep(1000);
				sendTestDataMessageToSelf(sns, "Test string");
				Thread.sleep(3000);	
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			ReceivedDataMessage recMsg = null;
			try {
				recMsg = inMsgQueue.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("Success! Message received!: " + new String(recMsg.getData()));
			
			
			
			sns.discard();
			
			environment.discard();
			
		} catch (InitializationException e) {
			e.printStackTrace();
		}
		

	}
	
	
	protected static int msgCounter = 0;
	
	public static void sendTestDataMessageToSelf(NodeService ns, String text) {
		
		MessageSendInfo msi = null;
		MessageAckCallback mac = new WaitMessageAckCallback() {
			public void notifyDelivered(Object callbackArg) {
				super.notifyDelivered(callbackArg);
				System.out.println("Message DELIVERED.");
			}
			
			public void notifyUndelivered(Object callbackArg) {
				super.notifyUndelivered(callbackArg);
				System.out.println("Message UNDELIVERED");
			}
		};
		
		byte[] data = new byte[1024];
		data = text.getBytes();
		
		DataMessage msg = new DataMessage(ns.getNode().getNodeId(), null, (short)0, (short)0, data);
		try {
			msi = ns.send(msg, mac, null);
			System.out.println("Message send info - serial no: " + msi.getSerialNo());
		} catch (NodeServiceException e) {
			e.printStackTrace();
		}
		msgCounter++;

	}

}
