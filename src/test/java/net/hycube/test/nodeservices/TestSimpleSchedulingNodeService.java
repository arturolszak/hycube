package net.hycube.test.nodeservices;

import java.util.concurrent.LinkedBlockingQueue;

import net.hycube.HyCubeSimpleSchedulingNodeService;
import net.hycube.NodeService;
import net.hycube.NodeServiceException;
import net.hycube.SimpleSchedulingNodeService;
import net.hycube.core.HyCubeNodeId;
import net.hycube.core.InitializationException;
import net.hycube.environment.DirectEnvironment;
import net.hycube.environment.Environment;
import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventCategory;
import net.hycube.eventprocessing.EventProcessException;
import net.hycube.eventprocessing.EventScheduler;
import net.hycube.eventprocessing.ProcessEventProxy;
import net.hycube.join.JoinWaitCallback;
import net.hycube.messaging.ack.MessageAckCallback;
import net.hycube.messaging.ack.WaitMessageAckCallback;
import net.hycube.messaging.data.DataMessage;
import net.hycube.messaging.data.ReceivedDataMessage;
import net.hycube.messaging.processing.MessageSendInfo;

public class TestSimpleSchedulingNodeService {

	public static final String address = "127.0.0.1:10026";
	
	
	public static class TestProcessEventProxy implements ProcessEventProxy {

		@Override
		public void processEvent(Event event) throws EventProcessException {
			System.out.println("*** Processing the external event: " + event.getEventArg());
		}
		
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {


		
		
		try {
			
			Environment environment = DirectEnvironment.initialize();
			JoinWaitCallback joinWaitCallback = new JoinWaitCallback();
			SimpleSchedulingNodeService sns = HyCubeSimpleSchedulingNodeService.initialize(environment, HyCubeNodeId.generateRandomNodeId(4, 32), address, null, joinWaitCallback, null, 0, true, null, null);
			
			LinkedBlockingQueue<ReceivedDataMessage> inMsgQueue = sns.registerPort((short)0);

			EventScheduler scheduler = sns.getEventScheduler();
			ProcessEventProxy ep = new TestProcessEventProxy();
			scheduler.scheduleEventWithDelay(new Event(0, EventCategory.extEvent, ep, "500:1"), null, 500);
			scheduler.scheduleEventWithDelay(new Event(0, EventCategory.extEvent, ep, "1500:1"), null, 1500);
			scheduler.scheduleEventWithDelay(new Event(0, EventCategory.extEvent, ep, "1500:2"), null, 1500);
			scheduler.scheduleEventWithDelay(new Event(0, EventCategory.extEvent, ep, "1400:1"), null, 1400);
			scheduler.scheduleEventWithDelay(new Event(0, EventCategory.extEvent, ep, "1600:1"), null, 1600);
			scheduler.scheduleEventWithDelay(new Event(0, EventCategory.extEvent, ep, "2000:1"), null, 2000);
			scheduler.scheduleEventWithDelay(new Event(0, EventCategory.extEvent, ep, "2050:1"), null, 2050);
			scheduler.scheduleEventWithDelay(new Event(0, EventCategory.extEvent, ep, "400:1"), null, 400);
			scheduler.scheduleEventWithDelay(new Event(0, EventCategory.extEvent, ep, "800:1"), null, 800);
			scheduler.scheduleEventWithDelay(new Event(0, EventCategory.extEvent, ep, "345:1"), null, 345);
			scheduler.scheduleEventWithDelay(new Event(0, EventCategory.extEvent, ep, "1860:1"), null, 1860);
			scheduler.scheduleEventWithDelay(new Event(0, EventCategory.extEvent, ep, "6080:1"), null, 6080);
			scheduler.scheduleEventWithDelay(new Event(0, EventCategory.extEvent, ep, "6090:1"), null, 6090);
			scheduler.scheduleEventWithDelay(new Event(0, EventCategory.extEvent, ep, "8390:1"), null, 8390);
			scheduler.scheduleEventWithDelay(new Event(0, EventCategory.extEvent, ep, "8490:1"), null, 8490);
			scheduler.scheduleEventWithDelay(new Event(0, EventCategory.extEvent, ep, "8590:1"), null, 8590);
			scheduler.scheduleEventWithDelay(new Event(0, EventCategory.extEvent, ep, "8690:1"), null, 8690);
			scheduler.scheduleEventWithDelay(new Event(0, EventCategory.extEvent, ep, "8790:1"), null, 8790);
			scheduler.scheduleEventWithDelay(new Event(0, EventCategory.extEvent, ep, "8890:1"), null, 8890);
			
			
			
			
			try {
				Thread.sleep(1000);
				sendTestDataMessageToSelf(sns, "Test string");
				Thread.sleep(30000);	
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
