package net.hycube.test.nodeservices;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import net.hycube.HyCubeMultiQueueMultipleNodeService;
import net.hycube.HyCubeMultipleNodeService;
import net.hycube.HyCubeNodeService;
import net.hycube.HyCubeSchedulingMultipleNodeService;
import net.hycube.NodeService;
import net.hycube.NodeServiceException;
import net.hycube.core.HyCubeNodeId;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeId;
import net.hycube.environment.DirectEnvironment;
import net.hycube.environment.Environment;
import net.hycube.eventprocessing.EventCategory;
import net.hycube.eventprocessing.EventProcessingErrorCallback;
import net.hycube.eventprocessing.EventQueueProcessingInfo;
import net.hycube.eventprocessing.EventType;
import net.hycube.eventprocessing.ThreadPoolInfo;
import net.hycube.join.JoinWaitCallback;
import net.hycube.messaging.ack.MessageAckCallback;
import net.hycube.messaging.ack.WaitMessageAckCallback;
import net.hycube.messaging.data.DataMessage;
import net.hycube.messaging.data.ReceivedDataMessage;
import net.hycube.messaging.processing.MessageSendInfo;
import net.hycube.transport.MessageReceiver;

public class TestMultipleNodeService {

	
	
	private static final long PROCESSING_THREAD_KEEP_ALIVE_TIME = 60;
	private static final int NUM_THREADS = 6;
	private static final int NUM_MR_THREADS = 2;
	private static final int NUM_NON_MR_THREADS = 4;


	/**
	 * @param args
	 */
	
	public static void main(String[] args) {
		
		String ip = "127.0.0.1";
		//String ip = "192.168.2.110";
		int startPort = 56000;
		int numNodes = 100;
		int recoveryRepeat = 1;
		
		//boolean separateMRQueue = true;
		boolean separateMRQueue = false;
		
		int testsNum = 1;
		
		for (int i = 0; i < testsNum; i++) {
			System.out.println("Test #" + i);
			runTest(ip, startPort, numNodes, recoveryRepeat, separateMRQueue);
			startPort += numNodes;
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
		
		
		System.out.println();
		System.out.println("msgCounter:     " + msgCounter);
		System.out.println("msgRecCounter:  " + msgRecCounter);
		
		
	}
	
	
	@SuppressWarnings("unchecked")
	public static void runTest(String ip, int startPort, int numNodes, int recoveryRepeat, boolean separateMRQueue) {
		
		Random rand = new Random();
		
		String[] addresses = new String[numNodes];
		for (int i=0; i<numNodes; i++) {
			String address = ip + ":" + (startPort+i);
			addresses[i] = address;
		}

		
		try {
			
			int n = addresses.length;
			
			Environment environment = DirectEnvironment.initialize();
			
			HyCubeMultipleNodeService mns;
			

			HyCubeNodeService[] ns = new HyCubeNodeService[n];
			
			NodeId[] nodeIds = new NodeId[n];
			
			LinkedBlockingQueue<ReceivedDataMessage>[] msgQueues = (LinkedBlockingQueue<ReceivedDataMessage>[]) new LinkedBlockingQueue<?>[n];
			
			
			
			
			EventQueueProcessingInfo[] eventQueuesProcessingInfo;
			
			if (separateMRQueue) {
			
				List<EventType> eventTypesOE = new ArrayList<EventType>(EventCategory.values().length);
				for (EventCategory ec : EventCategory.values()) {
					if (ec != EventCategory.receiveMessageEvent) {
						EventType et = new EventType(ec);
						eventTypesOE.add(et);
					}
				}
				
				ThreadPoolInfo tpiMR = new ThreadPoolInfo(NUM_MR_THREADS, PROCESSING_THREAD_KEEP_ALIVE_TIME);
				EventQueueProcessingInfo eqpiMR = new EventQueueProcessingInfo(tpiMR, new EventType[] {new EventType(EventCategory.receiveMessageEvent)}, true);
				
				ThreadPoolInfo tpiOE = new ThreadPoolInfo(NUM_NON_MR_THREADS, PROCESSING_THREAD_KEEP_ALIVE_TIME);
				EventQueueProcessingInfo eqpiOE = new EventQueueProcessingInfo(tpiOE, (EventType[])eventTypesOE.toArray(new EventType[eventTypesOE.size()]), false);
				
				eventQueuesProcessingInfo = new EventQueueProcessingInfo[] {eqpiOE, eqpiMR};
				
			}
			else {
				
				List<EventType> eventTypes = new ArrayList<EventType>(EventCategory.values().length);
				for (EventCategory ec : EventCategory.values()) {
					EventType et = new EventType(ec);
					eventTypes.add(et);
				}
				
				ThreadPoolInfo tpi = new ThreadPoolInfo(NUM_THREADS, PROCESSING_THREAD_KEEP_ALIVE_TIME);
				EventQueueProcessingInfo eqpi = new EventQueueProcessingInfo(tpi, (EventType[])eventTypes.toArray(new EventType[eventTypes.size()]), true);
				
				eventQueuesProcessingInfo = new EventQueueProcessingInfo[] {eqpi};
				
			}
			
			
			
			EventProcessingErrorCallback errorCallback = new EventProcessingErrorCallback() {
				@Override
				public void errorOccurred(Object arg) {
					System.out.println("A processing error occured. Error callback argument: " + arg.toString());
				}
			};
			
			Object errorCallbackArg = "ERROR CALLBACK ARG";
			
			
			
			//!!! SET APPROPRIATE LINE TO TEST ONE OF THE SERVICES:
			int service = 2;
			
			switch (service) {
				case 1:
					mns = HyCubeMultiQueueMultipleNodeService.initialize(environment, eventQueuesProcessingInfo, errorCallback, errorCallbackArg);
					break;
				case 2:
					mns = HyCubeSchedulingMultipleNodeService.initialize(environment, eventQueuesProcessingInfo, 0, errorCallback, errorCallbackArg);
					break;
				default:
					return;
			}
			
			
			
			
			
			//message receiver
			
			MessageReceiver messageReceiver = mns.initializeMessageReceiver();
			
			
			
			System.out.print("Join: " );
			for (int i = 0; i < n; i++) {
				
				System.out.print(i);
				System.out.flush();
				
				JoinWaitCallback joinWaitCallback = new JoinWaitCallback();
				String bootstrap = null;
				
				if (i >= 1) {
					int bootstrapIndex = rand.nextInt(i);
					bootstrap = addresses[bootstrapIndex];
				}
				
				NodeId nodeId = HyCubeNodeId.generateRandomNodeId(4, 32);
				
				
				
				ns[i] = mns.initializeNode(nodeId, addresses[i], bootstrap, joinWaitCallback, null, messageReceiver);
				
				
				
				nodeIds[i] = nodeId;
				msgQueues[i] = ns[i].registerPort((short)0);

				
				try {
					joinWaitCallback.waitJoin();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				
				System.out.print("#,");
				System.out.flush();
				
			}
			System.out.println();
			
			
			
			
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			for (int r = 0; r < recoveryRepeat; r++) {
				System.out.print("Recovering: ");
				System.out.flush();
				for (int i = n-1; i >= 0; i--) {
					System.out.print(i + ",");
					System.out.flush();
					
					ns[i].recover();
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

					
				}
				System.out.println();
			}
			
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			
			
			
			
			
			
			
			for (int from = 0; from < n; from++) {
				for (int to = 0; to < n; to++) {
					System.out.println("From: " + from + " to: " + to);
					sendAndRecv(from, to, ns, nodeIds, msgQueues);
				}
			}
				
			
			
				
			
			
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			
			
			
			
			System.out.print("Discarding: ");
			System.out.flush();
			
			for (int i = 0; i < n; i++) {
				System.out.print(i + ",");
				System.out.flush();
				mns.discardNode(ns[i]);
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			System.out.println();
			
			mns.discard();
			
			System.out.println("Discarded.");
			
			
			
			
			environment.discard();
			
			
			
			System.out.println("*** Finished ***");
			
			
			
			
		} catch (InitializationException e) {
			e.printStackTrace();
		}
		

	}
	
	
	
	public static void sendAndRecv(int from, int to, NodeService[] ns, NodeId[] nodeIds, LinkedBlockingQueue<ReceivedDataMessage>[] msgQueues) {
		try {
			sendTestDataMessage(ns[from], nodeIds[to], "Test string");
			Thread.sleep(20);	
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	
	
	
	protected static int msgCounter = 0;
	protected static int msgRecCounter = 0;
	
	
	public static void sendTestDataMessage(NodeService ns, NodeId nodeId, String text) {
		
		MessageSendInfo msi = null;
		MessageAckCallback mac = new WaitMessageAckCallback() {
			public void notifyDelivered(Object callbackArg) {
				super.notifyDelivered(callbackArg);
				msgRecCounter++;
				System.out.println("Message DELIVERED");
			}
			
			public void notifyUndelivered(Object callbackArg) {
				super.notifyUndelivered(callbackArg);
				System.out.println("Message UNDELIVERED");
			}
		};
		
		//byte[] data = new byte[1024];
		byte[] data = text.getBytes();
		
		

		DataMessage msg = new DataMessage(nodeId, null, (short)0, (short)0, data);
		try {
			msi = ns.send(msg, mac, null);
			System.out.println("Message send info - serial no: " + msi.getSerialNo());
		} catch (NodeServiceException e) {
			e.printStackTrace();
		}
		msgCounter++;

	}
	
	
}
