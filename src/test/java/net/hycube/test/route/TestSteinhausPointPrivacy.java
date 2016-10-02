package net.hycube.test.route;

import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import net.hycube.HyCubeNodeService;
import net.hycube.HyCubeSimpleNodeService;
import net.hycube.HyCubeSimpleSchedulingNodeService;
import net.hycube.NodeService;
import net.hycube.NodeServiceException;
import net.hycube.core.HyCubeNodeId;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeId;
import net.hycube.environment.DirectEnvironment;
import net.hycube.environment.Environment;
import net.hycube.join.JoinWaitCallback;
import net.hycube.messaging.ack.MessageAckCallback;
import net.hycube.messaging.ack.WaitMessageAckCallback;
import net.hycube.messaging.data.DataMessage;
import net.hycube.messaging.data.ReceivedDataMessage;
import net.hycube.messaging.processing.MessageSendInfo;
import net.hycube.routing.HyCubeRoutingManager;

public class TestSteinhausPointPrivacy {

	
	
	/**
	 * @param args
	 */
	
	public static void main(String[] args) {
		
		String ip = "127.0.0.1";
		//String ip = "192.168.2.110";
		int startPort = 56000;
		//int numNodes = 50;
		int numNodes = 50;
		int recoveryRepeat = 0;
		
		boolean registerRoute = true;
		
		boolean anonymousRoute = true;
		
		int testsNum = 1;
		
		for (int i = 0; i < testsNum; i++) {
			System.out.println("Test #" + i);
			runTest(ip, startPort, numNodes, recoveryRepeat, registerRoute, anonymousRoute);
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
	public static void runTest(String ip, int startPort, int numNodes, int recoveryRepeat, boolean registerRoute, boolean anonymousRoute) {
		
		Random rand = new Random();
		
		String[] addresses = new String[numNodes];
		for (int i=0; i<numNodes; i++) {
			String address = ip + ":" + (startPort+i);
			addresses[i] = address;
		}

		
		try {
			
			int n = addresses.length;
			
			Environment environment = DirectEnvironment.initialize();
			
			HyCubeNodeService[] ns = new HyCubeNodeService[n];

			NodeId[] nodeIds = new NodeId[n];
			LinkedBlockingQueue<ReceivedDataMessage>[] msgQueues = (LinkedBlockingQueue<ReceivedDataMessage>[]) new LinkedBlockingQueue<?>[n];
			
			HyCubeNodeId tmpNodeId = null;
			
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
				
				HyCubeNodeId nodeId;
				if (i % 2 == 0) {
					nodeId = HyCubeNodeId.generateRandomNodeId(4, 32);
					tmpNodeId = nodeId;
				}
				else {
					nodeId = HyCubeNodeId.parseNodeIdBinary(tmpNodeId.toBinaryString().replace('1', '3').replace('0', '1').replace('3', '0'), 4);
					System.out.println("\n %0: " + tmpNodeId.toBinaryString());
					System.out.println("\n~%0: " + nodeId.toBinaryString());
					tmpNodeId = null;
				}
				
				//!!! SET APPROPRIATE LINE TO TEST ONE OF THE SERVICES:
				int service = 2;
				
				switch (service) {
					case 1:
						ns[i] = HyCubeSimpleNodeService.initialize(environment, nodeId, addresses[i], bootstrap, joinWaitCallback, null, 0, true, null, null);
						break;
					case 2:
						ns[i] = HyCubeSimpleSchedulingNodeService.initialize(environment, nodeId, addresses[i], bootstrap, joinWaitCallback, null, 0, true, null, null);
						break;
					default:
						return;
				}
				
				
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
					sendAndRecv(from, to, ns, nodeIds, msgQueues, registerRoute, anonymousRoute);
				}
			}
				
			
//			sendAndRecv(0, 10, ns, nodeIds, msgQueues);
			
				
			
			
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
				ns[i].discard();
				
				try {
					Thread.sleep(20);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}
			System.out.println("Discarded.");
			
			environment.discard();
			
			System.out.println("*** Finished ***");
			
		} catch (InitializationException e) {
			e.printStackTrace();
		}
		

	}
	
	
	
	public static void sendAndRecv(int from, int to, NodeService[] ns, NodeId[] nodeIds, LinkedBlockingQueue<ReceivedDataMessage>[] msgQueues, boolean registerRoute, boolean anonymousRoute) {
		try {
			sendTestDataMessage(ns[from], nodeIds[to], "Test string", registerRoute, anonymousRoute);
			Thread.sleep(5);	
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		try {
			msgQueues[to].poll(10000, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
		}
		
	}
	
	
	
	
	
	protected static int msgCounter = 0;
	protected static int msgRecCounter = 0;
	
	
	public static void sendTestDataMessage(NodeService ns, NodeId nodeId, String text, boolean registerRoute, boolean anonymousRoute) {
		
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
		
		byte[] data = new byte[1024];
		data = text.getBytes();
		
		

		DataMessage msg = new DataMessage(nodeId, null, (short)0, (short)0, data);
		try {
			//msi = ns.send(msg, mac, null);
			msi = ns.send(msg, mac, null, HyCubeRoutingManager.createRoutingParameters(false, true, registerRoute, false, null, anonymousRoute));
			System.out.println("Message send info - serial no: " + msi.getSerialNo());
		} catch (NodeServiceException e) {
			e.printStackTrace();
		}
		msgCounter++;

	}
	
	

	
}
