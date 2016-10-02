package net.hycube.test.maintenance;

import java.util.concurrent.LinkedBlockingQueue;

import net.hycube.HyCubeNodeService;
import net.hycube.HyCubeSimpleNodeService;
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

public class TestJoin {

	
	
	/**
	 * @param args
	 */
	
	public static void main(String[] args) {
		
		String ip = "127.0.0.1";
		int startPort = 56000;
		int numNodes = 50;
		
		int testsNum = 10;
		
		for (int i = 0; i < testsNum; i++) {
			System.out.println("Test #" + i);
			runTest(ip, startPort, numNodes);
			startPort += numNodes;
			
		}
		
		
	}
	
	
	@SuppressWarnings("unchecked")
	public static void runTest(String ip, int startPort, int numNodes) {
		
		
		
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
			
			System.out.print("Join: " );
			for (int i = 0; i < n; i++) {
				
				JoinWaitCallback joinWaitCallback = new JoinWaitCallback();
				String bootstrap = null;
				if (i >= 1) bootstrap = addresses[i-1];
				ns[i] = HyCubeSimpleNodeService.initialize(environment, HyCubeNodeId.generateRandomNodeId(4, 32), addresses[i], bootstrap, joinWaitCallback, null, 0, true, null, null);
				nodeIds[i] = ns[i].getNode().getNodeId();
				msgQueues[i] = ns[i].registerPort((short)0);

				System.out.print(i);
				System.out.flush();
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
			
			
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			
//			for (int from = 0; from < 30; from++) {
//				for (int to = 0; to < 30; to++) {
//					System.out.println("From: " + from + " to: " + to);
//					sendAndRecv(from, to, ns, nodeIds, msgQueues);
//				}
//			}
				
			
			sendAndRecv(0, 10, ns, nodeIds, msgQueues);
			
				
			
			

			System.out.print("Discarding: ");
			System.out.flush();
			for (int i = 0; i < n; i++) {
				System.out.print(i + ",");
				System.out.flush();
				ns[i].discard();
			}
			System.out.println("Discarded.");
			
			environment.discard();
			
			System.out.println("*** Finished ***");
			
		} catch (InitializationException e) {
			e.printStackTrace();
		}
		

	}
	
	
	
	public static void sendAndRecv(int from, int to, HyCubeNodeService[] ns, NodeId[] nodeIds, LinkedBlockingQueue<ReceivedDataMessage>[] msgQueues) {
		try {
			sendTestDataMessage(ns[from], nodeIds[to], "Test string");
			Thread.sleep(200);	
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		ReceivedDataMessage recMsg = null;
		try {
			recMsg = msgQueues[to].take();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Success! Message received!: " + new String(recMsg.getData()));
		
		
		try {
			Thread.sleep(200);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	
	protected static int msgCounter = 0;
	
	public static void sendTestDataMessage(NodeService ns, NodeId nodeId, String text) {
		
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
