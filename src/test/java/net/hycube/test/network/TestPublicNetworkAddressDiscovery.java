package net.hycube.test.network;

import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

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

public class TestPublicNetworkAddressDiscovery {

	
	
	/**
	 * @param args
	 */
	
	public static void main(String[] args) {
		
		String pubIp = "192.168.1.35";
		String privIp = "192.168.2.147";
		
		int pubStartPort = 56000;
		int privStartPort = 57000;
		
		int numNodes = 100;
		
		int testsNum = 1;
		
		int recoveryRepeat = 2;
		
		for (int i = 0; i < testsNum; i++) {
			System.out.println("Test #" + i);
			runTest(pubIp, privIp, pubStartPort, privStartPort, numNodes, recoveryRepeat);
			pubStartPort += numNodes;
			privStartPort += numNodes;
			
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
	public static void runTest(String pubIp, String privIp, int pubStartPort, int privStartPort, int numNodes, int recoveryRepeat) {
		
		Random rand = new Random();
		
		String[] addresses = new String[numNodes];
		for (int i=0; i<numNodes; i++) {
			String address = (i%2 == 0 ? pubIp : privIp) + ":" + ((i%2 == 0 ? pubStartPort : privStartPort)+i);
			addresses[i] = address;
		}

		
		try {
			
			int n = addresses.length;
			
			Environment environment = DirectEnvironment.initialize();
			
			HyCubeNodeService[] ns = new HyCubeNodeService[n];

			NodeId[] nodeIds = new NodeId[n];
			LinkedBlockingQueue<ReceivedDataMessage>[] msgQueues = (LinkedBlockingQueue<ReceivedDataMessage>[]) new LinkedBlockingQueue<?>[n];
			
			System.out.println("Join: " );
			for (int i = 0; i < n; i++) {
				
				System.out.println(i);
				System.out.flush();
				
				String addressBeforeJoin = addresses[i];
				System.out.println("Address before join: " + addressBeforeJoin);
				
				JoinWaitCallback joinWaitCallback = new JoinWaitCallback();
				String bootstrap = null;
				
				if (i >= 1) {
					int bootstrapIndex = rand.nextInt(i);
					
					if (i % 2 == 1) {
						//connect only to nodes that are not in the same subnetwork (otherwise the router would not translate the address and send it directly to the recipient node (with original private "from" address))
						bootstrapIndex = bootstrapIndex - (bootstrapIndex % 2);
					}
					
					bootstrap = addresses[bootstrapIndex];
					System.out.println("Bootstrap: " + bootstrap);
				}
				
				NodeId nodeId = HyCubeNodeId.generateRandomNodeId(4, 32);
				
				//!!! SET APPROPRIATE LINE TO TEST ONE OF THE SERVICES:
				int service = 2;
				long before = System.currentTimeMillis();
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
				
				long after = System.currentTimeMillis();
				System.out.println("JOIN time: " + (after - before));
				
				addresses[i] = ns[i].getNode().getNodePointer().getNetworkNodePointer().getAddressString();
				System.out.println("Address after join: " + addresses[i]);
				
				if (! addressBeforeJoin.equals(addresses[i])) {
					System.out.println("!!! Address modified during JOIN !!!");
				}
				
				
				System.out.println("#");
				System.out.flush();
			}
			System.out.println();
						
				
			
			
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
	
	
	
	public static void sendAndRecv(int from, int to, NodeService[] ns, NodeId[] nodeIds, LinkedBlockingQueue<ReceivedDataMessage>[] msgQueues) {
		try {
			sendTestDataMessage(ns[from], nodeIds[to], "Test string");
			Thread.sleep(5);	
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
