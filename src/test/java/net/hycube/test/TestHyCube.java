package net.hycube.test;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import net.hycube.HyCubeNodeService;
import net.hycube.HyCubeSimpleNodeService;
import net.hycube.core.HyCubeNodeId;
import net.hycube.core.NodeId;
import net.hycube.environment.DirectEnvironment;
import net.hycube.environment.Environment;
import net.hycube.join.JoinWaitCallback;
import net.hycube.messaging.data.ReceivedDataMessage;

public class TestHyCube {

	public static String USAGE = "Usage: " + TestHyCube.class.getSimpleName() + " ipAddress startPort numNodes recoveryRepeatNumber";
	
	public static Environment environment;
	
	public static HyCubeNodeService[] ns;
	public static NodeId[] nodeIds;
	public static LinkedBlockingQueue<ReceivedDataMessage>[] msgQueues;
	
	
	/**
	 * @param args
	 */
	
	public static void main(String[] args) {
		
		if (args.length != 4) {
			System.out.println(USAGE);
			System.out.println();
			return;
		}
		
		String ip = args[0];
		int startPort = Integer.parseInt(args[1]);
		int numNodes = Integer.parseInt(args[2]);
		int recoveryRepeat = Integer.parseInt(args[3]);
		
		
		System.out.println("Creating a DHT of " + numNodes + " nodes, ip=" + ip + ", startPort=" + startPort + ", receoveryRepeat=" + recoveryRepeat);
		
		createDHT(ip, startPort, numNodes, recoveryRepeat);
		
		
		
		System.out.println();
		System.out.println("DHT created:");
		
		
		displayDHT();
		
		
		System.out.println("Press enter to discard the DHT.");
		
		
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		
		try {
			System.in.read();
		} catch (IOException e) {

		}
		
		
		
		destroyDHT();
		
		
		
	}
	
	
	@SuppressWarnings("unchecked")
	public static void createDHT(String ip, int startPort, int numNodes, int recoveryRepeat) {
		
		Random rand = new Random();
		
		String[] addresses = new String[numNodes];
		for (int i=0; i<numNodes; i++) {
			String address = ip + ":" + (startPort+i);
			addresses[i] = address;
		}

		
		try {
			
			int n = addresses.length;
			
			environment = DirectEnvironment.initialize();
			
			ns = new HyCubeNodeService[n];
			nodeIds = new NodeId[n];
			msgQueues = (LinkedBlockingQueue<ReceivedDataMessage>[]) new LinkedBlockingQueue<?>[n];
			
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
				
				ns[i] = HyCubeSimpleNodeService.initialize(environment, HyCubeNodeId.generateRandomNodeId(4, 32).toString(), addresses[i], bootstrap, joinWaitCallback, null, 0, true, null, null);
				nodeIds[i] = ns[i].getNode().getNodeId();
				msgQueues[i] = ns[i].registerPort((short)0);

				joinWaitCallback.waitJoin();
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
			
			
			
			
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}
		
	protected static void displayDHT() {
		
		System.out.println();
		for (int i = 0; i < ns.length; i++) {
			System.out.println("Node " + i +": " + ns[i].getNode().getNodeId().toHexString() + ", " + ns[i].getNode().getNetworkAdapter().getPublicAddressString());
		}
		System.out.println();
		
		
	}


	public static void destroyDHT() {
			
		System.out.print("Discarding: ");
		System.out.flush();
		for (int i = 0; i < ns.length; i++) {
			System.out.print(i + ",");
			System.out.flush();
			ns[i].discard();
		}
		System.out.println("Discarded.");
		
		environment.discard();
		
		System.out.println("*** Finished ***");
			
		

	}
	
	
	

}
