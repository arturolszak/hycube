package net.hycube.test.search;

import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import net.hycube.HyCubeNodeService;
import net.hycube.HyCubeSimpleNodeService;
import net.hycube.HyCubeSimpleSchedulingNodeService;
import net.hycube.NodeService;
import net.hycube.core.HyCubeNodeId;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeId;
import net.hycube.core.NodePointer;
import net.hycube.environment.DirectEnvironment;
import net.hycube.environment.Environment;
import net.hycube.join.JoinWaitCallback;
import net.hycube.messaging.data.ReceivedDataMessage;
import net.hycube.metric.Metric;
import net.hycube.search.SearchWaitCallback;

public class TestSearch {

	
	
	/**
	 * @param args
	 */
	
	public static void main(String[] args) {
		
		String ip = "127.0.0.1";
		//String ip = "192.168.2.110";
		int startPort = 55000;
		//int numNodes = 50;
		int numNodes = 100;
		int recoveryRepeat = 2;
		
		int searchesNum = 5;
		
		int k = 8;
		boolean ignoreTargetNode = false;
		
		int testsNum = 10;
		
		for (int i = 0; i < testsNum; i++) {
			System.out.println("Test #" + i);
			runTest(ip, startPort, numNodes, recoveryRepeat, searchesNum, k, ignoreTargetNode);
			startPort += numNodes;
			
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
		
		System.out.println();
		
	}
	
	
	@SuppressWarnings("unchecked")
	public static void runTest(String ip, int startPort, int numNodes, int recoveryRepeat, int searchsNum, int k, boolean ignoreTargetNode) {
		
		
		Random rand = new Random();
		
		String[] addresses = new String[numNodes];
		for (int i=0; i<numNodes; i++) {
			String address = ip + ":" + (startPort+i);
			addresses[i] = address;
		}

		
		int[] missedNums = new int[numNodes];
		for (int i = 0; i < numNodes; i++) missedNums[i] = 0;
		
		
		try {
			
			int n = addresses.length;
			
			Environment environment = DirectEnvironment.initialize();
			
			HyCubeNodeService[] ns = new HyCubeNodeService[n];

			NodeId[] nodeIds = new NodeId[n];
			LinkedBlockingQueue<ReceivedDataMessage>[] msgQueues = (LinkedBlockingQueue<ReceivedDataMessage>[]) new LinkedBlockingQueue<?>[n];
			
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
				
				//!!! SET APPROPRIATE LINE TO TEST ONE OF THE SERVICES:
				int service = 1;
				
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
			
			
			for (int search = 0; search < searchsNum; search++) {
				System.out.println();
				NodeId searchNodeId = HyCubeNodeId.generateRandomNodeId(4, 32);
				
				for (NodeService serv : ns) {	
					System.out.println("Seach for " + searchNodeId.toHexString() + " from node " + serv.getNode().getNodeId().toHexString());
					NodePointer[] res = search(serv, 0, searchNodeId, k, ignoreTargetNode);
					
					int prevMissed = 0;
					
					if (res != null) {
						int resInd = 0;
						System.out.println("Search results: ");
						for (NodePointer np : res) {
							int closerBefore = 0;
							
							double dist = HyCubeNodeId.calculateDistance((HyCubeNodeId) np.getNodeId(), (HyCubeNodeId)searchNodeId, Metric.EUCLIDEAN);
							for (NodeId id : nodeIds) {
								double nDist = HyCubeNodeId.calculateDistance((HyCubeNodeId) id, (HyCubeNodeId)searchNodeId, Metric.EUCLIDEAN);
								if (nDist < dist) closerBefore++;
							}

							System.out.print("-- result[" + resInd + "] : " + np.getNodeId().toString() + ", " + np.getNetworkNodePointer().getAddressString() + ". ");
							System.out.println("Closer existing: " + closerBefore + ". Closermissed-index-prevMissed: " + (closerBefore - resInd - prevMissed) + ". Closermissed-index: " + (closerBefore - resInd));
							
							missedNums[closerBefore-resInd-prevMissed]++;
							prevMissed = closerBefore-resInd-prevMissed;
							
							resInd++;
						}
					}
					else {
						System.out.println("Search results null");
					}
					
				}
				
				System.out.println();
				
			}
			
				
			
			
			int displayDetailed = 5;
			System.out.println();
			for (int i = 0; i < numNodes && i < displayDetailed; i++) {
				System.out.println(i + " nodes missed in " + missedNums[i] + " cases.");
			}
			int moreNodesMissedCases = 0;
			for (int i = displayDetailed; i < numNodes; i++) {
				moreNodesMissedCases = moreNodesMissedCases + missedNums[i];
			}
			System.out.println("More nodes missed in " + moreNodesMissedCases + " cases.");
			System.out.println();	
			
			
			
			
			
			try {
				Thread.sleep(2000);
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
					Thread.sleep(50);
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


	private static NodePointer[] search(NodeService serv, int i, NodeId searchNodeId, int k, boolean ignoreTargetNode) {
		
		SearchWaitCallback searchCallback = new SearchWaitCallback();
		
		serv.search(searchNodeId, null, (short) k, ignoreTargetNode, searchCallback, null);
		
		NodePointer[] searchResults = null;
		try {
			searchResults = searchCallback.waitForResult(0);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		
		return searchResults;
		
	}
	
	
	

	
	

	
}
