package net.hycube.test.dht;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import net.hycube.HyCubeNodeService;
import net.hycube.HyCubeSimpleNodeService;
import net.hycube.HyCubeSimpleSchedulingNodeService;
import net.hycube.core.HyCubeNodeId;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeId;
import net.hycube.core.NodePointer;
import net.hycube.dht.DeleteWaitCallback;
import net.hycube.dht.GetWaitCallback;
import net.hycube.dht.HyCubeDHTManagerEntryPoint;
import net.hycube.dht.HyCubeResource;
import net.hycube.dht.HyCubeResourceDescriptor;
import net.hycube.dht.HyCubeRoutingDHTManager;
import net.hycube.dht.PutWaitCallback;
import net.hycube.dht.RefreshPutWaitCallback;
import net.hycube.environment.DirectEnvironment;
import net.hycube.environment.Environment;
import net.hycube.join.JoinWaitCallback;
import net.hycube.messaging.data.ReceivedDataMessage;
import net.hycube.search.SearchWaitCallback;

public class TestDHT {

	
	
	/**
	 * @param args
	 */
	
	public static void main(String[] args) {
		
		String ip = "127.0.0.1";
		//String ip = "192.168.2.110";
		int startPort = 55000;
		int numNodes = 200;
		//int numNodes = 100;
		int recoveryRepeat = 2;
		
		int k = 8;
		
		int testsNum = 50;
		
		int waitTimeAfterPut = 16000;
		int waitTimeBeforeGet = 0;
		boolean refreshResources = false;
		boolean deleteResources = false;
		
		boolean exactPut = false;	
		boolean exactRefreshPut = true;
		
		boolean exactGet = true;
		//boolean exactGet = false;
		
		boolean exactDelete = false;
		
		int skipPutNodesNum = 1;
		//int skipPutNodesNum = 1;
		
		
		boolean getFromClosest = true;
		//boolean getFromClosest = true;
		
		
		boolean setPutRecipient = false;		// if false -> recipient = null and only one message will be routed, exact set must be false
		boolean setGetRecipient = true;	// if false -> recipient = null and only one message will be routed, exact get must be false
		
		boolean secureRouting = false;
		
		boolean skipRandomNextHops = false;
		
		
		boolean registerRoute = true;
		
		boolean anonymousRoute = true;
		
		
		int replicateNum = 4;
		
		
		
		runTest(ip, startPort, numNodes, recoveryRepeat, testsNum, k, waitTimeAfterPut, waitTimeBeforeGet, refreshResources, deleteResources, exactPut, exactRefreshPut, exactGet, exactDelete, getFromClosest, skipPutNodesNum, setPutRecipient, setGetRecipient, secureRouting, skipRandomNextHops, registerRoute, anonymousRoute, replicateNum);
		startPort += numNodes;
			
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.println();
		
	}
	
	
	@SuppressWarnings({ "unchecked", "unused" })
	public static void runTest(String ip, int startPort, int numNodes, int recoveryRepeat, int testsNum, int k, int waitTimeAfterPut, int waitTimeBeforeGet, boolean refreshResources, boolean deleteResources, boolean exactPut, boolean exactRefreshPut, boolean exactGet, boolean exactDelete, boolean getFromClosest, int skipPutNodesNum, boolean setPutRecipient, boolean setGetRecipient, boolean secureRouting, boolean skipRandomNextHops, boolean registerRoute, boolean anonymousRoute, int replicateNum) {
		
		
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
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
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
						Thread.sleep(50);
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
			
			
			
			
			
			int[] founds = new int[k+1];
			ArrayList<Object[]> tests = new ArrayList<Object[]>();
			
			
			for (int t = 0; t < testsNum; t++) {
				
				NodeId resNodeId = HyCubeNodeId.generateRandomNodeId(4, 32);
				BigInteger key = resNodeId.getBigInteger();
				
				
				int replicasNum = 0;
				System.out.print("Checking replicas num: ");
				int ii=0;
				for (HyCubeNodeService serv : ns) {
					System.out.print(ii + ", ");
					ii++;
					boolean isReplica = (Boolean) ((HyCubeDHTManagerEntryPoint)(serv.getNode().getDHTManagerEntryPoint())).isReplica(key, serv.getNode().getNodeId(), k);
					if (isReplica) replicasNum++;
				}
				System.out.println();
				System.out.println("Replicas number: " + replicasNum);
				
				
				int pNodeInd = rand.nextInt(ns.length);
				
				int resId = rand.nextInt();
				
				
				HyCubeResourceDescriptor rd = new HyCubeResourceDescriptor(HyCubeResourceDescriptor.OPEN_BRACKET + HyCubeResourceDescriptor.KEY_RESOURCE_ID + HyCubeResourceDescriptor.EQUALS + Integer.toString(resId) + HyCubeResourceDescriptor.CLOSE_BRACKET);
				String resourceUrl = Integer.toString(pNodeInd) + "::" + rd.getResourceId();
				rd.setResourceUrl(resourceUrl);
				HyCubeResource res = new HyCubeResource(rd, ("DATA" + Integer.toString(resId).toString()).getBytes());
				
				
				
				
				//put
				
				System.out.println("Sending put: ");
				
				NodePointer[] storeNodes = search(ns[pNodeInd], resNodeId, k, false);
				
				if (skipPutNodesNum > 0) {
					skipPutNodesNum = Math.min(skipPutNodesNum, storeNodes.length);
					storeNodes = Arrays.copyOfRange(storeNodes, skipPutNodesNum, storeNodes.length);
				}
				
				tests.add(new Object[] {resNodeId, key, res, pNodeInd, storeNodes});
				
				if (setPutRecipient) {
					for (NodePointer np : storeNodes) {
						System.out.print("@");
						put(ns[pNodeInd], np, key, res, exactPut, secureRouting, skipRandomNextHops, registerRoute, anonymousRoute);
					}
				}
				else {
					System.out.print("@");
					put(ns[pNodeInd], null, key, res, exactPut, secureRouting, skipRandomNextHops, registerRoute, anonymousRoute);
				}
				
				System.out.println();
				
				
				
			}
							
			

			
			if (replicateNum > 0) {
				for (int i = 0; i < replicateNum; i++) {
					System.out.println("Replicating...: (" + (i+1) + ")");
					for (HyCubeNodeService serv : ns) {
						serv.getNode().getBackgroundProcessEntryPoint("HyCubeDHTReplicationBackgroundProcess").processOnce();
					}
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}	
			}
			
			
			
			//wait after put:
			
			try {
				Thread.sleep(waitTimeAfterPut);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			
			
			
			
			
			if (deleteResources) {
				
				for (int t = 0; t < testsNum; t++) {
					
					System.out.println("Sending delete: ");
					
					NodeId resNodeId = (NodeId) tests.get(t)[0];
					BigInteger key = (BigInteger) tests.get(t)[1];
					HyCubeResource res = (HyCubeResource) tests.get(t)[2];
					int pNodeInd = (Integer) tests.get(t)[3];
					
					//NodePointer[] deleteNodes = search(ns[pNodeInd], resNodeId, k, false);
					NodePointer[] deleteNodes = (NodePointer[]) tests.get(t)[4];
					
					for (NodePointer np : deleteNodes) {
						System.out.print("@");
						delete(ns[pNodeInd], np, key, res.getResourceDescriptor(), exactPut, secureRouting, skipRandomNextHops, registerRoute, anonymousRoute);
					}
					
					System.out.println();
					
				}
			}
			
			
			if (refreshResources) {
				
		
				for (int t = 0; t < testsNum; t++) {
					
					System.out.println("Sending refresh: ");
					
					NodeId resNodeId = (NodeId) tests.get(t)[0];
					BigInteger key = (BigInteger) tests.get(t)[1];
					HyCubeResource res = (HyCubeResource) tests.get(t)[2];
					int pNodeInd = (Integer) tests.get(t)[3];
					
					//NodePointer[] refreshNodes = search(ns[pNodeInd], resNodeId, k, false);
					NodePointer[] refreshNodes = (NodePointer[]) tests.get(t)[4];
					
					for (NodePointer np : refreshNodes) {
						System.out.print("@");
						refreshPut(ns[pNodeInd], np, key, res.getResourceDescriptor(), exactRefreshPut, secureRouting, skipRandomNextHops, registerRoute, anonymousRoute);
					}
					
					System.out.println();
					
				}
				
				
				
			}
	
			
			
				
				
			
			
				
			
			//wait before get:
			
			try {
				Thread.sleep(waitTimeBeforeGet);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			
			
			for (int t = 0; t < testsNum; t++) {
				
				
				int gNodeInd = rand.nextInt(ns.length);
				
				//get
				
				System.out.println("Sending gets ");
				
				NodePointer[] getNodes = search(ns[gNodeInd], (NodeId) tests.get(t)[0], k, false);
				
				HyCubeResource[] resRetrieved = null;
				int found = 0;
				if (setGetRecipient) {
					for (NodePointer np : getNodes) {
						System.out.print("@");
						resRetrieved = get(ns[gNodeInd], np, (BigInteger)tests.get(t)[1], ((HyCubeResource)tests.get(t)[2]).getResourceDescriptor(), exactGet, getFromClosest, secureRouting, skipRandomNextHops, registerRoute, anonymousRoute);
						if (resRetrieved.length > 0) {
							found++;
						}
					}
				}
				else {
					System.out.print("@");
					resRetrieved = get(ns[gNodeInd], null, (BigInteger)tests.get(t)[1], ((HyCubeResource)tests.get(t)[2]).getResourceDescriptor(), exactGet, getFromClosest, secureRouting, skipRandomNextHops, registerRoute, anonymousRoute);
					if (resRetrieved.length > 0) {
						found++;
					}
				}
				System.out.println();
				
				founds[found]++;
				
				

				
				System.out.println();
				
			}
			
			System.out.println();
			System.out.println("K = " + k);
			for (int i = 0; i < founds.length; i++) {
				System.out.println(i +" : " + founds[i]);
			}

			
			
			
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


	private static NodePointer[] search(HyCubeNodeService serv, NodeId searchNodeId, int k, boolean ignoreTargetNode) {
		
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
	
	
	
	private static boolean put(HyCubeNodeService serv, NodePointer np, BigInteger key, HyCubeResource res, boolean exactPut, boolean secureRouting, boolean skipRandomNextHops, boolean registerRoute, boolean anonymousRoute) {
		
		PutWaitCallback putCallback = new PutWaitCallback();
		
		serv.put(np, key, res, putCallback, null, HyCubeRoutingDHTManager.createPutParameters(exactPut, secureRouting, skipRandomNextHops, registerRoute, anonymousRoute));
		
		try {
			return (Boolean) putCallback.waitPut();
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
		
	}
	
	private static boolean refreshPut(HyCubeNodeService serv, NodePointer np, BigInteger key, HyCubeResourceDescriptor rd, boolean exactRefreshPut, boolean secureRouting, boolean skipRandomNextHops, boolean registerRoute, boolean anonymousRoute) {
	
		RefreshPutWaitCallback refreshPutCallback = new RefreshPutWaitCallback();
		
		serv.refreshPut(np, key, rd, refreshPutCallback, null, HyCubeRoutingDHTManager.createRefreshParameters(exactRefreshPut, secureRouting, skipRandomNextHops, registerRoute, anonymousRoute));
		
		try {
			return (Boolean) refreshPutCallback.waitRefreshPut();
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
		
	}
	
	
	private static HyCubeResource[] get(HyCubeNodeService serv, NodePointer np, BigInteger key, HyCubeResourceDescriptor rd, boolean exactGet, boolean getFromClosest, boolean secureRouting, boolean skipRandomNextHops, boolean registerRoute, boolean anonymousRoute) {
		
		GetWaitCallback getCallback = new GetWaitCallback();
		
		serv.get(np, key, rd, getCallback, null, HyCubeRoutingDHTManager.createGetParameters(exactGet, getFromClosest, secureRouting, skipRandomNextHops, registerRoute, anonymousRoute));
		
		try {
			return (HyCubeResource[]) getCallback.waitGet();
		} catch (InterruptedException e) {
			e.printStackTrace();
			return null;
		}
		
	}
	
	private static boolean delete(HyCubeNodeService serv, NodePointer np, BigInteger key, HyCubeResourceDescriptor rd, boolean exactDelete, boolean secureRouting, boolean skipRandomNextHops, boolean registerRoute, boolean anonymousRoute) {
		
		DeleteWaitCallback deleteCallback = new DeleteWaitCallback();
		
		serv.delete(np, key, rd, deleteCallback, null, HyCubeRoutingDHTManager.createDeleteParameters(exactDelete, secureRouting, skipRandomNextHops, registerRoute, anonymousRoute));
		
		try {
			return (Boolean) deleteCallback.waitDelete();
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
		
	}

	
	

	
}
