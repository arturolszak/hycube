package net.hycube.test.dht;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import net.hycube.HyCubeMultiQueueMultipleNodeService;
import net.hycube.HyCubeMultipleNodeService;
import net.hycube.HyCubeNodeService;
import net.hycube.HyCubeSchedulingMultipleNodeService;
import net.hycube.core.HyCubeNodeId;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeId;
import net.hycube.core.NodePointer;
import net.hycube.dht.DeleteWaitCallback;
import net.hycube.dht.GetWaitCallback;
import net.hycube.dht.HyCubeResource;
import net.hycube.dht.HyCubeResourceDescriptor;
import net.hycube.dht.PutWaitCallback;
import net.hycube.dht.RefreshPutWaitCallback;
import net.hycube.environment.DirectEnvironment;
import net.hycube.environment.Environment;
import net.hycube.eventprocessing.EventCategory;
import net.hycube.eventprocessing.EventProcessingErrorCallback;
import net.hycube.eventprocessing.EventQueueProcessingInfo;
import net.hycube.eventprocessing.EventType;
import net.hycube.eventprocessing.ThreadPoolInfo;
import net.hycube.join.JoinWaitCallback;
import net.hycube.messaging.data.ReceivedDataMessage;
import net.hycube.search.SearchWaitCallback;
import net.hycube.transport.MessageReceiver;

public class TestDHTWithMultipleNodeService {

	
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
		int startPort = 55000;
		int numNodes = 50;
		//int numNodes = 100;
		int recoveryRepeat = 2;
		
		int k = 8;
		
		int testsNum = 20;
		
		int waitTimeAfterPut = 8000;
		//int waitTimeBeforeGet = 8000;
		int waitTimeBeforeGet = 0;
		boolean refreshResources = false;
		boolean deleteResources = true;
		
		
		boolean separateMRQueue = false;
		
		
		
		runTest(ip, startPort, separateMRQueue, numNodes, recoveryRepeat, testsNum, k, waitTimeAfterPut, waitTimeBeforeGet, refreshResources, deleteResources);
		startPort += numNodes;
			
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.println();
		
	}
	
	
	@SuppressWarnings({ "unchecked" })
	public static void runTest(String ip, int startPort, boolean separateMRQueue, int numNodes, int recoveryRepeat, int testsNum, int k, int waitTimeAfterPut, int waitTimeBeforeGet, boolean refreshResources, boolean deleteResources) {
		
		
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
			HyCubeMultipleNodeService mns;

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
			int service = 1;
			
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
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			
			
			
			
			int[] founds = new int[k+1];
			ArrayList<Object[]> tests = new ArrayList<Object[]>();
			
			
			for (int t = 0; t < testsNum; t++) {
				
				NodeId resNodeId = HyCubeNodeId.generateRandomNodeId(4, 32);
				BigInteger key = resNodeId.getBigInteger();
				
				int pNodeInd = rand.nextInt(ns.length);
				
				int resId = rand.nextInt();
				
				
				HyCubeResourceDescriptor rd = new HyCubeResourceDescriptor(HyCubeResourceDescriptor.OPEN_BRACKET + HyCubeResourceDescriptor.KEY_RESOURCE_ID + HyCubeResourceDescriptor.EQUALS + Integer.toString(resId) + HyCubeResourceDescriptor.CLOSE_BRACKET);
				HyCubeResource res = new HyCubeResource(rd, ("DATA" + Integer.toString(resId).toString()).getBytes());
				
				
				//put
				
				System.out.println("Sending put: ");
				
				NodePointer[] storeNodes = search(ns[pNodeInd], resNodeId, k, false);
				
				tests.add(new Object[] {resNodeId, key, res, pNodeInd, storeNodes});
				
				for (NodePointer np : storeNodes) {
					System.out.print("@");
					put(ns[pNodeInd], np, key, res);
				}
				System.out.println();
				
				
				
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
					
					NodePointer[] deleteNodes = search(ns[pNodeInd], resNodeId, k, false);
					//NodePointer[] deleteNodes = (NodePointer[]) tests.get(t)[4];
					
					for (NodePointer np : deleteNodes) {
						System.out.print("@");
						delete(ns[pNodeInd], np, key, res.getResourceDescriptor());
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
					
					NodePointer[] refreshNodes = search(ns[pNodeInd], resNodeId, k, false);
					//NodePointer[] refreshNodes = (NodePointer[]) tests.get(t)[4];
					
					for (NodePointer np : refreshNodes) {
						System.out.print("@");
						refreshPut(ns[pNodeInd], np, key, res.getResourceDescriptor());
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
				for (NodePointer np : getNodes) {
					System.out.print("@");
					resRetrieved = get(ns[gNodeInd], np, (BigInteger)tests.get(t)[1], ((HyCubeResource)tests.get(t)[2]).getResourceDescriptor());
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
				mns.discardNode(ns[i]);
				try {
					Thread.sleep(100);
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
	
	
	private static boolean put(HyCubeNodeService serv, NodePointer np, BigInteger key, HyCubeResource res) {
		
		PutWaitCallback putCallback = new PutWaitCallback();
		
		serv.put(np, key, res, putCallback, null);
		
		try {
			return (Boolean) putCallback.waitPut();
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
		
	}
	
	private static boolean refreshPut(HyCubeNodeService serv, NodePointer np, BigInteger key, HyCubeResourceDescriptor rd) {
		
		RefreshPutWaitCallback refreshPutCallback = new RefreshPutWaitCallback();
		
		serv.refreshPut(np, key, rd, refreshPutCallback, null);
		
		try {
			return (Boolean) refreshPutCallback.waitRefreshPut();
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
		
	}
	
	private static HyCubeResource[] get(HyCubeNodeService serv, NodePointer np, BigInteger key, HyCubeResourceDescriptor rd) {
		
		GetWaitCallback getCallback = new GetWaitCallback();
		
		serv.get(np, key, rd, getCallback, null);
		
		try {
			return (HyCubeResource[]) getCallback.waitGet();
		} catch (InterruptedException e) {
			e.printStackTrace();
			return null;
		}
		
	}
	
	private static boolean delete(HyCubeNodeService serv, NodePointer np, BigInteger key, HyCubeResourceDescriptor rd) {
		
		DeleteWaitCallback deleteCallback = new DeleteWaitCallback();
		
		serv.delete(np, key, rd, deleteCallback, null);
		
		try {
			return (Boolean) deleteCallback.waitDelete();
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
		
	}

	
	

	
}
