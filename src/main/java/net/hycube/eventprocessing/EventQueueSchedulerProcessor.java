package net.hycube.eventprocessing;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.UnrecoverableRuntimeException;
import net.hycube.environment.Environment;
import net.hycube.logging.LogHelper;
import net.hycube.utils.HashMapUtils;

public class EventQueueSchedulerProcessor implements EventScheduler, EventQueueProcessor {

	public static int WAKEABLE_SLEEP_TIMEOUT = 1000;

	public static boolean SCHEDULE_AT_FIXED_TIME_INTERVAL = true;
	public static int SCHEDULE_TIME_INTERVAL = 20;	//ms
	//public static int SCHEDULE_TIME_INTERVAL = 40;	//ms

	
	
	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(EventQueueSchedulerProcessor.class); 

	
	
	public class EventQueueProcessorRunnable implements Runnable, WakeableManager, EventScheduler {

		
			
		protected BlockingQueue<Event> queue;
		protected int queueIndex;
		
		protected LinkedList<Wakeable> wakeables;
		protected HashMap<Wakeable, Integer> nonWakeablesAfter;
		protected Lock lock;
		protected int availableProcessingResources;
		
		protected LinkedList<ScheduledEvent> scheduledEvents;
		protected LinkedList<Thread> threadsSleepingSorted;
		protected HashMap<Thread, Long> threadWakeupTimeMap;
		
		

		public EventQueueProcessorRunnable(BlockingQueue<Event> queue, int queueIndex, int availableProcessingResources) {
			this(queue, queueIndex, null, availableProcessingResources);
		}
		
		public EventQueueProcessorRunnable(BlockingQueue<Event> queue, int queueIndex, Lock wakeableManagerLock, int availableProcessingResources) {
			
			if (devLog.isDebugEnabled()) {
				devLog.debug("Creating the event queue processor runnable object.");
			}
			
			this.queue = queue;
			this.queueIndex = queueIndex;
			
			if (devLog.isDebugEnabled()) {
				devLog.debug("Creating wakeable manager with " + availableProcessingResources + " available slots. The lock object is: " + wakeableManagerLock);
			}
			
			this.wakeables = new LinkedList<Wakeable>();
			this.nonWakeablesAfter = new HashMap<Wakeable, Integer>();
			this.availableProcessingResources = availableProcessingResources;
			
			this.lock = new ReentrantLock(true);
			
			scheduledEvents = new LinkedList<ScheduledEvent>();
			
			threadsSleepingSorted = new LinkedList<Thread>();
			threadWakeupTimeMap = new HashMap<Thread, Long>();
			
			
		}
		
		public BlockingQueue<Event> getQueue() {
			return this.queue;
		}
		
		
		@Override
		public void run() {
			if (devLog.isDebugEnabled()) {
				devLog.debug("Processing the queue, index: " + queueIndex);
			}
			try {
				if (running) {
					
					if (error) {
						if (devLog.isDebugEnabled()) {
							devLog.debug("An error occured in a different processing thread. Not processing any more queue events. Queue index: " + queueIndex);
						}
						return;
					}
					
					while (paused) {
						try {
							Thread.sleep(1000);
						}
						catch (InterruptedException e) {
							if (devLog.isDebugEnabled()) {
								devLog.debug("Interrupted on Thread.sleep while in paused state, queue index: " + queueIndex);
							}
						}
					}
					

					//start processing

					//take the event from the queue - this is a blocking call and is called before submitting next runnable to the thread pool, so only one thread will be waiting on that queue at a time
					if (devLog.isDebugEnabled()) {
						devLog.debug("Taking the event from the event queue, queue index: " + queueIndex);
					}


					processScheduler();

					Event event = null;
					
					try {
						//lock.lock();
						long nextMaxSleepTime = getNextMaxSleepTime();
						event = queue.poll(nextMaxSleepTime, TimeUnit.MILLISECONDS);

					} 
					catch (InterruptedException e) {
						if (devLog.isDebugEnabled()) {
							devLog.debug("Interrupted on queue.poll(), queue index: " + queueIndex);
						}
					}
					finally {
						//lock.unlock();
					}

					//to optimize future sleep times, mark that this thread already woke up (possibly before the timeout):
					removeCurrThreadSleepInfo();

					if (running) {

						processScheduler();	//check the scheduler events again, to ensure that processing of the event (possible wakeable) will not block the scheduler events to be scheduled
	
	
						//submit new task to the thread pool - this will make the new thread to wait on the queue for the next event to come
						if (devLog.isDebugEnabled()) {
							devLog.debug("Submitting new task (waiting on the queue for new events) to the thread pool, queue index: " + queueIndex);
						}
						try {
							threadPools[queueIndex].submit(this);
						}
						catch (RejectedExecutionException e) {
							return;
						}
	
						
						if (event != null) {
							//process the event	
							if (devLog.isDebugEnabled()) {
								devLog.debug("Processing the event..., queue index: " + queueIndex);
							}
							
							event.process();
							
						}
	
						//to optimize future sleep times, mark that this thread already woke up (possibly was a sleeping wakeable and woke up before the timeout):
						removeCurrThreadSleepInfo();
						
					}						
					
				}
			
			} 
			catch (Throwable e) {
	    		if (devLog.isFatalEnabled()) {
	    			devLog.fatal("An exception thrown while processing an event. The thread will be terminated.", e);
	    		}
	    		if (userLog.isFatalEnabled()) {
	    			userLog.fatal("An error occured while processing an event. The thread will be terminated.");
	    		}
	    		
	    		synchronized (errorLock) {
	    			if (error == false) {
	    				error = true;
	    				
	    				//stop processing the queues, in a separate thread to avoid deadlock when waiting for this thread to finish
	    				ExecutorService errorExecutor = Executors.newSingleThreadExecutor();
	    				errorExecutor.submit(new Runnable() {
		    					public void run() {
		    						synchronized(EventQueueSchedulerProcessor.this) {
			    						stop();
			    						//if specified, execute the error callback
			    						errorCallback.errorOccurred(errorCallbackArg);
		    						}
		    					}
	    					});

	    				errorExecutor.shutdown();
	    			
	    			}
	    		}
	    		
	    	}
			
			if (devLog.isDebugEnabled()) {
				devLog.debug("Processing the queue finished, index: " + queueIndex);
			}
			
		}
		
		
		protected void removeCurrThreadSleepInfo() {
			//to optimize future sleep times, mark that this thread already woke up (possibly before the timeout):
			lock.lock();
			Thread currThread = Thread.currentThread();
			try {
				threadsSleepingSorted.remove(currThread);
				threadWakeupTimeMap.remove(currThread);
			}
			finally {
				lock.unlock();
			}
		}

		
		
		@Override
		public Lock getWakeableManagerLock() {
			return lock;
		}

		@Override
		public boolean addWakeable(Wakeable wakeable) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("Registering Wakeable object: " + wakeable.toString());
			}
			
			lock.lock();
			try {
				if (!this.wakeables.contains(wakeable)) {
					this.wakeables.add(wakeable);
					this.nonWakeablesAfter.put(wakeable, Integer.valueOf(0));
					return true;
				}
				else return false;
			}
			finally {
				lock.unlock();
			}
		}

		@Override
		public boolean removeWakeable(Wakeable wakeable) {
			if (devLog.isDebugEnabled()) {
				devLog.debug("Unregistering Wakeable object: " + wakeable.toString());
			}
			
			lock.lock();
			try {
				this.nonWakeablesAfter.remove(wakeable);
				return this.wakeables.remove(wakeable);
			}
			finally {
				lock.unlock();
			}
		}
		
		protected List<Wakeable> getWakeables() {
			lock.lock();
			try {
				List<Wakeable> copyList = new LinkedList<Wakeable>(wakeables);
				return copyList;
			}
			finally {
				lock.unlock();
			}
		}

		@Override
		public void discard() {
			if (devLog.isDebugEnabled()) {
				devLog.debug("Discarding - unregistering all Wakeable objects...");
			}
			
			lock.lock();
			try {
				this.nonWakeablesAfter.clear();
				this.wakeables.clear();
			}
			finally {
				lock.unlock();
			}
						
		}


		@Override
		public void wakeup() {
			
			if (devLog.isDebugEnabled()) {
				devLog.debug("Wakeup called.");
			}
			
			lock.lock();
			try {
				if (!wakeables.isEmpty()) {
					int nonWakeablesAfterLastWakeable = nonWakeablesAfter.get(wakeables.getLast());
					nonWakeablesAfterLastWakeable++;
					nonWakeablesAfter.put(wakeables.getLast(), nonWakeablesAfterLastWakeable);
					
					int numWakeables = wakeables.size();
					int numNonWakeables = 0;
					for (int nonWakeableAfter : nonWakeablesAfter.values()) {
						numNonWakeables = numNonWakeables + nonWakeableAfter;
					}
					
					if (devLog.isDebugEnabled()) {
						devLog.debug("numWakeables: " + numWakeables + ", numNonWakeables: " + numNonWakeables + ", availableSlots: " + availableProcessingResources);
					}
					
					while (numWakeables + numNonWakeables > availableProcessingResources && numWakeables > 0) {
						
						Wakeable firstWakeable = wakeables.getFirst();
						
						if (devLog.isDebugEnabled()) {
							devLog.debug("Waking the first enqueued Wakeable: " + firstWakeable.toString());
						}
						
						numWakeables--;
						numNonWakeables = numNonWakeables - nonWakeablesAfter.get(firstWakeable);
						firstWakeable.wakeup();
						wakeables.remove(firstWakeable);
						nonWakeablesAfter.remove(firstWakeable);
						
						if (devLog.isDebugEnabled()) {
							devLog.debug("numWakeables: " + numWakeables + ", numNonWakeables: " + numNonWakeables + ", availableSlots: " + availableProcessingResources);
						}
						
					}
					
				}
				else {
					//no Wakeable to wake up, do nothing
					if (devLog.isTraceEnabled()) {
						devLog.debug("No Wakeable to wake up, do nothing.");
					}
				}
			}
			finally {
				lock.unlock();
			}

		}

		/**
		 * The time returned ensures that, for every scheduled event \emph{e} within $k$ first scheduled events ($k = availableSlots$),
		 * the number of threads that would be available for scheduled execution (not sleeping/blocking at the scheduled execution time)
		 * is not less than the number of events scheduled, up to the event \emph{e}.
		 * That would ensure that $k$ scheduled events can be executed in parallel (by the maximum number of available threads).
		 */
		@Override
		public int getNextMaxSleepTime() {
			
			long wakeupTime = 0;
			long sleepTime;
			
			lock.lock();
			try {
			
				purgeWakeupTimes();
				
				devLog.debug("Calculating next wakeable sleep time. Scheduled events: " + scheduledEvents + ", sleeping threads: " + threadsSleepingSorted);
				
				
				ListIterator<ScheduledEvent> schedIter = scheduledEvents.listIterator();
				ListIterator<Thread> threadsSleepingSortedIter = threadsSleepingSorted.listIterator();
				
				ScheduledEvent nextSched;
				long nextWakeupTime;
				

				
				int wakedupBefore = 0;
				
				boolean maxTime = false;
				
				for (int i = 1; i <= availableProcessingResources; i++) {
					
					if (!schedIter.hasNext()) {
						//means that this thread will not have to wake up for any scheduled task to be enqueued 
						maxTime = true;
						break;
					}
					//else:
					
					nextSched = schedIter.next();
					
					if (threadsSleepingSortedIter.hasNext()) {
						nextWakeupTime = threadWakeupTimeMap.get(threadsSleepingSortedIter.next());
						while (nextWakeupTime < nextSched.getExecutionTime()) {
							wakedupBefore++;
							if (threadsSleepingSortedIter.hasNext()) {
								nextWakeupTime = threadWakeupTimeMap.get(threadsSleepingSortedIter.next());
							}
							else break;
						}
						threadsSleepingSortedIter.previous();	//so we check the previous elemens once again for the next scheduled task
					}
					
					int procResourcesToServeTasks = availableProcessingResources - 1 - threadsSleepingSorted.size() + wakedupBefore;	//-1: this thread
					if (procResourcesToServeTasks < i) {
						wakeupTime = nextSched.getExecutionTime();
						break;
						//will break latest when i=availableProcessingResources (availableProcessingResources resources will never be able to serve more events simultanously)
					}
					
				}
				if (maxTime) {
					sleepTime = WAKEABLE_SLEEP_TIMEOUT;
				}
				else {
					long currTime = environment.getTimeProvider().getCurrentTime(); 
					sleepTime = wakeupTime - currTime;
				}
				
				if (sleepTime < 0) sleepTime = 0;
				if (sleepTime > WAKEABLE_SLEEP_TIMEOUT) sleepTime = WAKEABLE_SLEEP_TIMEOUT;
				
			
			
				//add estimated wakeup time to the wakeupTimes list (ordered):
				ListIterator<Thread> iter = threadsSleepingSorted.listIterator();
				int ind = 0;
				while (iter.hasNext()) {
					long next = threadWakeupTimeMap.get(iter.next());
					if (wakeupTime <= next) break;
					else ind++;
				}
				Thread currrentThread = Thread.currentThread();
				threadWakeupTimeMap.put(currrentThread, wakeupTime);
				threadsSleepingSorted.add(ind, currrentThread);

			}
			finally {
				lock.unlock();
			}

			devLog.debug("Next wakeable sleep time: " + sleepTime);
			
			//return the sleep time
			return (int)sleepTime;
		}
		
		protected void purgeWakeupTimes() {
			long currTime = environment.getTimeProvider().getCurrentTime();
			if (! threadsSleepingSorted.isEmpty()) {
				Thread t = threadsSleepingSorted.getFirst();
				while (threadWakeupTimeMap.get(t) <= currTime) {
					
					threadsSleepingSorted.removeFirst();
					threadWakeupTimeMap.remove(t);
					if (! threadsSleepingSorted.isEmpty()) {
						t = threadsSleepingSorted.getFirst();
					}
					else break;
				}
			}
			
		}


		@Override
		public void scheduleEvent(ScheduledEvent scheduledEvent) {
			
			if (SCHEDULE_AT_FIXED_TIME_INTERVAL) {
				long executionTime = scheduledEvent.getExecutionTime();
				if ((executionTime % SCHEDULE_TIME_INTERVAL) != 0) {
					executionTime = executionTime + SCHEDULE_TIME_INTERVAL - (executionTime % SCHEDULE_TIME_INTERVAL);
				}
				scheduledEvent.setExecutionTime(executionTime);
			}
			
			
			lock.lock();
			try {
				
//				//binary find (logarithmic time), insert as the latest with this execution time (scheduledEvents is ordered)
//				
//				int lowInd = 0;
//				int highInd = scheduledEvents.size();
//				
//				int ind = highInd;
//				
//				while (lowInd != highInd) {
//					ind = lowInd + (highInd - lowInd) / 2;
//					if (scheduledEvent.getExecutionTime() < scheduledEvents.get(ind).getExecutionTime()) {
//						highInd = ind;
//					}
//					else {//if (scheduledEvent.getExecutionTime() >= scheduledEvents.get(ind).getExecutionTime()) {
//						lowInd = ind + 1;
//					}
//				}
//				ind = lowInd;
				
				
				//add the scheduled event to the scheduledEvents list (ordered):
				ListIterator<ScheduledEvent> iter = scheduledEvents.listIterator();
				int ind = 0;
				while (iter.hasNext()) {
					ScheduledEvent next = iter.next();
					if (scheduledEvent.getExecutionTime() < next.getExecutionTime()) break;
					else ind++;
				}


				
				scheduledEvents.add(ind, scheduledEvent);
				
				

				
				
				
				//if the queue is empty and the newly scheduled event execution time is earlier than the wakeup time of the poll operation, 
				//or if the wakeable event is going to wakeup after the scheduled event execution time,
				//it may delay the scheduled event
				//-> so insert a dummy event to return from the poll call and perform a wakeup,
				//this will make sure that the dummy event will be processed, after which, the scheduler will be processed and subsequent calls to getNextSleepTime will take new scheduled event into consideration
								
				if (!threadsSleepingSorted.isEmpty()) {
					if (threadWakeupTimeMap.get(threadsSleepingSorted.getFirst()) > scheduledEvent.getExecutionTime()) {

						//put dummy event to the queue
						try {
							queue.put(new DummyEvent());
						} catch (InterruptedException e) {
							// the queue should have no size limit - should not happen
							throw new UnrecoverableRuntimeException("A put operation on the event queue threw an InterruptedException.", e);
						}

					}
				}
				

			}
			finally {
				lock.unlock();
			}
			
		}

		@Override
		public void scheduleEvent(Event event, Queue<Event> queue,
				long executionTime) {
			scheduleEvent(new ScheduledEvent(event, queue, executionTime));
			
		}

		@Override
		public void scheduleEventWithDelay(Event event, Queue<Event> queue,
				long delay) {
			long currTime = environment.getTimeProvider().getCurrentTime();
			long executionTime = currTime + delay;
			scheduleEvent(new ScheduledEvent(event, queue, executionTime));
			
		}
		
		
		
		protected void processScheduler() {
			lock.lock();
			
			try {
				
				long currTime = environment.getTimeProvider().getCurrentTime();
				
				ListIterator<ScheduledEvent> iter = scheduledEvents.listIterator();
				while (iter.hasNext()) {
					ScheduledEvent scheduledEvent = iter.next();
					if (scheduledEvent.getExecutionTime() <= currTime) {
						try {
							devLog.debug("Enqueuing a scheduled event: " + scheduledEvent.getEvent().hashCode() + ", ex. time: " + scheduledEvent.getExecutionTime());
							queue.put(scheduledEvent.getEvent());
						} catch (InterruptedException e) {
							//this should never happen if the processor is running
							if (running) {
								throw new UnrecoverableRuntimeException("An exception was thrown while inserting an event to an event queue.");
							}
							//else -> do nothing
						}
						iter.remove();
					}
					else {
						break;	//no more events should be added to the queue, as they are ordered by the execution time
					}
				}
			}
			finally {
				lock.unlock();
			}
		}
		
	}
	
	public static class DummyEvent extends Event {
		public DummyEvent() {
			super(0, EventCategory.undefinedEvent, 0, (ProcessEventProxy)null, null);
		}
	}
	
	
	protected Environment environment;
	
	protected EventQueueProcessingInfo[] eventQueuesProcessingInfo;
	protected BlockingQueue<Event>[] queues;
	protected ThreadPoolInfo[] threadPoolInfos;
	protected ExecutorService[] threadPools;
	protected Future<?>[][] futures;
	protected EventQueueProcessorRunnable[] queueProcessorRunnables;
	protected HashMap<BlockingQueue<Event>, EventQueueProcessorRunnable> queueProcessorRunnablesByQueue;
	
	protected volatile boolean initialized = false;
	protected volatile boolean running = false;
	protected volatile boolean paused = false;
	protected volatile boolean error = false;
	
	protected Object errorLock = new Object();
	
	protected EventProcessingErrorCallback errorCallback;
	protected Object errorCallbackArg;

	
	
	
	
	@SuppressWarnings("unchecked")
	public synchronized void initialize(Environment environment, BlockingQueue<Event> queue) {
		initialize(environment, (BlockingQueue<Event>[]) new BlockingQueue<?>[] {queue}, new EventQueueProcessingInfo[] {new EventQueueProcessingInfo(new ThreadPoolInfo(1, 60), null, true)});
	}
	
	@SuppressWarnings("unchecked")
	public synchronized void initialize(Environment environment, BlockingQueue<Event> queue, EventQueueProcessingInfo eventQueueProcessingInfo) {
		initialize(environment, (BlockingQueue<Event>[]) new BlockingQueue<?>[] {queue}, new EventQueueProcessingInfo[] {eventQueueProcessingInfo});
	}
	
	public synchronized void initialize(Environment environment, BlockingQueue<Event>[] queues, EventQueueProcessingInfo[] eventQueuesProcessingInfo) {
		initialize(environment, queues, eventQueuesProcessingInfo, null, null);
	}
	
	@SuppressWarnings("unchecked")
	public synchronized void initialize(Environment environment, BlockingQueue<Event>[] queues, EventQueueProcessingInfo[] eventQueuesProcessingInfo, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) {
		
		//check:
		
		if (environment == null || environment.getTimeProvider() == null) {
			throw new IllegalArgumentException("Invalid Environment object specified");
		}
		this.environment = environment;
		
		if (queues == null || eventQueuesProcessingInfo == null) {
			throw new IllegalArgumentException("The queues and threadPoolInfos must be not null");
		}
		if (queues.length != eventQueuesProcessingInfo.length) {
			throw new IllegalArgumentException("The number of queues should be equal to the number of thread pool info objects");
		}
		for (int i = 0; i < queues.length; i++) {
			if (queues[i] == null) {
				throw new IllegalArgumentException("queue [" + i + "] is null");
			}
			
			if (eventQueuesProcessingInfo[i] == null) {
				throw new IllegalArgumentException("threadPoolInfo[" + i + "] is null");
			}
			
			if (eventQueuesProcessingInfo[i].getThreadPoolInfo() == null) {
				throw new IllegalArgumentException("eventQueuesProcessingInfo[" + i + "].threadPoolInfo is null");
			}

			//check poolSize, or keepAliveTime less than zero
			if (eventQueuesProcessingInfo[i].getThreadPoolInfo().getPoolSize() < 0) {
				throw new IllegalArgumentException("threadPoolInfos[" + i + "].poolSize is less than 0.");
			}
			if (eventQueuesProcessingInfo[i].getThreadPoolInfo().getKeepAliveTimeSec() < 0) {
				throw new IllegalArgumentException("threadPoolInfos[" + i + "].keepAliveTimeSec is less than 0."); 
			}

		}
		
		
		if (devLog.isInfoEnabled()) {
			devLog.info("Initializing the event queue processor.");
		}
		if (userLog.isInfoEnabled()) {
			userLog.info("Initializing the event queue processor.");
		}
		
		this.errorCallback = errorCallback;
		this.errorCallbackArg = errorCallbackArg;
		
		//prepare the runnable objects
		this.queues = new BlockingQueue[queues.length];
		this.queueProcessorRunnables = new EventQueueProcessorRunnable[queues.length];
		this.eventQueuesProcessingInfo = new EventQueueProcessingInfo[eventQueuesProcessingInfo.length];
		this.threadPoolInfos = new ThreadPoolInfo[eventQueuesProcessingInfo.length];
		this.queueProcessorRunnablesByQueue = new HashMap<BlockingQueue<Event>, EventQueueProcessorRunnable>(HashMapUtils.getHashMapCapacityForElementsNum(eventQueuesProcessingInfo.length, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		for (int i = 0; i < queues.length; i++) {
			this.queues[i] = queues[i];
			this.eventQueuesProcessingInfo[i] = eventQueuesProcessingInfo[i];
			this.threadPoolInfos[i] = eventQueuesProcessingInfo[i].getThreadPoolInfo();
			
			EventQueueProcessorRunnable queueProcessorRunnable;
			
			if (eventQueuesProcessingInfo[i].getWakeable()) {
				
				queueProcessorRunnable = new EventQueueProcessorRunnable(queues[i], i, threadPoolInfos[i].getPoolSize());
				
				//check if the queue is notifying, if it is -> set the lock to the wakeable's lock
				if (!(queues[i] instanceof NotifyingQueue<?>)) {
					//throw
				}
				
				((NotifyingQueue<?>)queues[i]).setInsertNotifyLock(queueProcessorRunnable.getWakeableManagerLock());
				
				WakeableManagerQueueListener<Event> wakeupQueueListener = new WakeableManagerQueueListener<Event>(queueProcessorRunnable);
				((NotifyingQueue<Event>)queues[i]).addListener(wakeupQueueListener);
				
			}
			else {
				queueProcessorRunnable = new EventQueueProcessorRunnable(queues[i], i, threadPoolInfos[i].getPoolSize());
			}
			
			this.queueProcessorRunnables[i] = queueProcessorRunnable;
			this.queueProcessorRunnablesByQueue.put(this.queues[i], queueProcessorRunnable);
	
		}

		
		initialized = true;
		
	}
	
	
	public WakeableManager getWakeableManagerByQueue(LinkedBlockingQueue<Event> queue) {
		if (initialized) return queueProcessorRunnablesByQueue.get(queue);
		else return null;
	}
	
	
	@Override
	public synchronized boolean isRunning() {
		return running;
	}
	
	@Override
	public synchronized boolean isInitialized() {
		return initialized;
	}
	
	@Override
	public synchronized boolean isPaused() {
		return paused;
	}
	

	
	
	
	
	@Override
	public void start() {
		if (!initialized) {
			throw new EventQueueProcessorRuntimeException("The event queue processor has not been initialized.");
		}
		
		running = true;
		paused = false;
				
		if (devLog.isInfoEnabled()) {
			devLog.info("Starting processing the event queues.");
		}
		if (userLog.isInfoEnabled()) {
			userLog.info("Starting processing the event queues.");
		}
		
		this.threadPools = new ExecutorService[queues.length];
		this.futures = new Future[queues.length][];
		for (int i = 0; i < queues.length; i++) {
			ThreadPoolExecutor tpe = new ThreadPoolExecutor(threadPoolInfos[i].getPoolSize(), threadPoolInfos[i].getPoolSize(), threadPoolInfos[i].getKeepAliveTimeSec(), TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
			if (threadPoolInfos[i].getKeepAliveTimeSec() > 0) {
				tpe.allowCoreThreadTimeOut(true);
			}
			this.threadPools[i] = tpe;
			
			//for (int j = 0; j < threadPoolInfos[i].getPoolSize(); j++) {
				this.threadPools[i].submit(queueProcessorRunnables[i]);
			//}
			//will propagate automatically to all available threads
			
		}
		
		if (devLog.isInfoEnabled()) {
			devLog.info("Started processing the event queues.");
		}
		if (userLog.isInfoEnabled()) {
			userLog.info("Started processing the event queues.");
		}

	}

	@Override
	public void stop() {
		if (running) {
			
			if (devLog.isInfoEnabled()) {
				devLog.info("Stopping processing the event queues.");
			}
			if (userLog.isInfoEnabled()) {
				userLog.info("Stopping processing the event queues.");
			}

			this.running = false;			
			
			for (int i = 0; i < threadPools.length; i++) {
				//log debug
				threadPools[i].shutdownNow();	//!!!should interrupt the threads waiting on a blocking queue, if threads cannot be interrupted, set some timeout on queue.take() -> queue.poll(timeout, TimeUnit.Milliseconds)
			}
			for (int i = 0; i < threadPools.length; i++) {
				while (! threadPools[i].isTerminated()) {
					try {
						threadPools[i].awaitTermination(100, TimeUnit.MILLISECONDS);
						threadPools[i].shutdownNow();
					} catch (InterruptedException e) {
						//debug log
					}
				}
				//threadPools[i].shutdown();
			}
			
			for (int i = 0; i < threadPools.length; i++) {
				if (this.eventQueuesProcessingInfo[i].getWakeable()) {
					((NotifyingQueue<?>)this.queues[i]).discard();
				}
				this.queueProcessorRunnables[i].discard();
			}
			
			this.threadPools = null;
			paused = false;
			
			if (devLog.isInfoEnabled()) {
				devLog.info("Stopped processing the event queues.");
			}
			if (userLog.isInfoEnabled()) {
				userLog.info("Stopped processing the event queues.");
			}
			
		}

	}

	@Override
	public void pause() {
		if (devLog.isInfoEnabled()) {
			devLog.info("Pausing processing the event queues.");
		}
		if (userLog.isInfoEnabled()) {
			userLog.info("Pausing processing the event queues.");
		}
		
		paused = true;

	}

	@Override
	public void resume() {
		if (devLog.isInfoEnabled()) {
			devLog.info("Resuming processing the event queues.");
		}
		if (userLog.isInfoEnabled()) {
			userLog.info("Resuming processing the event queues.");
		}
		
		paused = false;
		
	}

	@Override
	public void clear() {
		if (devLog.isInfoEnabled()) {
			devLog.info("Clearing the event queue processor.");
		}
		if (userLog.isInfoEnabled()) {
			userLog.info("Clearing the event queue processor.");
		}
		
		if (running) stop();
		this.queueProcessorRunnables = null;
		this.queues = null;
		this.eventQueuesProcessingInfo = null;
		this.threadPoolInfos = null;
		error = false;
		errorCallback = null;
		errorCallbackArg = null;
		initialized = false;
		
	}

	@Override
	public void scheduleEvent(ScheduledEvent scheduledEvent) {
		if (scheduledEvent.getEvent() == null) {
			throw new IllegalArgumentException("The event of the scheduledEvent is not set");
		}
		
		if (scheduledEvent.getQueue() != null) {
			schedule(scheduledEvent.getEvent(), scheduledEvent.getQueue(), scheduledEvent.getExecutionTime());
		}
		else {
			throw new IllegalArgumentException("The queue is not specified.");
		}
		
	}

	@Override
	public void scheduleEvent(Event event, Queue<Event> queue,
			long executionTime) {
		if (event == null) {
			throw new IllegalArgumentException("The event object is not set");
		}
		
		if (queue != null) {
			schedule(event, queue, executionTime);
		}
		else {
			throw new IllegalArgumentException("The queue is not specified.");
		}
		
	}

	@Override
	public void scheduleEventWithDelay(Event event, Queue<Event> queue,
			long delay) {
		
		if (event == null) {
			throw new IllegalArgumentException("The event object is not set");
		}
		
		if (delay < 0) {
			throw new IllegalArgumentException("Delay must not be negative");
		}
		
		long currTime = environment.getTimeProvider().getCurrentTime();
		long executionTime = currTime + delay;
		
		if (queue != null) {
			schedule(event, queue, executionTime);
		}
		else {
			throw new IllegalArgumentException("The queue is not specified.");
		}
		
	}

	
	
	
	protected void schedule(Event event, Queue<Event> queue, long executionTime) {
		//add the event to the scheduledEvents list of the appropriate runnable - dodac tam metode schedule() i lockowac na wakeablemanagerlock (moze zmienic nazwe tej zmiennej na wakeablemanagerschedulerlock)?
		EventScheduler scheduler = this.queueProcessorRunnablesByQueue.get(queue);
		if (scheduler == null) {
			throw new IllegalArgumentException("Invalid scheduler queue specified");
		}
		
		scheduler.scheduleEvent(event, queue, executionTime);
	}


	



	


}
