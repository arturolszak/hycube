package net.hycube.eventprocessing;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.hycube.logging.LogHelper;

public class SimpleThreadPoolEventQueueProcessor implements EventQueueProcessor {
	
	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(SimpleThreadPoolEventQueueProcessor.class); 
	

	public class EventQueueProcessorRunnable implements Runnable {

		protected BlockingQueue<Event> queue;
		protected int index;
		
		public EventQueueProcessorRunnable(BlockingQueue<Event> queue, int index) {
			this.queue = queue;
			this.index = index;
		}
		
		public BlockingQueue<Event> getQueue() {
			return this.queue;
		}
		
		
		@Override
		public void run() {
			if (devLog.isDebugEnabled()) {
				devLog.debug("Processing the queue, index: " + index);
			}
			try {
				while (running) {
					
					if (error) {
						if (devLog.isDebugEnabled()) {
							devLog.debug("An error occured in a different processing thread. Not processing any more queue events. Queue index: " + index);
						}
						return;
					}
					
					if (paused) {
						try {
							Thread.sleep(1000);
						}
						catch (InterruptedException e) {
							if (devLog.isDebugEnabled()) {
								devLog.debug("Interrupted on Thread.sleep while in paused state, queue index: " + index);
							}
						}
						continue;
					}
					
					Event event = null;
					
					try {
						if (devLog.isDebugEnabled()) {
							devLog.debug("Taking the event from the event queue, queue index: " + index);
						}
						
						event = queue.take();
						
						if (devLog.isDebugEnabled()) {
							devLog.debug("Processing the event..., queue index: " + index);
						}
						
					} catch (InterruptedException e) {
						if (devLog.isDebugEnabled()) {
							devLog.debug("Interrupted on queue.take(), queue index: " + index);
						}
					}
					if (event != null) {
						event.process();
					}
				}
			
			} 
			catch (Throwable e) {
	    		if (devLog.isFatalEnabled()) {
	    			devLog.fatal("An exception thrown while processing an event. The processing will be terminated.", e);
	    		}
	    		if (userLog.isFatalEnabled()) {
	    			userLog.fatal("An error occured while processing an event. The processing will be terminated.");
	    		}
	    		
	    		synchronized (errorLock) {
	    			if (error == false) {
	    				error = true;
	    				
	    				if (devLog.isDebugEnabled()) {
	    					devLog.debug("Stopping the event processor and calling the user callback in a separate thread.");
	    				}
	    				
	    				//stop processing the queues, in a separate thread to avoid deadlock when waiting for this thread to finish
	    				ExecutorService errorExecutor = Executors.newSingleThreadExecutor();
	    				errorExecutor.submit(new Runnable() {
		    					public void run() {
		    						synchronized(SimpleThreadPoolEventQueueProcessor.this) {
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
				devLog.debug("Processing the queue finished, index: " + index);
			}
			
		}
		
	}
	
	
	protected BlockingQueue<Event>[] queues;
	protected int threadCounts[]; 
	protected ExecutorService[] threadPools;
	protected Future<?>[][] futures;
	EventQueueProcessorRunnable[] queueProcessorRunnables;
	
	protected volatile boolean initialized = false;
	protected volatile boolean running = false;
	protected volatile boolean paused = false;
	protected volatile boolean error = false;
	
	protected Object errorLock = new Object();
	
	protected EventProcessingErrorCallback errorCallback;
	protected Object errorCallbackArg;
	
	
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
	public synchronized void start() {
		if (!initialized) {
			throw new EventQueueProcessorRuntimeException("The event queue processor has not been initialized.");
		}
		
		if (devLog.isInfoEnabled()) {
			devLog.info("Starting processing the event queues.");
		}
		if (userLog.isInfoEnabled()) {
			userLog.info("Starting processing the event queues.");
		}
		
		running = true;
		paused = false;
				
		this.threadPools = new ExecutorService[queues.length];
		this.futures = new Future[queues.length][];
		for (int i = 0; i < queues.length; i++) {
			this.threadPools[i] = Executors.newFixedThreadPool(threadCounts[i]);
			this.futures[i] = new Future[threadCounts[i]];
			for (int j = 0; j < threadCounts[i]; j++) {
				futures[i][j] = this.threadPools[i].submit(queueProcessorRunnables[i]);
			}
		}
		
		if (devLog.isInfoEnabled()) {
			devLog.info("Started processing the event queues.");
		}
		if (userLog.isInfoEnabled()) {
			userLog.info("Started processing the event queues.");
		}
		
	}
	
	@Override
	public synchronized void stop() {

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
	public synchronized void pause() {
		if (devLog.isInfoEnabled()) {
			devLog.info("Pausing processing the event queues.");
		}
		if (userLog.isInfoEnabled()) {
			userLog.info("Pausing processing the event queues.");
		}
		
		paused = true;
	}
	
	@Override
	public synchronized void resume() {
		if (devLog.isInfoEnabled()) {
			devLog.info("Resuming processing the event queues.");
		}
		if (userLog.isInfoEnabled()) {
			userLog.info("Resuming processing the event queues.");
		}
		
		paused = false;
	}
	
	@Override
	public synchronized void clear() {
		
		if (devLog.isInfoEnabled()) {
			devLog.info("Clearing the event queue processor.");
		}
		if (userLog.isInfoEnabled()) {
			userLog.info("Clearing the event queue processor.");
		}
		
		if (running) stop();
		this.queueProcessorRunnables = null;
		this.queues = null;
		this.threadCounts = null;
		error = false;
		errorCallback = null;
		errorCallbackArg = null;
		initialized = false;
	}
	
	@SuppressWarnings("unchecked")
	public synchronized void initialize(BlockingQueue<Event> queue) {
		initialize(new BlockingQueue[] {queue}, new int[] {1});
	}
	
	public synchronized void initialize(BlockingQueue<Event>[] queues, int[] threadCounts) {
		initialize(queues, threadCounts, null, null);
	}
	
	@SuppressWarnings("unchecked")
	public synchronized void initialize(BlockingQueue<Event>[] queues, int[] threadCounts, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) {
		
		//check:
		if (queues == null || threadCounts == null || queues.length != threadCounts.length) {
			throw new IllegalArgumentException("queues and threadCounts should be not null and contain the same number of elements.");
		}
		for (int i = 0; i < queues.length; i++) {
			if (queues[i] == null) {
				throw new IllegalArgumentException("queues[" + i + "] is null.");
			}
			if (threadCounts[i] <= 0) {
				throw new IllegalArgumentException("threadCounts[" + i + "] is less than or equal 0."); 
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
		this.threadCounts = new int[threadCounts.length];
		for (int i = 0; i < queues.length; i++) {
			this.queues[i] = queues[i];
			EventQueueProcessorRunnable queueProcessorRunnable = new EventQueueProcessorRunnable(queues[i], i);
			this.queueProcessorRunnables[i] = queueProcessorRunnable;
			this.threadCounts[i] = threadCounts[i];
		}
		
		initialized = true;
		
	}


	
}
