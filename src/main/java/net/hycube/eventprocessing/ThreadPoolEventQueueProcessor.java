package net.hycube.eventprocessing;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.hycube.logging.LogHelper;

public class ThreadPoolEventQueueProcessor implements EventQueueProcessor {
	
	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(ThreadPoolEventQueueProcessor.class); 
	

	public class EventQueueProcessorRunnable implements Runnable {

		protected BlockingQueue<Event> queue;
		protected int queueIndex;
		
		public EventQueueProcessorRunnable(BlockingQueue<Event> queue, int queueIndex) {
			this.queue = queue;
			this.queueIndex = queueIndex;
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
						
					Event event = null;
					
					try {
						//take the event from the queue - this is a blocking call and is called before submitting next runnable to the thread pool, so only one thread will be waiting on that queue at a time
						if (devLog.isDebugEnabled()) {
							devLog.debug("Taking the event from the event queue, queue index: " + queueIndex);
						}
						
						event = queue.take();
						
					} catch (InterruptedException e) {
						if (devLog.isDebugEnabled()) {
							devLog.debug("Interrupted on queue.take(), queue index: " + queueIndex);
						}
					}
					
					if (running) {
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
		    						synchronized(ThreadPoolEventQueueProcessor.this) {
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
		
	}
	
	
	protected BlockingQueue<Event>[] queues;
	protected ThreadPoolInfo[] threadPoolInfos; 
	protected ExecutorService[] threadPools;
	protected Future<?>[][] futures;
	protected EventQueueProcessorRunnable[] queueProcessorRunnables;
	
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
						threadPools[i].awaitTermination(1000, TimeUnit.MILLISECONDS);
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
		this.threadPoolInfos = null;
		error = false;
		errorCallback = null;
		errorCallbackArg = null;
		initialized = false;
	}
	
	@SuppressWarnings("unchecked")
	public synchronized void initialize(BlockingQueue<Event> queue) {
		initialize((BlockingQueue<Event>[]) new BlockingQueue<?>[] {queue}, new ThreadPoolInfo[] {new ThreadPoolInfo(1, 60)});
	}
	
	@SuppressWarnings("unchecked")
	public synchronized void initialize(BlockingQueue<Event> queue, ThreadPoolInfo threadPoolInfo) {
		initialize((BlockingQueue<Event>[]) new BlockingQueue<?>[] {queue}, new ThreadPoolInfo[] {threadPoolInfo});
	}
	
	public synchronized void initialize(BlockingQueue<Event>[] queues, ThreadPoolInfo[] threadPoolInfos) {
		initialize(queues, threadPoolInfos, null, null);
	}
	
	@SuppressWarnings("unchecked")
	public synchronized void initialize(BlockingQueue<Event>[] queues, ThreadPoolInfo[] threadPoolInfos, EventProcessingErrorCallback errorCallback, Object errorCallbackArg) {
		
		//check:
		if (queues == null || threadPoolInfos == null) {
			throw new IllegalArgumentException("The queues and threadPoolInfos must be not null");
		}
		if (queues.length != threadPoolInfos.length) {
			throw new IllegalArgumentException("The number of queues should be equal to the number of thread pool info objects");
		}
		for (int i = 0; i < queues.length; i++) {
			if (queues[i] == null) {
				throw new IllegalArgumentException("queue [" + i + "] is null");
			}
			if (threadPoolInfos == null) {
				throw new IllegalArgumentException("threadPoolInfos[" + i + "] is null");
			}

			//check poolSize, or keepAliveTime less than zero
			if (threadPoolInfos[i].getPoolSize() < 0) {
				throw new IllegalArgumentException("threadPoolInfos[" + i + "].poolSize is less than 0.");
			}
			if (threadPoolInfos[i].getKeepAliveTimeSec() < 0) {
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
		this.threadPoolInfos = new ThreadPoolInfo[threadPoolInfos.length];
		for (int i = 0; i < queues.length; i++) {
			this.queues[i] = queues[i];
			this.threadPoolInfos[i] = threadPoolInfos[i];
			EventQueueProcessorRunnable queueProcessorRunnable = new EventQueueProcessorRunnable(queues[i], i);
			this.queueProcessorRunnables[i] = queueProcessorRunnable;
		}
		
		initialized = true;
		
	}


	
}
