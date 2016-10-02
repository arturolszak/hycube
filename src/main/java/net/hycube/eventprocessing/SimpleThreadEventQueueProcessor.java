package net.hycube.eventprocessing;

import java.lang.Thread.State;
import java.util.concurrent.BlockingQueue;

import net.hycube.logging.LogHelper;

public class SimpleThreadEventQueueProcessor implements EventQueueProcessor {
	
	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(SimpleThreadEventQueueProcessor.class); 
	

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
						
					} catch (InterruptedException e) {
						if (devLog.isDebugEnabled()) {
							devLog.debug("Interrupted on queue.take(), queue index: " + index);
						}
					}
					
					if (event != null) {
						if (devLog.isDebugEnabled()) {
							devLog.debug("Processing the event..., queue index: " + index);
						}
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
	    					devLog.debug("Stopping the event processor in a separate thread.");
	    				}
	    				
	    				//stop processing the queues, in a separate thread to avoid deadlock when waiting for this thread to finish
	    				Thread stopThread = new Thread(new Runnable() {
		    					public void run() {
		    						stop();
		    					}
	    					});
	    				stopThread.start();
	    				
	    				if (devLog.isDebugEnabled()) {
	    					devLog.debug("Calling the user callback in a separate thread.");
	    				}
	    				
	    				//if specified, execute the error callback
	    				if (errorCallback != null) {
	    					Thread errorCallbackThread = new Thread(new Runnable() {
			    					public void run() {
			    						errorCallback.errorOccurred(errorCallbackArg);
			    					}
		    					});
	    					errorCallbackThread.start();
	    				}
	    				
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
	protected Thread[][] threads;
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
		
		running = true;
		paused = false;
		
		if (devLog.isInfoEnabled()) {
			devLog.info("Starting processing the event queues.");
		}
		if (userLog.isInfoEnabled()) {
			userLog.info("Starting processing the event queues.");
		}
		
		this.threads = new Thread[queues.length][];
		for (int i = 0; i < queues.length; i++) {
			EventQueueProcessorRunnable queueProcessorRunnable = queueProcessorRunnables[i];
			this.threads[i] = new Thread[threadCounts[i]];
			for (int j = 0; j < threadCounts[i]; j++) {
				threads[i][j] = new Thread(queueProcessorRunnable, "Event queue processor thread " + i + "." + j);
			}
		}
		for (int i = 0; i < threads.length; i++) {
			for (int j = 0; j < threads[i].length; j++) {
				threads[i][j].start();
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
			
			
			for (int i = 0; i < threads.length; i++) {
				for (int j = 0; j < threads[i].length; j++) {
					threads[i][j].interrupt();
				}
			}
			for (int i = 0; i < threads.length; i++) {
				for (int j = 0; j < threads[i].length; j++) {
					while (threads[i][j].getState() != State.TERMINATED) {
						try {
							threads[i][j].join();
						} catch (InterruptedException e) {
							//debug log
						}
					}
				}
			}

			this.threads = null;			
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
