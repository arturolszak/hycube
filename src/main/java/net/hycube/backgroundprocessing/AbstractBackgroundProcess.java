package net.hycube.backgroundprocessing;

import java.util.Queue;

import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.environment.NodeProperties;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventCategory;
import net.hycube.eventprocessing.EventScheduler;
import net.hycube.eventprocessing.EventType;
import net.hycube.logging.LogHelper;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public abstract class AbstractBackgroundProcess implements BackgroundProcess {

	
	protected class BackgroundProcessEntryPointImpl implements BackgroundProcessEntryPoint {
		
		@Override
		public Object call() {
			throw new UnsupportedOperationException("Entry point not implemented.");
		}
		
		@Override
		public Object call(Object arg) {
			throw new UnsupportedOperationException("Entry point not implemented.");
		}
		
		@Override
		public Object call(Object[] args) {
			throw new UnsupportedOperationException("Entry point not implemented.");
		}
		
		@Override
		public Object call(Object entryPoint, Object[] args) {
			throw new UnsupportedOperationException("Entry point not implemented.");
		}
		
		@Override
		public boolean isRunning() {
			return AbstractBackgroundProcess.this.isRunning();
		}
		
		@Override
		public void start() {
			AbstractBackgroundProcess.this.start();
		}
		
		@Override
		public void stop() {
			AbstractBackgroundProcess.this.stop();
		}
		
		@Override
		public void processOnce() {
			AbstractBackgroundProcess.this.doProcess();
		}
		
	};
	
	
//	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(AbstractBackgroundProcess.class); 
	
	
	protected static final String PROP_KEY_SCHEDULE_INTERVAL = "ScheduleInterval";
	
	protected long nextSchedTime;
	
	protected NodeAccessor nodeAccessor;
	protected NodeProperties properties;
	
	
	protected String eventTypeKey;
	protected EventType eventType;
	
	protected BackgroundProcessEventProxy bkgProcessEventProxy;
	
	protected int scheduleInterval;
	
	protected boolean running;
	protected boolean discarded;
	
	protected boolean scheduled;
	
	protected BackgroundProcessEntryPoint entryPoint;
	
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		this.discarded = false;
		
		
		this.properties = properties;
		
		this.nodeAccessor = nodeAccessor;
		
		this.eventTypeKey = properties.getProperty(PROP_KEY_EVENT_TYPE_KEY);
		
		this.eventType = new EventType(EventCategory.executeBackgroundProcessEvent, this.eventTypeKey);
		
		try {
			this.scheduleInterval = (Integer) properties.getProperty(PROP_KEY_SCHEDULE_INTERVAL, MappedType.INT);
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize a background process instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		
		this.bkgProcessEventProxy = new BackgroundProcessEventProxy(this);
		

		this.entryPoint = new BackgroundProcessEntryPointImpl();
		
		
		long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
		setNextSchedTime(currTime + scheduleInterval);
		
		this.scheduled = false;
		
	}

	
	@Override
	public void process() throws BackgroundProcessException {
		
		nodeAccessor.getDiscardLock().readLock().lock();
		
		if (discarded) {
			nodeAccessor.getDiscardLock().readLock().unlock();
			return;
		}
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("process called: " + eventTypeKey);
		}
		
		//unset the scheduled flag, so the next run may be scheduled
		synchronized(this) {
			this.scheduled = false;
		}
		
		try {
			if (running) {	//doesn't have to be synchronized - no need to block threads, one more run is allowed
				doProcess();
			}
		} catch(Exception e) {
			nodeAccessor.getDiscardLock().readLock().unlock();
			throw new BackgroundProcessException("An exception has been thrown while processing a background process event.", e);
		}
		
		long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
    	long nextSchedTime = currTime + scheduleInterval; 
    	setNextSchedTime(nextSchedTime);
		
		schedule();
		
		nodeAccessor.getDiscardLock().readLock().unlock();
		
	}

	
	
	protected abstract void doProcess() throws BackgroundProcessException;
	
	
	
	@Override
	public void schedule() {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("schedule called: " + eventTypeKey);
		}
		
		
		nodeAccessor.getDiscardLock().readLock().lock();
		
		if (discarded) {
			nodeAccessor.getDiscardLock().readLock().unlock();
			return;
		}
		
		
		synchronized(this) {
			
			if (running) {
			
				//check if the next run is not already scheduled
				if (! scheduled) {
					
					//create the event
					Event event = new Event(0, eventType, bkgProcessEventProxy, BackgroundProcessEventProxy.BackgroundProcessEventProxyOperation.PROCESS);
					
					//schedule the event:
					Queue<Event> queue = nodeAccessor.getEventQueue(eventType);
					EventScheduler scheduler = nodeAccessor.getEventScheduler();
					scheduler.scheduleEvent(event, queue, getNextSchedTime());
					
					scheduled = true;
					
				}
			
			}
			
		}
		
		
		nodeAccessor.getDiscardLock().readLock().unlock();
		
		
	}
	
	

	@Override
	public void discard() {
		
		stop();
		
		this.discarded = true;
		
		
	}
	
	
	@Override
	public String getEventTypeKey() {
		return eventTypeKey;
	}
	
	
	protected synchronized long getNextSchedTime() {
		return nextSchedTime;
	}
	
	
	protected synchronized void setNextSchedTime(long schedTime) {
		this.nextSchedTime = schedTime;
	}

	
	@Override
	public BackgroundProcessEntryPoint getBackgroundProcessEntryPoint() {
		return entryPoint;
	}
	
	
	
	
	/**
	 * Returns true if the process is running (see start/stop methods)
	 */
	public boolean isRunning() {
		synchronized(this) {
			return running;
		}
	}
	
	
	
	
	/**
	 * Schedules the execution of the background process
	 */
	@Override
	public void start() {
		synchronized(this) {
			if (! running) {
				running = true;
				schedule();
			}
		}
	}
	
	
	/**
	 * Prevents scheduling next executions of the background process
	 */
	@Override
	public void stop() {
		synchronized(this) {
			if (running) {
				running = false;
			}
		}
	}
	
	
	
	
	
	
	/**
	 * Runs the background process
	 */
	@Override
	public void processOnce() throws BackgroundProcessException {
		doProcess();		
	}
	
	
	
}
