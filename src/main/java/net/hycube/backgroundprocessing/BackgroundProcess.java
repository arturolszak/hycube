package net.hycube.backgroundprocessing;

import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.environment.NodeProperties;

public interface BackgroundProcess {

	
	public static final String PROP_KEY_EVENT_TYPE_KEY = "EventTypeKey";
	public static final String PROP_KEY_SCHEDULE_IMMEDIATELY = "ScheduleImmediately";
	

	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException;
	
	
	/**
	 * Runs the background process logic
	 */
	public void process() throws BackgroundProcessException;
	
	
	/**
	 * Schedules the next execution of the background process (implementation is process dependent)
	 */
	public void schedule();
	
	
	/**
	 * Discard the process - will cause the following calls of schedule to do nothing 
	 */
	public void discard();
	
	
	
	/**
	 * Returns true if the process is running (see start/stop methods)
	 */
	public boolean isRunning();
	
	
	/**
	 * Schedules the execution of the background process
	 */
	public void start();
	
	
	/**
	 * Prevents scheduling next executions of the background process
	 */
	public void stop();
	
	
	/**
	 * Runs the background process
	 */
	public void processOnce() throws BackgroundProcessException;
	
	
	/**
	 * Gets the event type key for this background process events
	 * @return
	 */
	public String getEventTypeKey();
	
	
	
	/**
	 * This method returns an entry point to the Background process for external calls. It may as well return null if the extension does not have an entry point (cannot be called from outside).
	 * @return
	 */
	public BackgroundProcessEntryPoint getBackgroundProcessEntryPoint();

	
	
}
