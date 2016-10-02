package net.hycube.backgroundprocessing;

import net.hycube.common.EntryPoint;

public interface BackgroundProcessEntryPoint extends EntryPoint {

	/**
	 * Runs the background process
	 */
	public void processOnce();
	
	
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
	
	
	
}
