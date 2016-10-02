package net.hycube.environment;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SystemTimeProvider implements TimeProvider {

	
	public static final int DEFAULT_SCHEDULER_THREAD_POOL_SIZE = 1;
	
	
	protected ScheduledThreadPoolExecutor schedExecService;
	protected boolean discarded;
	

	public SystemTimeProvider() {
		this(DEFAULT_SCHEDULER_THREAD_POOL_SIZE);
	}
	
	public SystemTimeProvider(int schedulerThreadPoolSize) {
		schedExecService = new ScheduledThreadPoolExecutor(schedulerThreadPoolSize);
		discarded = false;	
	}
	
	
	public boolean isDiscarded() {
		return discarded;
	}
	
	
	@Override
	public long getCurrentTime() {
		
		return System.currentTimeMillis();
		
	}

	@Override
	public void schedule(ScheduledTask scheduledTask) {
		schedule(scheduledTask, scheduledTask.getExecutionTime());
		
	}

	@Override
	public void schedule(ScheduledTask scheduledTask, long executionTime) {
		
		long currTime = System.currentTimeMillis();
		long delay = executionTime - currTime;
		if (delay < 0) delay = 0;
		
		scheduleWithDelay(scheduledTask, delay);
		
	}

	
	@Override
	public void scheduleWithDelay(ScheduledTask scheduledTask, long delay) {
		schedExecService.schedule(scheduledTask, delay, TimeUnit.MILLISECONDS);
		
	}

	
	@Override
	public void discard() {
		schedExecService.shutdownNow();
		discarded = true;
		
	}
	
	
}
