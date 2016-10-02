package net.hycube.environment;


public class TimeProviderScheduler implements Scheduler {
	
	protected TimeProvider timeProvider;
	
	
	public TimeProviderScheduler(TimeProvider timeProvider) {
		this.timeProvider = timeProvider;
	}
	

	@Override
	public void scheduleTask(ScheduledTask scheduledTask) {
		timeProvider.schedule(scheduledTask);
	}
	

	@Override
	public void scheduleTask(ScheduledTask scheduledTask, long executionTime) {
		timeProvider.schedule(scheduledTask, executionTime);
	}

	
	@Override
	public void scheduleTaskWithDelay(ScheduledTask scheduledTask, long delay) {
		timeProvider.scheduleWithDelay(scheduledTask, delay);
	}
	
	
}
