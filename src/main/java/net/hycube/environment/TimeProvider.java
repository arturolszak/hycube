package net.hycube.environment;

public interface TimeProvider {
	
	public long getCurrentTime();
	
	public void schedule(ScheduledTask scheduledTask);
	
	public void schedule(ScheduledTask scheduledTask, long executionTime);
	
	public void scheduleWithDelay(ScheduledTask scheduledTask, long delay);
	
	
	public void discard();
	
	
}
