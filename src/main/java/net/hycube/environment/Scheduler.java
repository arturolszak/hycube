package net.hycube.environment;

public interface Scheduler {

	public void scheduleTask(ScheduledTask scheduledTask);

	public void scheduleTask(ScheduledTask scheduledTask, long executionTime);

	public void scheduleTaskWithDelay(ScheduledTask scheduledTask, long delay);

}