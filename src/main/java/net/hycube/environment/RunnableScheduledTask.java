package net.hycube.environment;

public class RunnableScheduledTask implements ScheduledTask {
	
	protected Runnable runnable;
	protected long executionTime;
	
	public RunnableScheduledTask(Runnable runnable) {
		this(runnable, 0);
	}
	
	public RunnableScheduledTask(Runnable runnable, long executionTime) {
		this.runnable = runnable;
		this.executionTime = executionTime;
	}
	
	@Override
	public long getExecutionTime() {
		return executionTime;
	}

	@Override
	public void setExecutionTime(long executionTime) {
		this.executionTime = executionTime;
		
	}
	
	@Override
	public void run() {
		this.runnable.run();
	}


	
	
}
