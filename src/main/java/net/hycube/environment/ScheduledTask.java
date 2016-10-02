package net.hycube.environment;

import java.util.Comparator;

public interface ScheduledTask extends Runnable {
	
	public class ScheduledTaskComparator implements Comparator <ScheduledTask> {

		@Override
		public int compare(ScheduledTask st1, ScheduledTask st2) {
			if (st1.getExecutionTime() == st2.getExecutionTime()) return 0;
			if (st1.getExecutionTime() > st2.getExecutionTime()) return 1;
			else return -1;
		}
		
	}
	
	
	public long getExecutionTime();
	public void setExecutionTime(long executionTime);
	
	@Override
	public void run();
	
	
}
