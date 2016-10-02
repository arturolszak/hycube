package net.hycube.eventprocessing;

import java.util.Queue;

public interface EventScheduler {

	public void scheduleEvent(ScheduledEvent scheduledEvent);

	public void scheduleEvent(Event event, Queue<Event> queue, long executionTime);

	public void scheduleEventWithDelay(Event event, Queue<Event> queue, long delay);
	
}
