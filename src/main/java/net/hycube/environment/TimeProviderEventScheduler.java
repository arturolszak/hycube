package net.hycube.environment;

import java.util.Queue;

import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventScheduler;
import net.hycube.eventprocessing.ScheduledEvent;


public class TimeProviderEventScheduler extends TimeProviderScheduler implements EventScheduler {
	
	public TimeProviderEventScheduler(TimeProvider timeProvider) {
		super(timeProvider);
	}
	
	public void scheduleEvent(ScheduledEvent scheduledEvent) {
		if (scheduledEvent.getEvent() == null) {
			throw new IllegalArgumentException("The event of the scheduledEvent is not set");
		}
		if (scheduledEvent.getQueue() == null) {
			throw new IllegalArgumentException("The queue for the scheduledEvent is not set");
		}
		timeProvider.schedule(scheduledEvent);
	}
	
	public void scheduleEvent(Event event, Queue<Event> queue, long executionTime) {
		ScheduledEvent scheduledEvent = new ScheduledEvent(event, queue, executionTime);
		scheduleEvent(scheduledEvent);
	}
	
	public void scheduleEventWithDelay(Event event, Queue<Event> queue, long delay) {
		ScheduledEvent scheduledEvent = new ScheduledEvent(event, queue);
		scheduleTaskWithDelay(scheduledEvent, delay);
	}

}
