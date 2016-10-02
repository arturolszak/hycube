package net.hycube.eventprocessing;

import java.util.Queue;

import net.hycube.environment.ScheduledTask;
import net.hycube.eventprocessing.Event;

public class ScheduledEvent implements ScheduledTask {

	protected long executionTime;
	
	protected Event event;
	protected Queue<Event> queue;

	public Event getEvent() {
		return event;
	}
	
	public Queue<Event> getQueue() {
		return queue;
	}
	
	
	
	public ScheduledEvent(Event event) {
		this(event, null, 0);
	}
	
	public ScheduledEvent(Event event, Queue<Event> queue) {
		this(event, queue, 0);
	}
	
	public ScheduledEvent(Event event, Queue<Event> queue, long executionTime) {
		this.event = event;
		this.queue = queue;
		this.executionTime = executionTime;
	}

	
	@Override
	public long getExecutionTime() {
		return executionTime;
	}
	
	public void setExecutionTime(long executionTime) {
		this.executionTime = executionTime;
	}
	
	
	@Override
	public void run() {
		queue.add(event);
	}

	
	
	
}
