package net.hycube.eventprocessing;

import java.util.Comparator;


public class Event {
	
	protected long timestamp;
	protected EventType eventType;
	protected int priority;
	protected ProcessEventProxy processEventProxy;
	protected Object[] eventArgs;
	
	
	public Event(long timestamp, EventType eventType, int priority, ProcessEventProxy processEventProxy, Object eventArg) {
		this.timestamp = timestamp;
		this.eventType = eventType;
		this.priority = priority;
		this.processEventProxy = processEventProxy;
		this.eventArgs = new Object[] {eventArg};
	
	}
	
	public Event(long timestamp, EventType eventType, int priority, ProcessEventProxy processEventProxy, Object[] eventArgs) {
		this.timestamp = timestamp;
		this.eventType = eventType;
		this.priority = priority;
		this.processEventProxy = processEventProxy;
		this.eventArgs = eventArgs;
	
	}
	
	public Event(long timestamp, EventType eventType, ProcessEventProxy processEventProxy, Object eventArg) {
		this.timestamp = timestamp;
		this.eventType = eventType;
		this.priority = 0;
		this.processEventProxy = processEventProxy;
		this.eventArgs = new Object[] {eventArg};
	
	}
	
	public Event(long timestamp, EventType eventType, ProcessEventProxy processEventProxy, Object[] eventArgs) {
		this.timestamp = timestamp;
		this.eventType = eventType;
		this.priority = 0;
		this.processEventProxy = processEventProxy;
		this.eventArgs = eventArgs;
	
	}
	
	
	public Event(long timestamp, EventCategory eventCategory, int priority, ProcessEventProxy processEventProxy, Object eventArg) {
		this(timestamp, eventCategory.getEventType(), priority, processEventProxy, eventArg);
	
	}
	
	public Event(long timestamp, EventCategory eventCategory, int priority, ProcessEventProxy processEventProxy, Object[] eventArgs) {
		this(timestamp, eventCategory.getEventType(), priority, processEventProxy, eventArgs);
	
	}
	
	public Event(long timestamp, EventCategory eventCategory, ProcessEventProxy processEventProxy, Object eventArg) {
		this(timestamp, eventCategory.getEventType(), processEventProxy, eventArg);
	
	}
	
	public Event(long timestamp, EventCategory eventCategory, ProcessEventProxy processEventProxy, Object[] eventArgs) {
		this(timestamp, eventCategory.getEventType(), processEventProxy, eventArgs);
	
	}
	
	
	public class EventTimeComparator implements Comparator <Event> {
		@Override
		public int compare(Event e1, Event e2) {
			if (e1.getTimestamp() == e2.getTimestamp()) return 0;
			if (e1.getTimestamp() > e2.getTimestamp()) return 1;
			else return -1;
		}
	}
	
	public class EventPriorityComparator implements Comparator <Event> {
		@Override
		public int compare(Event e1, Event e2) {
			if (e1.getPriority() == e2.getPriority()) return 0;
			if (e1.getPriority() > e2.getPriority()) return 1;
			else return -1;
		}
	}
	
	public class EventTimePriorityComparator implements Comparator <Event> {
		@Override
		public int compare(Event e1, Event e2) {
			if (e1.getTimestamp() == e2.getTimestamp()) {
				if (e1.getPriority() == e2.getPriority()) return 0;
				if (e1.getPriority() > e2.getPriority()) return 1;
				else return -1;
			}
			if (e1.getTimestamp() > e2.getTimestamp()) return 1;
			else return -1;
		}
	}
	
	
	
	public long getTimestamp() {
		return timestamp;
	}
	
	public EventType getEventType() {
		return eventType;
	}
	
	public EventCategory getEventCategory() {
		return eventType.getEventCategory();
	}
	
	public int getPriority() {
		return priority;
	}
	
	public ProcessEventProxy getProcessEventProxy() {
		return processEventProxy;
	}	
	
	public Object getEventArg() {
		return eventArgs[0];
	}
	
	public Object getEventArg(int index) {
		return eventArgs[index];
	}
	
	public Object[] getEventArgs() {
		return eventArgs;
	}
	
	public void process() throws EventProcessException {
		if (this.processEventProxy != null) {
			this.processEventProxy.processEvent(this);
		}
	}
	
}
