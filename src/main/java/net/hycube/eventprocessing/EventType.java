package net.hycube.eventprocessing;

public class EventType {

		
	protected EventCategory eventCategory;
	protected String eventTypeKey;
	
	
	public EventType(EventCategory eventCategory) {
		this(eventCategory, null);
	}
	
	
	public EventType(EventCategory eventCategory, String eventTypeKey) {
		if (eventTypeKey == null) eventTypeKey = "";
		this.eventCategory = eventCategory;
		this.eventTypeKey = eventTypeKey;
	}
	
	
	public EventCategory getEventCategory() {
		return eventCategory;
	}
	
	public String getEventTypeKey() {
		return eventTypeKey;
	}
	
	
	
	@Override
	public int hashCode() {
		return eventCategory.hashCode() + eventTypeKey.hashCode();
	}
	
	@Override
	public boolean equals(Object eventType) {
		if (eventType instanceof EventType) {
			if (this.eventCategory == ((EventType) eventType).eventCategory
					&& this.eventTypeKey.equals(((EventType) eventType).eventTypeKey)) {
				return true;
			}
		}
		return false;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder().append(this.eventCategory.toString());
		if (eventTypeKey != null && (!eventTypeKey.isEmpty())) sb.append('[').append(eventTypeKey).append(']');
		return sb.toString();
		
	}
	
}
