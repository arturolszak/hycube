package net.hycube.eventprocessing;




public enum EventCategory {
    
	undefinedEvent,
	
	receiveMessageEvent,
	processReceivedMessageEvent,
    
    pushMessageEvent,
    pushSystemMessageEvent,
    
    processAckCallbackEvent,
    processMsgReceivedCallbackEvent,
    
    
    
    executeBackgroundProcessEvent,

    extEvent,	//additional events may be generated and processed by extending code
    
    ;
    
	
	
	
	public EventType getEventType() {
		return getEventType(null);
	}
	
    public EventType getEventType(String eventTypeKey) {
		return new EventType(this, eventTypeKey);
	}
    
	
}
