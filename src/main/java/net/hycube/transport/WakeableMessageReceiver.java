package net.hycube.transport;

import net.hycube.core.InitializationException;
import net.hycube.environment.Environment;
import net.hycube.environment.NodeProperties;
import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.NotifyingBlockingQueue;
import net.hycube.eventprocessing.Wakeable;
import net.hycube.eventprocessing.WakeableManager;

public interface WakeableMessageReceiver extends MessageReceiver, Wakeable {

	public void initialize(Environment environment, NotifyingBlockingQueue<Event> receiveEventQueue, NodeProperties properties) throws MessageReceiverException, InitializationException;
	
	public void initialize(Environment environment, NotifyingBlockingQueue<Event> receiveEventQueue, WakeableManager wakeableManager, NodeProperties properties) throws MessageReceiverException, InitializationException;
	
	
}
