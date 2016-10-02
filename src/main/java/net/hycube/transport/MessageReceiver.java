package net.hycube.transport;

import java.util.concurrent.BlockingQueue;

import net.hycube.core.InitializationException;
import net.hycube.environment.Environment;
import net.hycube.environment.NodeProperties;
import net.hycube.eventprocessing.Event;



public interface MessageReceiver {

	public void initialize(Environment environment, BlockingQueue<Event> receiveEventQueue, NodeProperties properties) throws InitializationException, MessageReceiverException;
	
//	public void initialize(Environment environment, BlockingQueue<Event> receiveEventQueue, NodeProperties properties, Object arg) throws InitializationException, MessageReceiverException;
	
	public void registerNetworkAdapter(NetworkAdapter networkAdapter) throws MessageReceiverException;
	
	public void unregisterNetworkAdapter(NetworkAdapter networkAdapter);
	
	public void startMessageReceiver();
	
	public void startMessageReceiver(int numEventsToEnqueue);
	
	public void receiveMessage() throws MessageReceiverException;
	
	public boolean isInitialized();
	
	public void discard() throws MessageReceiverException;
	
}
