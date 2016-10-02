package net.hycube.core;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.hycube.backgroundprocessing.BackgroundProcessEntryPoint;
import net.hycube.common.EntryPoint;
import net.hycube.dht.DHTManager;
import net.hycube.environment.Environment;
import net.hycube.environment.NodeProperties;
import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventCategory;
import net.hycube.eventprocessing.EventScheduler;
import net.hycube.eventprocessing.EventType;
import net.hycube.eventprocessing.ProcessEventProxy;
import net.hycube.extensions.Extension;
import net.hycube.join.JoinManager;
import net.hycube.leave.LeaveManager;
import net.hycube.lookup.LookupManager;
import net.hycube.maintenance.NotifyProcessor;
import net.hycube.messaging.ack.AckProcessInfo;
import net.hycube.messaging.callback.MessageReceivedCallback;
import net.hycube.messaging.data.ReceivedDataMessage;
import net.hycube.messaging.messages.MessageFactory;
import net.hycube.messaging.processing.MessageSendProcessInfo;
import net.hycube.messaging.processing.MessageSendProcessor;
import net.hycube.messaging.processing.ProcessMessageException;
import net.hycube.messaging.processing.ReceivedMessageProcessor;
import net.hycube.nexthopselection.NextHopSelector;
import net.hycube.routing.RoutingManager;
import net.hycube.search.SearchManager;
import net.hycube.transport.NetworkAdapter;
import net.hycube.transport.NetworkAdapterException;
import net.hycube.transport.ReceivedMessageProcessProxy;

public interface NodeAccessor {

	public Node getNode();
	
	public NodeId getNodeId();
	public NodeIdFactory getNodeIdFactory();
	public RoutingTable getRoutingTable();
	public Environment getEnvironment();
	public MessageFactory getMessageFactory();
	public NodeProperties getProperties();
	public NodeParameterSet getNodeParameterSet();
	public LinkedBlockingQueue<ReceivedDataMessage> getAppMessageInQueue();
	public Map<Short, LinkedBlockingQueue<ReceivedDataMessage>> getAppPortMessageInQueues();
	public MessageReceivedCallback getAppMessageReceivedCallback();
	public Map<Short, MessageReceivedCallback> getAppPortMessageReceivedCallbacks();
	public Object getAppMessageInLock();
	public Collection<ReceivedMessageProcessor> getReceivedMessageProcessors();
	public Collection<MessageSendProcessor> getMessageSendProcessors();
	public NextHopSelector getNextHopSelector(String nextHopSelectorKey);
	public RoutingManager getRoutingManager();
	public LookupManager getLookupManager();
	public SearchManager getSearchManager();
	public JoinManager getJoinManager();
	public LeaveManager getLeaveManager();
	public DHTManager getDHTManager();
	public NotifyProcessor getNotifyProcessor();
	public NetworkAdapter getNetworkAdapter();
	public NodePointer getNodePointer();

	public ProcessEventProxy getProcessEventProxy();
	public ReceivedMessageProcessProxy getNodeProcessReceivedMessageProxy();
	
	public Object getData(String key, Object defaultValue);
	public Object getData(String key);
	public void setData(String key, Object data);
	public void removeData(String key);
	
	public Extension getExtension(String key);
	
	
	public EntryPoint getExtensionEntryPoint(String extensionKey);
	public BackgroundProcessEntryPoint getBackgroundProcessEntryPoint(String backgroundProcessKey);
	public EntryPoint getRoutingManagerEntryPoint();
	public EntryPoint getLookupManagerEntryPoint();
	public EntryPoint getSearchManagerEntryPoint();
	public EntryPoint getJoinManagerEntryPoint();
	public EntryPoint getDHTManagerEntryPoint();

	public boolean sendMessage(MessageSendProcessInfo mspi, boolean wait) throws NetworkAdapterException, ProcessMessageException;
	public boolean sendMessageToNode(MessageSendProcessInfo mspi, boolean wait) throws NetworkAdapterException, ProcessMessageException;
	public boolean resendMessage(AckProcessInfo ackProcessInfo) throws NetworkAdapterException, ProcessMessageException;
		
	public int getNextMessageSerialNo();
	
	public BlockingQueue<Event> getEventQueue(EventCategory eventCategory);
	public BlockingQueue<Event> getEventQueue(EventType eventType);
	
	public EventScheduler getEventScheduler();

	public ReentrantReadWriteLock getDiscardLock();
		

}
