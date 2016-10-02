package net.hycube;

import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import net.hycube.common.EntryPoint;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeId;
import net.hycube.core.NodePointer;
import net.hycube.dht.DeleteCallback;
import net.hycube.dht.GetCallback;
import net.hycube.dht.HyCubeDHTManagerEntryPoint;
import net.hycube.dht.HyCubeResource;
import net.hycube.dht.HyCubeResourceDescriptor;
import net.hycube.dht.PutCallback;
import net.hycube.dht.RefreshPutCallback;
import net.hycube.environment.Environment;
import net.hycube.environment.NodeProperties;
import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventScheduler;
import net.hycube.eventprocessing.EventType;
import net.hycube.maintenance.HyCubeRecoveryExtensionEntryPoint;



public class HyCubeNodeProxyService extends NodeProxyService implements HyCubeNodeService {

//	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
//	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeNodeProxyService.class); 
	
	public static final String PROP_KEY_RECOVERY_EXTENSION_KEY = "RecoveryExtensionKey";
	
	
	
	protected HyCubeRecoveryExtensionEntryPoint recoveryExtensionEntryPoint;
	protected HyCubeDHTManagerEntryPoint dhtManagerEntryPoint;
	
	
	
	public static HyCubeNodeProxyService initialize(String networkAddress, Environment environment, Map<EventType, LinkedBlockingQueue<Event>> eventQueues, EventScheduler eventScheduler) throws InitializationException {
		return initialize(null, null, null, networkAddress, environment, eventQueues, eventScheduler);
	}
	
	public static HyCubeNodeProxyService initialize(NodeId nodeId, String networkAddress, Environment environment, Map<EventType, LinkedBlockingQueue<Event>> eventQueues, EventScheduler eventScheduler) throws InitializationException {
		return initialize(null, nodeId, null, networkAddress, environment, eventQueues, eventScheduler);
	}
	
	public static HyCubeNodeProxyService initialize(String nodeIdString, String networkAddress, Environment environment, Map<EventType, LinkedBlockingQueue<Event>> eventQueues, EventScheduler eventScheduler) throws InitializationException {
		return initialize(null, null, nodeIdString, networkAddress, environment, eventQueues, eventScheduler);
	}
	
	protected static HyCubeNodeProxyService initialize(HyCubeNodeProxyService nodeProxy, NodeId nodeId, String nodeIdString, String networkAddress, Environment environment, Map<EventType, LinkedBlockingQueue<Event>> eventQueues, EventScheduler eventScheduler) throws InitializationException {
		if (nodeProxy == null) {
			nodeProxy = new HyCubeNodeProxyService();
		}
		NodeProxyService.initialize(nodeProxy, nodeId, nodeIdString, networkAddress, environment, eventQueues, eventScheduler);
		
		
		//read the configuration - extension keys
		
		String nodeProxyServiceKey = environment.getNodeProperties().getProperty(PROP_KEY_NODE_PROXY_SERVICE);
		if (nodeProxyServiceKey == null || nodeProxyServiceKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, environment.getNodeProperties().getAbsoluteKey(PROP_KEY_NODE_PROXY_SERVICE), "Invalid parameter value: " + environment.getNodeProperties().getAbsoluteKey(PROP_KEY_NODE_PROXY_SERVICE));
		NodeProperties nodeProxyServiceProperties = environment.getNodeProperties().getNestedProperty(PROP_KEY_NODE_PROXY_SERVICE, nodeProxyServiceKey);
		
				
		String recoveryExtensionKey = nodeProxyServiceProperties.getProperty(PROP_KEY_RECOVERY_EXTENSION_KEY);
		if (recoveryExtensionKey == null || recoveryExtensionKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, nodeProxyServiceProperties.getAbsoluteKey(PROP_KEY_RECOVERY_EXTENSION_KEY), "Invalid parameter value: " + nodeProxyServiceProperties.getAbsoluteKey(PROP_KEY_RECOVERY_EXTENSION_KEY));
		
				
		//get the extension entry points:
		EntryPoint entryPoint;
		

		//Get the recovery extension entry point:
		entryPoint = nodeProxy.node.getExtensionEntryPoint(recoveryExtensionKey);
		if (entryPoint != null && entryPoint instanceof HyCubeRecoveryExtensionEntryPoint) {
			nodeProxy.recoveryExtensionEntryPoint = (HyCubeRecoveryExtensionEntryPoint) entryPoint;
		}
		else {
			throw new InitializationException(InitializationException.Error.NODE_SERVICE_INITIALIZATION_ERROR, recoveryExtensionKey, "No entry point defined for the recovery extension or the entry point object is of illegal type.");
		}
		
		
		
		//Get the dht manager entry point:
		entryPoint = nodeProxy.node.getDHTManagerEntryPoint();
		if (entryPoint != null && entryPoint instanceof HyCubeDHTManagerEntryPoint) {
			nodeProxy.dhtManagerEntryPoint = (HyCubeDHTManagerEntryPoint) entryPoint;
		}
		else {
			throw new InitializationException(InitializationException.Error.NODE_SERVICE_INITIALIZATION_ERROR, null, "No entry point defined for the DHT manager or the entry point object is of illegal type.");
		}
		
		
		return nodeProxy;
		
	}
	

	
	

	//HyCubeNodeService methods:
	
	
	@Override
	public void recover() {
		recoveryExtensionEntryPoint.callRecover();
	}

	
	@Override
	public void recoverNS() {
		recoveryExtensionEntryPoint.callRecoverNS();
	}
	
	
	
	
	
	@Override
	public PutCallback put(NodePointer recipient, BigInteger key, HyCubeResource resource, PutCallback putCallback, Object putCallbackArg) {
		return dhtManagerEntryPoint.put(recipient, key, resource, putCallback, putCallbackArg, null);
	}

	@Override
	public RefreshPutCallback refreshPut(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor resourceDescriptor, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg) {
		return dhtManagerEntryPoint.refreshPut(recipient, key, resourceDescriptor, refreshPutCallback, refreshPutCallbackArg, null);
	}

	@Override
	public GetCallback get(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor criteria, GetCallback getCallback, Object getCallbackArg) {
		return dhtManagerEntryPoint.get(recipient, key, criteria, getCallback, getCallbackArg, null);
	}
	
	@Override
	public PutCallback put(BigInteger key, HyCubeResource resource, PutCallback putCallback, Object putCallbackArg) {
		return dhtManagerEntryPoint.put(key, resource, putCallback, putCallbackArg, null);
	}

	@Override
	public RefreshPutCallback refreshPut(BigInteger key, HyCubeResourceDescriptor resourceDescriptor, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg) {
		return dhtManagerEntryPoint.refreshPut(key, resourceDescriptor, refreshPutCallback, refreshPutCallbackArg, null);
	}

	@Override
	public GetCallback get(BigInteger key, HyCubeResourceDescriptor criteria, GetCallback getCallback, Object getCallbackArg) {
		return dhtManagerEntryPoint.get(key, criteria, getCallback, getCallbackArg, null);
	}


	@Override
	public DeleteCallback delete(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor criteria, DeleteCallback deleteCallback, Object deleteCallbackArg) {
		return dhtManagerEntryPoint.delete(recipient, key, criteria, deleteCallback, deleteCallbackArg, null);
	}
	
	@Override
	public PutCallback put(NodePointer recipient, BigInteger key, HyCubeResource resource, PutCallback putCallback, Object putCallbackArg, Object[] parameters) {
		return dhtManagerEntryPoint.put(recipient, key, resource, putCallback, putCallbackArg, parameters);
	}

	@Override
	public RefreshPutCallback refreshPut(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor resourceDescriptor, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg, Object[] parameters) {
		return dhtManagerEntryPoint.refreshPut(recipient, key, resourceDescriptor, refreshPutCallback, refreshPutCallbackArg, parameters);
	}

	@Override
	public GetCallback get(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor criteria, GetCallback getCallback, Object getCallbackArg, Object[] parameters) {
		return dhtManagerEntryPoint.get(recipient, key, criteria, getCallback, getCallbackArg, parameters);
	}
	
	@Override
	public PutCallback put(BigInteger key, HyCubeResource resource, PutCallback putCallback, Object putCallbackArg, Object[] parameters) {
		return dhtManagerEntryPoint.put(key, resource, putCallback, putCallbackArg, parameters);
	}

	@Override
	public RefreshPutCallback refreshPut(BigInteger key, HyCubeResourceDescriptor resourceDescriptor, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg, Object[] parameters) {
		return dhtManagerEntryPoint.refreshPut(key, resourceDescriptor, refreshPutCallback, refreshPutCallbackArg, parameters);
	}

	@Override
	public GetCallback get(BigInteger key, HyCubeResourceDescriptor criteria, GetCallback getCallback, Object getCallbackArg, Object[] parameters) {
		return dhtManagerEntryPoint.get(key, criteria, getCallback, getCallbackArg, parameters);
	}

	@Override
	public DeleteCallback delete(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor criteria, DeleteCallback deleteCallback, Object deleteCallbackArg, Object[] parameters) {
		return dhtManagerEntryPoint.delete(recipient, key, criteria, deleteCallback, deleteCallbackArg, parameters);
	}
	
	
	

	
	
	
	
}
