package net.hycube.dht;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.HyCubeNodeId;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.core.NodeId;
import net.hycube.environment.NodeProperties;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.logging.LogHelper;
import net.hycube.utils.HashMapUtils;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public class HyCubeSimpleDHTStorageManager implements HyCubeDHTStorageManager {

	
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeSimpleDHTStorageManager.class); 
	
	
	protected static final String PROP_KEY_STORE_MULTIPLE_COPIES = "StoreMultipleCopies";
	protected static final String PROP_KEY_MAX_RESOURCES_NUM = "MaxResourcesNum";
	protected static final String PROP_KEY_MAX_KEY_SLOT_SIZE = "MaxKeySlotSize";
	protected static final String PROP_KEY_MAX_RESOURCE_SLOT_SIZE = "MaxResourceSlotSize";
	
	
	protected static final int INITIAL_HASH_TABLE_SIZE = 10;
	protected static final int INITIAL_HASH_TABLE_SLOT_SIZE = 1;
	protected static final int INITIAL_HASH_TABLE_RES_SLOT_SIZE = 1;

	
	protected NodeAccessor nodeAccessor;
	protected NodeProperties properties;
	
	protected HyCubeDHTManager dhtManager;
	
	
	protected HashMap<BigInteger, HashMap<String, HashMap<String, HyCubeResourceEntry>>> resources;
	protected LinkedList<HyCubeResourceEntry> resourceEntries;	//ordered by refresh time
	protected int resourcesNum;

	
	protected boolean storeMultipleCopies;
	protected int maxResourcesNum;
	protected int maxKeySlotSize;
	protected int maxResourceSlotSize;
	
	
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		this.nodeAccessor = nodeAccessor;
		this.properties = properties;
		
		this.dhtManager = (HyCubeDHTManager) nodeAccessor.getDHTManager();
		
		
		int initialHashTableSize = INITIAL_HASH_TABLE_SIZE;
		this.resources = new HashMap<BigInteger, HashMap<String, HashMap<String, HyCubeResourceEntry>>>(HashMapUtils.getHashMapCapacityForElementsNum(initialHashTableSize, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		
		this.resourceEntries = new LinkedList<HyCubeResourceEntry>();
		
		this.resourcesNum = 0;
		
		try {
			
			storeMultipleCopies = (Boolean) properties.getProperty(PROP_KEY_STORE_MULTIPLE_COPIES, MappedType.BOOLEAN);
			
			maxResourcesNum = (Integer) properties.getProperty(PROP_KEY_MAX_RESOURCES_NUM, MappedType.INT);
			
			maxKeySlotSize = (Integer) properties.getProperty(PROP_KEY_MAX_KEY_SLOT_SIZE, MappedType.INT);
			
			maxResourceSlotSize = (Integer) properties.getProperty(PROP_KEY_MAX_RESOURCE_SLOT_SIZE, MappedType.INT);
			
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize the DHT storage manager instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
		
	}


	@Override
	public Object putToStorage(BigInteger key, NodeId senderNodeId, Object value) {
		return putToStorage(key, senderNodeId, value, null);
	}
	
	@Override
	public Object putToStorage(BigInteger key, NodeId senderNodeId, Object value, Object[] parameters) {
		
		if ( ! (value instanceof HyCubeResource)) throw new IllegalArgumentException("The value is expected to be an instance of: " + HyCubeResource.class.getName());
		long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
		return putToStorage(key, senderNodeId, (HyCubeResource)value, currTime, parameters);

	}
	
	@Override
	public boolean putToStorage(BigInteger key, NodeId senderNodeId, HyCubeResource r, long refreshTime) {
		return putToStorage(key, senderNodeId, r, refreshTime, false, null);
	}
	
	@Override
	public boolean putToStorage(BigInteger key, NodeId senderNodeId, HyCubeResource r, long refreshTime, boolean replication) {
		return putToStorage(key, senderNodeId, r, refreshTime, replication, null);
	}
	
	@Override
	public boolean putToStorage(BigInteger key, NodeId senderNodeId, HyCubeResource r, long refreshTime, Object[] parameters) {
		return putToStorage(key, senderNodeId, r, refreshTime, false, parameters);
	}
	
	@Override
	public boolean putToStorage(BigInteger key, NodeId senderNodeId, HyCubeResource r, long refreshTime, boolean replication, Object[] parameters) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Putting to storage...");
		}
		
		if (! dhtManager.getResourceAccessController().checkPutAccess((HyCubeNodeId) senderNodeId, r.getResourceDescriptor(), replication)) {
			return false;
		}
		
		
		HashMap<String, HashMap<String, HyCubeResourceEntry>> slot;
		HashMap<String, HyCubeResourceEntry> resSlot;
		
		long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
		if (refreshTime > currTime) refreshTime = currTime;
		
		
		HyCubeResourceEntry re = new HyCubeResourceEntry(key, r, refreshTime);
		
		synchronized (resources) {
			
			if (! resources.containsKey(key)) {
				slot = new HashMap<String, HashMap<String, HyCubeResourceEntry>>(HashMapUtils.getHashMapCapacityForElementsNum(INITIAL_HASH_TABLE_SLOT_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
				resources.put(key, slot);
			}
			else {
				slot = resources.get(key);
			}
			
			if (slot.containsKey(r.getResourceDescriptor().getResourceId())) {
				resSlot = slot.get(r.getResourceDescriptor().getResourceId());
			}
			else {
				
				//if the slot doesn't contain the new resource (resource id), check if the limit would not be exceeded
				if (maxKeySlotSize > 0 && slot.size() >= maxKeySlotSize) {
					return false;
				}
				
				resSlot = new HashMap<String, HyCubeResourceEntry>(HashMapUtils.getHashMapCapacityForElementsNum(INITIAL_HASH_TABLE_RES_SLOT_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
				slot.put(r.getResourceDescriptor().getResourceId(), resSlot);
			}
			
			
			
			//previous (if exists) will be removed from the resourceEntries collection
			HyCubeResourceEntry previous = null;
			
			
			String resourceUrl = r.getResourceDescriptor().getResourceUrl();
			if (resourceUrl == null || resourceUrl.isEmpty()) {
				return false;
			}
			
			if (storeMultipleCopies || resSlot.isEmpty()) {
				
				if (! resSlot.containsKey(resourceUrl)) {
					//if resSlot doesn't contain the resource url, check if the limit would not be exceeded
					if (maxResourceSlotSize > 0 && resSlot.size() >= maxResourceSlotSize) {
						return false;
					}
				}
				
				previous = resSlot.remove(resourceUrl);
				
			}
			else {
				//it is assumed here that resSlot contains at most one entry (according to this logic)
				if (resSlot.containsKey(resourceUrl)) {
					//this is the resource from the same node, just replace it
					
					previous = resSlot.remove(resourceUrl);
					
				}
				else {
					//and remove the previous resource entry for that resource id (different resource url):
					Iterator<String> iter = resSlot.keySet().iterator();
					String previousResourceUrl = iter.next();
					
					previous = resSlot.remove(previousResourceUrl);
					
				}
				
			}
			
			
			//remove the previous resource from the resources linked list ordered by the refresh time
			if (previous != null) {
				//resourceEntries.remove(previous);
				
				//mark as deleted and respect that flag
				re.setDeleted(true);
				
				resourcesNum--;
				
			}
			
			
			
			if (maxResourcesNum == 0 || resourcesNum < maxResourcesNum) {
				
				//if the previous entry was replaced, there will always be a free space for the new entry
				
				//save the resource
				resSlot.put(resourceUrl, re);
				
				//insert the resource to the resources linked list ordered by the refresh time
				resourceEntries.addLast(re);
				resourcesNum++;
				
			}
			else {				
				return false;
			}
			
		}
		
		return true;
		
	}

	
	
	
	@Override
	public Object refreshPutToStorage(BigInteger key, NodeId senderNodeId, Object value) {
		return refreshPutToStorage(key, senderNodeId, value, null);
	}
	
	
	@Override
	public Object refreshPutToStorage(BigInteger key, NodeId senderNodeId, Object value, Object[] parameters) {
		if ( ! (value instanceof HyCubeResourceDescriptor)) throw new IllegalArgumentException("The value is expected to be an instance of: " + HyCubeResourceDescriptor.class.getName());
		long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
		return refreshPutToStorage(key, senderNodeId, (HyCubeResourceDescriptor)value, currTime, parameters);

	}
	
	@Override
	public boolean refreshPutToStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor rd, long refreshTime) {
		return refreshPutToStorage(key, senderNodeId, rd, refreshTime, false, null);
	}
	
	@Override
	public boolean refreshPutToStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor rd, long refreshTime, boolean replication) {
		return refreshPutToStorage(key, senderNodeId, rd, refreshTime, replication, null);
	}
	
	@Override
	public boolean refreshPutToStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor rd, long refreshTime, Object[] parameters) {
		return refreshPutToStorage(key, senderNodeId, rd, refreshTime, false, parameters);
	}
	
	@Override
	public boolean refreshPutToStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor rd, long refreshTime, boolean replication, Object[] parameters) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Refreshing put to storage...");
		}
		
		if (! dhtManager.getResourceAccessController().checkRefreshPutAccess((HyCubeNodeId) senderNodeId, rd, replication)) {
			return false;
		}
		
		HashMap<String, HashMap<String, HyCubeResourceEntry>> slot;
		HashMap<String, HyCubeResourceEntry> resSlot;
		
		long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
		if (refreshTime > currTime) refreshTime = currTime;
		
		
		synchronized (resources) {
			
			if (resources.containsKey(key)) {
				slot = resources.get(key);
			}
			else {
				return false;
			}
			
			if (slot.containsKey(rd.getResourceId())) {
				resSlot = slot.get(rd.getResourceId());
			}
			else {
				return false;
			}
			

			HyCubeResourceEntry re = resSlot.get(rd.getResourceUrl());
			
			
			//remove the previous resource from the resources linked list ordered by the refresh time
			if (re != null && refreshTime > re.getRefreshTime()) {
				
				String resourceUrl = rd.getResourceUrl();
				if (resourceUrl == null || resourceUrl.isEmpty()) {
					return false;
				}
				

				//mark as deleted and respect that flag
				re.setDeleted(true);
				
				resSlot.remove(rd.getResourceUrl());

				
				//insert the resource to the resources linked list ordered by the refresh time

				HyCubeResourceEntry reRefreshed = new HyCubeResourceEntry(re.getKey(), re.getResource(), refreshTime);

				resourceEntries.addLast(reRefreshed);
				
				resSlot.put(rd.getResourceUrl(), reRefreshed);
				
				
			}
			
			
			
		}
		
		return true;
		
	}
	
	
	
	
	
	
	@Override
	public Object[] getFromStorage(BigInteger key, NodeId senderNodeId, Object detail) {
		return getFromStorage(key, senderNodeId, detail, null);
	}
	
	@Override
	public Object[] getFromStorage(BigInteger key, NodeId senderNodeId, Object detail, Object[] parameters) {
		if ( ! (detail instanceof HyCubeResourceDescriptor)) throw new IllegalArgumentException("The detail is expected to be an instance of: " + HyCubeResourceDescriptor.class.getName());
		return getFromStorage(key, senderNodeId, (HyCubeResourceDescriptor)detail, parameters);
		
	}
		
	
	@Override
	public HyCubeResourceEntry[] getFromStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor criteria) {
		return getFromStorage(key, senderNodeId, criteria, null);
	}
	
	@Override
	public HyCubeResourceEntry[] getFromStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor criteria, Object[] parameters) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Getting from storage...");
		}
		
		List<HyCubeResourceEntry> res = new ArrayList<HyCubeResourceEntry>();
		
		synchronized (resources) {
		
			HashMap<String, HashMap<String, HyCubeResourceEntry>> slot = resources.get(key);
			if (slot != null) {
				for (HashMap<String, HyCubeResourceEntry> resSlot : slot.values()) {
					for (HyCubeResourceEntry re : resSlot.values()) {
						HyCubeResourceDescriptor rd = re.getResource().getResourceDescriptor();
						if (rd.matches(criteria)) {
							if (dhtManager.getResourceAccessController().checkGetAccess((HyCubeNodeId) senderNodeId, rd)) {
								res.add(re);
							}
						}
					}
				}
			}
			
		}
		
		return res.toArray(new HyCubeResourceEntry[res.size()]);
		
	}

	
	
	@Override
	public Object deleteFromStorage(BigInteger key, NodeId senderNodeId, Object detail) {
		return deleteFromStorage(key, senderNodeId, detail, null);
	}
	
	@Override
	public Object deleteFromStorage(BigInteger key, NodeId senderNodeId, Object detail, Object[] parameters) {
		if ( ! (detail instanceof HyCubeResourceDescriptor)) throw new IllegalArgumentException("The detail is expected to be an instance of: " + HyCubeResourceDescriptor.class.getName());
		return deleteFromStorage(key, senderNodeId, (HyCubeResourceDescriptor)detail, parameters);
	}
	
	
	@Override
	public boolean deleteFromStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor criteria) {
		return deleteFromStorage(key, senderNodeId, criteria, null);
	}
	
	@Override
	public boolean deleteFromStorage(BigInteger key, NodeId senderNodeId, HyCubeResourceDescriptor criteria, Object[] parameters) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("N:" + nodeAccessor.getNodeId().hashCode() + ": " + "Deleting from storage...");
		}
		
		if (! dhtManager.getResourceAccessController().checkDeleteAccess((HyCubeNodeId) senderNodeId, criteria)) {
			return false;
		}
		
		HashMap<String, HashMap<String, HyCubeResourceEntry>> slot;
		HashMap<String, HyCubeResourceEntry> resSlot;
		
		synchronized (resources) {
			
			if (resources.containsKey(key)) {
				slot = resources.get(key);
			}
			else {
				return false;
			}
			
			if (criteria.getResourceId() != null && slot.containsKey(criteria.getResourceId())) {
				resSlot = slot.get(criteria.getResourceId());
			}
			else {
				return false;
			}
			
			String resourceUrl = criteria.getResourceUrl();
			
			if (resourceUrl != null && (! resourceUrl.isEmpty())) {
			
				HyCubeResourceEntry re = resSlot.get(criteria.getResourceUrl());
				
				if (re != null && re.getResource().getResourceDescriptor().matches(criteria)) {
				
					//remove the resource from the resources linked list ordered by the refresh time
					
					//set deleted and respect that flag
					re.setDeleted(true);
					
					resSlot.remove(resourceUrl);
					
					resourcesNum--;
					
					
					if (resSlot.isEmpty()) {
						slot.remove(re.getResource().getResourceDescriptor().getResourceId());
					}
					if (slot.isEmpty()) {
						resources.remove(re.getKey());
					}
				
					return true;
				
				}
				else return false;
			}
			else return false;
			
			
		}
		
		
	}
	
	
	public void discardOutdatedEntries(long discardTime) {
		
		synchronized (resources) {
			ListIterator<HyCubeResourceEntry> iter = resourceEntries.listIterator();
			while (iter.hasNext()) {
				HyCubeResourceEntry re = iter.next();
				if (re.isDeleted()) {
					//remove the item - was marked as deleted
					iter.remove();
				}
				else if (re.getRefreshTime() <= discardTime) {
					//discard
					iter.remove();
					resourcesNum--;


					HashMap<String, HashMap<String, HyCubeResourceEntry>> slot;
					HashMap<String, HyCubeResourceEntry> resSlot;
					
					slot = resources.get(re.getKey());
					resSlot = slot.get(re.getResource().getResourceDescriptor().getResourceId());
					resSlot.remove(re.getResource().getResourceDescriptor().getResourceUrl());
					
					if (resSlot.isEmpty()) {
						slot.remove(re.getResource().getResourceDescriptor().getResourceId());
					}
					if (slot.isEmpty()) {
						resources.remove(re.getKey());
					}
				}
				else break;
			}
			
		}
		
	}
	
	
	
	
	@Override
	public Map<BigInteger, HyCubeResourceReplicationEntry[]> getResourcesInfoForReplication() {
		
		synchronized (resources) {

			Map<BigInteger, HyCubeResourceReplicationEntry[]> resourceInfos = new HashMap<BigInteger, HyCubeResourceReplicationEntry[]>(HashMapUtils.getHashMapCapacityForElementsNum(resources.keySet().size(), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
			
			for (BigInteger key : resources.keySet()) {
				
				LinkedList<HyCubeResourceEntry> resEntriesForKey = new LinkedList<HyCubeResourceEntry>();
				
				HashMap<String, HashMap<String, HyCubeResourceEntry>> keyResources = resources.get(key);
				for (HashMap<String, HyCubeResourceEntry> resIdResources : keyResources.values()) {
					for (HyCubeResourceEntry res : resIdResources.values()) {
						resEntriesForKey.add(res);
					}
				}
				
				ArrayList<HyCubeResourceReplicationEntry> replicationInfo = new ArrayList<HyCubeResourceReplicationEntry>(resEntriesForKey.size());
				for (HyCubeResourceEntry resourceEntry : resEntriesForKey) {
					if (! resourceEntry.isDeleted()) {
						HyCubeResourceReplicationEntry repEntry = new HyCubeResourceReplicationEntry(resourceEntry.getKey(), resourceEntry.getResource().getResourceDescriptor(), resourceEntry.getRefreshTime());
						replicationInfo.add(repEntry);
					}
				}
				
				resourceInfos.put(key, replicationInfo.toArray(new HyCubeResourceReplicationEntry[replicationInfo.size()]));
				
			}
		
			return resourceInfos;
			
		}
		
		
		
	}
	
	
	
}
