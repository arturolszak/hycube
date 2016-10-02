package net.hycube.messaging.fragmentation;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.ListIterator;

import net.hycube.configuration.GlobalConstants;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.environment.NodeProperties;
import net.hycube.logging.LogHelper;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.messaging.messages.Message;
import net.hycube.utils.HashMapUtils;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public class HyCubeMessageFragmenter implements MessageFragmenter {

	
	protected static final int INITIAL_INITIAL_MESSAGES_COLLECTION_SIZE = GlobalConstants.DEFAULT_INITIAL_COLLECTION_SIZE;
	
	protected static final int HEADER_EXTENSION_DATA_LENGTH = 2 * Short.SIZE / 8;
	
	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeMessageFragmenter.class); 
	
	
	
	protected static class ReceivedMessageAssembly {
		int fragmentsNum;
		HyCubeMessage[] fragments;
		long firstFragmentReceivedTimestamp;
		long nodeIdHash;
		long msgSerialNo;
		String key;
		
		//prevent removing it by the purge procedure from the assemblies map if was already processed - a new object might have been added already with the same key
		boolean removed;
		
	}
	
	
	
	protected final static String PROP_KEY_HEADER_EXTENSION_INDEX = "HeaderExtensionIndex";
	protected final static String PROP_KEY_FRAGMENT_LENGTH = "FragmentLength";
	protected final static String PROP_KEY_FRAGMENT_RETENTION_PERIOD = "FragmentsRetentionPeriod";
	protected final static String PROP_KEY_PREVENT_FRAGMENT_DUPLICATES = "PreventFragmentDuplicates";
	
	
		
	protected NodeAccessor nodeAccessor;
	protected NodeProperties properties;
	
	
	protected LinkedList<ReceivedMessageAssembly> assemblies;
	protected HashMap<String, ReceivedMessageAssembly> assembliesMap;
	
	
	protected int headerExtensionIndex;
	protected int fragmentLength;
	protected int fragmentsRetentionPeriod;
	protected boolean preventFragmentDuplicates;
	
	
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		this.nodeAccessor = nodeAccessor;
		this.properties = properties;
		
		try {
			
			this.headerExtensionIndex = (Integer) properties.getProperty(PROP_KEY_HEADER_EXTENSION_INDEX, MappedType.INT);
			
			this.fragmentLength = (Integer) properties.getProperty(PROP_KEY_FRAGMENT_LENGTH, MappedType.INT);
			
			this.fragmentsRetentionPeriod = (Integer) properties.getProperty(PROP_KEY_FRAGMENT_RETENTION_PERIOD, MappedType.INT);
			
			this.preventFragmentDuplicates = (Boolean) properties.getProperty(PROP_KEY_PREVENT_FRAGMENT_DUPLICATES, MappedType.BOOLEAN);
			
			
			
		}
		catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, e.getKey(), "An exception was thrown while initializing the network adapter. Invalid parameter value: " + e.getKey());
		}
		
		
		
		assemblies = new LinkedList<HyCubeMessageFragmenter.ReceivedMessageAssembly>();
		assembliesMap = new HashMap<String, ReceivedMessageAssembly>(HashMapUtils.getHashMapCapacityForElementsNum(INITIAL_INITIAL_MESSAGES_COLLECTION_SIZE, GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR), GlobalConstants.DEFAULT_HASH_MAP_LOAD_FACTOR);
		
		
		
	}
	
	
	
	@Override
	public int getFragmentLength() {
		return fragmentLength;
	}
	
	
	@Override
	public int getMaxFragmentsCount() {
		return Short.MAX_VALUE;
	}
	
	

	@Override
	public Message[] fragmentMessage(Message msg) throws MessageFragmentationException {
		
		if (msg.getByteLength() <= fragmentLength) return new Message[] {msg};
		
		if (!(msg instanceof HyCubeMessage)) {
			throw new MessageFragmentationRuntimeException("The message object is expected to be an instance of: " + HyCubeMessage.class.getName());
		}
		HyCubeMessage hmsg = (HyCubeMessage)msg;
		
		
		int headerLength = hmsg.getHeaderLength();
		
		if (headerLength >= fragmentLength) {
			throw new MessageFragmentationException("The message cannot be fragmented. The header length is greater than the fragment length.");
		}
		
		
		byte[] data = hmsg.getData();
		int dataLength = hmsg.getData().length;
		
		//if (dataLength == 0)	//skip this case, impossible when msg.getByteLength() > fragmentLength and headerLength < fragmentLength
		
		int dataFragmentLength = fragmentLength - headerLength;
		int fragmentsNum = dataLength / dataFragmentLength;
		if (dataLength % dataFragmentLength != 0) fragmentsNum += 1;
		
		if (fragmentsNum > Short.MAX_VALUE) {
			throw new MessageFragmentationException("The message cannot be fragmented. The number of the fragments would exceed the maximum value " + Short.MAX_VALUE);
		}
		
		byte[][] dataFragments = new byte[fragmentsNum][];
		HyCubeMessage[] fragments = new HyCubeMessage[fragmentsNum];
		
		for (int i = 0; i < fragmentsNum; i++) {
			int startIndex = i * dataFragmentLength;
			int endIndex = Math.min((i + 1) * dataFragmentLength, dataLength);
			dataFragments[i] = Arrays.copyOfRange(data, startIndex, endIndex);
			fragments[i] = hmsg.clone();
			fragments[i].setCRC32(hmsg.getCRC32());
			fragments[i].setData(dataFragments[i]);
			
			//set the fragmentation information in the message header
			
			ByteBuffer b = ByteBuffer.allocate(HEADER_EXTENSION_DATA_LENGTH);
			b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
			b.putShort((short) i);
			b.putShort((short) fragmentsNum);
			fragments[i].setHeaderExtension(headerExtensionIndex, b.array());
			
			
		}
		
		return fragments;
		
	}

	
	
	@Override
	public Message reassemblyMessage(Message message) throws MessageFragmentationException {
		
		HyCubeMessage msg = (HyCubeMessage)message;
		
		boolean complete = true;
		int totalDataLength = 0; 
		ReceivedMessageAssembly rma;
		
		try {
		
			synchronized (assemblies) {
				
				long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
				long senderIdHash = msg.getSenderId().calculateHash();
				String strIdHash = Long.toHexString(senderIdHash);
				String strMsgSerialNo = Integer.toHexString(msg.getSerialNo());
				String strRouteId = Integer.toHexString(msg.getRouteId());
				String str = new StringBuilder(strIdHash.length() + strMsgSerialNo.length() + strRouteId.length() + 2).append(strIdHash).append('.').append(strMsgSerialNo).append('.').append(strRouteId).toString();
	
				purgeRecentMessages(currTime);
				
				//process the received fragment
				
				if (!(msg instanceof HyCubeMessage)) {
					throw new MessageFragmentationRuntimeException("The message object is expected to be an instance of: " + HyCubeMessage.class.getName());
				}
				HyCubeMessage hmsg = (HyCubeMessage)msg;
	
				byte[] bytes = hmsg.getHeaderExtension(headerExtensionIndex);			
				
				if (bytes == null || bytes.length == 0) {
					//the message is not fragmented
					return msg;
				}
				
				ByteBuffer b = ByteBuffer.wrap(bytes);
				b.order(HyCubeMessage.MESSAGE_BYTE_ORDER);
				
				int fragmentIndex = b.getShort();
				int totalFragmentsNum = b.getShort();
				
				if (totalFragmentsNum == 0) {
					//the message is not fragmented
					return msg;
				}
				
				if (totalFragmentsNum > 0 && totalFragmentsNum <= fragmentIndex) {
					if (devLog.isDebugEnabled()) {
						devLog.debug("Discarding message #" + msg.getSerialNoAndSenderString() + ". The message is corrupted. The fragment index is greater or equel the total number of fragments.");
					}
					if (msgLog.isInfoEnabled()) {
						msgLog.info("Discarding message #" + msg.getSerialNoAndSenderString() + ". The message is corrupted. The fragment index is greater or equel the total number of fragments.");
					}
					return null;
				}
				
				
				if (assembliesMap.containsKey(str)) {
					rma = assembliesMap.get(str);
					
					if (rma.fragmentsNum != totalFragmentsNum) {
						if (devLog.isDebugEnabled()) {
							devLog.debug("Discarding message #" + msg.getSerialNoAndSenderString() + ". The total number of fragments doesn't match the previously received fragments.");
						}
						if (msgLog.isInfoEnabled()) {
							msgLog.info("Discarding message #" + msg.getSerialNoAndSenderString() + ". The total number of fragments doesn't match the previously received fragments.");
						}
						return null;
					}
					
					if (rma.fragments[fragmentIndex] == null) {
						//this fragment has not been yet received
						rma.fragments[fragmentIndex] = hmsg;
					}
					else {
						//this fragment has already been received -> it is a duplicate
						if (preventFragmentDuplicates) {
							if (devLog.isDebugEnabled()) {
								devLog.debug("Discarding message #" + msg.getSerialNoAndSenderString() + ". The message is a duplicate.");
							}
							if (msgLog.isInfoEnabled()) {
								msgLog.info("Discarding message #" + msg.getSerialNoAndSenderString() + ". The message is a duplicate.");
							}
							return null;
						}
						else {
							//replace the previous fragment with the new one:
							rma.fragments[fragmentIndex] = hmsg;
						}
					}
					
					
					
				}
				else {
					
					rma = new ReceivedMessageAssembly();
					rma.key = str;
					rma.fragmentsNum = totalFragmentsNum;
					rma.fragments = new HyCubeMessage[totalFragmentsNum];
					rma.nodeIdHash = senderIdHash;
					rma.msgSerialNo = msg.getSerialNo();
					rma.firstFragmentReceivedTimestamp = currTime;
					rma.fragments[fragmentIndex] = hmsg;
					
					//add new ReceivedMessageAssembly to the assembly collections
					assemblies.add(rma);
					assembliesMap.put(str, rma);
					
				}
					
				
				//check if the complete message was received:
				for (int i = 0; i < rma.fragmentsNum; i++) {
					if (rma.fragments[i] != null) {
						if (rma.fragments[i].getData() != null) {
							totalDataLength += rma.fragments[i].getData().length;
						}
					}
					else {
						complete = false;
						break;
					}
				}
				if (complete) {
					//remove the message from assemblies collection
					if (preventFragmentDuplicates) {
						//do nothing, don't remove to prevent receiving duplicates
					}
					else {
						rma.removed = true;	//will not be processed by purge (so don't have to remove the object from the linked list in linear time)
						assembliesMap.remove(str);
					}
	
				}
			}
			if (complete) {
				
				if (devLog.isDebugEnabled()) {
					devLog.debug("Completed assembling message.");
				}
				
				ByteBuffer assembledDataBuffer = ByteBuffer.allocate(totalDataLength);
	
				for (int i = 0; i < rma.fragmentsNum; i++) {
					assembledDataBuffer.put(rma.fragments[i].getData());
				}
				
				//create the assembled message based on the first (index 0) fragment
				HyCubeMessage assembledMessage = rma.fragments[0];
	
				assembledMessage.setData(assembledDataBuffer.array());
					
				//remove the fragmentation information:
				assembledMessage.setHeaderExtension(headerExtensionIndex, null);
				
				return assembledMessage;
				
			}
		}
		catch (RuntimeException e) {
			throw new MessageFragmentationRuntimeException("An exception has been thrown while reassembling a message.", e);
		}

		return null;
		
		

	}

	
	


		


	protected void purgeRecentMessages(long time) {
		
		ListIterator<ReceivedMessageAssembly> iter = assemblies.listIterator();
		while (iter.hasNext()) {
			ReceivedMessageAssembly rma = iter.next();
			if (time >= rma.firstFragmentReceivedTimestamp + fragmentsRetentionPeriod) {

				iter.remove();
				
				if (rma.removed) {
					//don't process already removed ReceivedMessageAssembly as there might be another ReceivedMessageAssembly with the same key added to the hash table
					continue;
				}
					
				assembliesMap.remove(rma.key);
				
			}
			else {
				//the list is sorted by time -> no more elements have to be checked
				break;
			}
		}		
		
	}
	
	
	
	
}
