/**
 * 
 */
package net.hycube.transport;

import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.environment.NodeProperties;
import net.hycube.messaging.messages.Message;

/**
 * @author Artur Olszak
 *
 */
public interface NetworkAdapter {

	public boolean isInitialized();
	
	
	public String getInterfaceAddressString();
	
	public byte[] getInterfaceAddressBytes();
	
	public NetworkNodePointer getInterfaceNetworkNodePointer();
	
	
	public String getPublicAddressString();
	
	public byte[] getPublicAddressBytes();
	
	public NetworkNodePointer getPublicNetworkNodePointer();
	
	
	public void setPublicAddress(String addressString);
	
	public void setPublicAddress(byte[] addressBytes);
	
	public void setPublicAddress(NetworkNodePointer networkNodePointer);

	
	
	
	public void initialize(String networkAddress, ReceivedMessageProcessProxy msgReceivedProxy, NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException;
	
	public void discard() throws NetworkAdapterException;
	
	public void sendMessage(Message msg, NetworkNodePointer nodePointer) throws NetworkAdapterException;
	
	public void messageReceived(Message msg, NetworkNodePointer directSender);
	

	
	public int getMaxMessageLength();
	
	public boolean isFragmentMessages();
	
	public int getMessageFragmentLength();
	
	public int getMaxMassageFragmentsCount();
	
	

	public NetworkNodePointer createNetworkNodePointer(String addressString);
	
	public NetworkNodePointer createNetworkNodePointer(byte[] addressBytes);
	
	
	public Object validateNetworkAddress(String networkAddress);
	
	public Object validateNetworkAddress(byte[] networkAddressBytes);
	

	public int getAddressByteLength();
	
	
	
	
	
	public long getProximity(NetworkNodePointer np);
	

}
