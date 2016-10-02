package net.hycube.transport;

import java.io.Serializable;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import net.hycube.core.UnrecoverableRuntimeException;


public class UDPNodePointer implements NetworkNodePointer, Serializable {

	
	private static final long serialVersionUID = 8026899277042726579L;
	
	
	public static final int IP4_ADDRESS_BYTE_LENGTH = 4;
	public static final int PORT_BYTE_LENGTH = 4;
	public static final int ADDRESS_BYTE_LENGTH = IP4_ADDRESS_BYTE_LENGTH + PORT_BYTE_LENGTH;
	
	
	
	public UDPNodePointer(String address) {
		
		this.addressString = address;
		
		inetSocketAddress = validateNetworkAddress(address); 
		if (inetSocketAddress == null) {
			throw new IllegalArgumentException("The input address string is invalid.");
		}
		
		ip = inetSocketAddress.getAddress().getHostAddress();
		port = inetSocketAddress.getPort();
		
		
		try {
			ByteBuffer b = ByteBuffer.allocate(ADDRESS_BYTE_LENGTH);
			b.order(ByteOrder.BIG_ENDIAN);
			
			b.put(inetSocketAddress.getAddress().getAddress());
			b.putInt(inetSocketAddress.getPort());
			
			addressBytes = b.array();
		}
		catch (Exception e) {
			throw new UnrecoverableRuntimeException("An error occured while converting the address to its byte representation.", e);
		}
		
	}
	
	public UDPNodePointer(byte[] addressBytes) {
		
		inetSocketAddress = validateNetworkAddress(addressBytes);
		if (inetSocketAddress == null) {
			throw new IllegalArgumentException("The input address string is invalid.");
		}
		
		ip = inetSocketAddress.getAddress().getHostAddress();
		port = inetSocketAddress.getPort();
		
		String portString = Integer.toString(port);
		
		addressString = new StringBuilder(ip.length() + portString.length() + 1).append(ip).append(":").append(portString).toString();
		
		this.addressBytes = addressBytes;
		
		
	}
	
	
	
	protected String ip;
	protected int port;
	protected InetSocketAddress inetSocketAddress;
	protected String addressString;
	protected byte[] addressBytes;
	
	@Override
	public String getAddressString() {
		return addressString;
	}
	
	
	@Override
	public byte[] getAddressBytes() {
		return addressBytes; 
	}
	
	
	@Override
	public int getByteLength() {
		return UDPNodePointer.getAddressByteLength();
	}
	
	
	public static int getAddressByteLength() {
		return ADDRESS_BYTE_LENGTH;
	}
	
	
	public String getIP() {
		return ip;
	}

	public int getPort() {
		return port;
	}
	
	public InetSocketAddress getInetSocketAddress() {
		return inetSocketAddress;
	}
	
	public static InetSocketAddress validateNetworkAddress(String networkAddress) {
		InetSocketAddress isa = null;
		try {
			String[] addrSplit = networkAddress.split(":");
			isa = new InetSocketAddress(addrSplit[0], Integer.parseInt(addrSplit[1]));
			if (isa.getAddress().getHostAddress() == null || isa.getAddress().getHostAddress().isEmpty()) {
				return null;
			}
			if ( ! (isa.getAddress() instanceof Inet4Address)) {
				//it may be also ip6
				//as we want the constant IP address byte length of 4 bytes, only IP4 is supported by this NodePointer class
				return null;
			}
		}
		catch (Exception e) {
			return null;
		}
		return isa;
	}
	
	public static InetSocketAddress validateNetworkAddress(String address, int port) {
		InetSocketAddress isa = null;
		try {
			isa = new InetSocketAddress(address, port);
			if (isa.getAddress().getHostAddress() == null || isa.getAddress().getHostAddress().isEmpty()) {
				return null;
			}
			if ( ! (isa.getAddress() instanceof Inet4Address)) {
				//it may be also ip6
				//as we want the constant IP address byte length of 4 bytes, only IP4 is supported by this NodePointer class
				return null;
			}
		}
		catch (Exception e) {
			return null;
		}
		return isa;
	}
	
	
	public static InetSocketAddress validateNetworkAddress(byte[] addressBytes) {
		
		ByteBuffer b = ByteBuffer.wrap(addressBytes);
		b.order(ByteOrder.BIG_ENDIAN);
		
		byte[] address = new byte[IP4_ADDRESS_BYTE_LENGTH];
		b.get(address);
		int port = b.getInt();
		
		InetSocketAddress isa = null;
		try {
			InetAddress ia = InetAddress.getByAddress(address);
			if ( ! (ia instanceof Inet4Address)) {
				//it may be also ip6
				//as we want the constant IP address byte length of 4 bytes, only IP4 is supported by this NodePointer class
				return null;
			}
			isa = new InetSocketAddress(ia, port);
			if (isa.getAddress().getHostAddress() == null || isa.getAddress().getHostAddress().isEmpty()) {
				return null;
			}
		}
		catch (Exception e) {
			return null;
		}
		return isa;
	}
	
	public static InetSocketAddress validateNetworkAddress(byte[] address, int port) {
		InetSocketAddress isa = null;
		try {
			InetAddress ia = InetAddress.getByAddress(address);
			if ( ! (ia instanceof Inet4Address)) {
				//it may be also ip6
				//as we want the constant IP address byte length of 4 bytes, only IP4 is supported by this NodePointer class
				return null;
			}
			isa = new InetSocketAddress(ia, port);
			if (isa.getAddress().getHostAddress() == null || isa.getAddress().getHostAddress().isEmpty()) {
				return null;
			}
		}
		catch (Exception e) {
			return null;
		}
		return isa;
	}


	
}
