package net.hycube.transport;

import java.io.Serializable;

public interface NetworkNodePointer extends Serializable {

	public String getAddressString();
	
	public byte[] getAddressBytes();
	
	public int getByteLength();
	
	
	
}
