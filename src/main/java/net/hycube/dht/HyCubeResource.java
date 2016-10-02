package net.hycube.dht;

import java.io.Serializable;


public class HyCubeResource implements Serializable {
	
	
	private static final long serialVersionUID = 7536770097402193946L;
	
	
	protected HyCubeResourceDescriptor resourceDescriptor;
	protected byte[] data;
	
	
	
	public HyCubeResourceDescriptor getResourceDescriptor() {
		return resourceDescriptor;
	}

	public void setResourceDescriptor(HyCubeResourceDescriptor rd) {
		this.resourceDescriptor = rd;
	}
	

	public byte[] getData() {
		return data;
	}


	public void setData(byte[] data) {
		this.data = data;
	}


	
	
	protected HyCubeResource() {
		
	}
	
	
	public HyCubeResource(HyCubeResourceDescriptor rd) {
		this(rd, null);
	}
	
	public HyCubeResource(String resourceDescriptorString) {
		this(resourceDescriptorString, null);
	}
	
	
	public HyCubeResource(HyCubeResourceDescriptor rd, byte[] data) {
		this.resourceDescriptor = rd;
		this.data = data;
		
	}

	
	public HyCubeResource(String resourceDescriptorString, byte[] data) {
		this.resourceDescriptor = new HyCubeResourceDescriptor(resourceDescriptorString);
		this.data = data;
		
	}
	

	
	@Override
	public String toString() {
		return resourceDescriptor.getDescriptorString();
		
		
	}
	
	

	
}
