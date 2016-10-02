package net.hycube.messaging.fragmentation;

import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.environment.NodeProperties;
import net.hycube.messaging.messages.Message;

public interface MessageFragmenter {

	public void initialize(NodeAccessor nodeAccessor, NodeProperties nodeProperties) throws InitializationException;
	
	public int getFragmentLength();
	public int getMaxFragmentsCount();
	
	public Message[] fragmentMessage(Message msg) throws MessageFragmentationException;
	public Message reassemblyMessage(Message msg) throws MessageFragmentationException;
	
	
}
