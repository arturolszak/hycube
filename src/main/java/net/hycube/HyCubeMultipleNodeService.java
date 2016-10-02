package net.hycube;

import net.hycube.core.InitializationException;
import net.hycube.core.NodeId;
import net.hycube.join.JoinCallback;
import net.hycube.transport.MessageReceiver;

public interface HyCubeMultipleNodeService extends MultipleNodeService {

	public abstract HyCubeNodeService initializeNode(String networkAddress,
			String bootstrapNodeAddress, JoinCallback joinCallback,
			Object callbackArg, MessageReceiver messageReceiver)
			throws InitializationException;

	public abstract HyCubeNodeService initializeNode(NodeId nodeId,
			String networkAddress, String bootstrapNodeAddress,
			JoinCallback joinCallback, Object callbackArg,
			MessageReceiver messageReceiver) throws InitializationException;

	public abstract HyCubeNodeService initializeNode(String nodeIdString,
			String networkAddress, String bootstrapNodeAddress,
			JoinCallback joinCallback, Object callbackArg,
			MessageReceiver messageReceiver) throws InitializationException;

	public abstract HyCubeNodeService initializeNode(NodeId nodeId,
			String nodeIdString, String networkAddress,
			String bootstrapNodeAddress, JoinCallback joinCallback,
			Object callbackArg, MessageReceiver messageReceiver)
			throws InitializationException;
	
	
	
}