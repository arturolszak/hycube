package net.hycube.pastry.routing;

import net.hycube.core.NodePointer;
import net.hycube.logging.LogHelper;
import net.hycube.messaging.messages.HyCubeMessage;
import net.hycube.pastry.nexthopselection.PastryNextHopSelectionParameters;
import net.hycube.routing.HyCubeRoutingManager;

public class PastryRoutingManager extends HyCubeRoutingManager {

	
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(PastryRoutingManager.class);
//	private static org.apache.commons.logging.Log msgLog = LogHelper.getMessagesLog();
	


	@Override
	public NodePointer findNextHop(HyCubeMessage msg) {
		
		if (devLog.isTraceEnabled()) {
			devLog.trace("Finding next hop for message #" + msg.getSerialNoAndSenderString());
		}
		
		PastryNextHopSelectionParameters parameters = new PastryNextHopSelectionParameters();
		parameters.setPMHApplied(msg.isPMHApplied());
		parameters.setSkipRandomNumOfNodesApplied(msg.isSkipRandomNumOfNodesApplied());
		parameters.setSecureRoutingApplied(msg.isSecureRoutingApplied());
		parameters.setIncludeMoreDistantNodes(false);
		
		NodePointer res = nextHopSelector.findNextHop(msg.getRecipientId(), parameters);
		
		msg.setPMHApplied(parameters.isPMHApplied());
		msg.setSkipRandomNumOfNodesApplied(parameters.isSkipRandomNumOfNodesApplied());
		msg.setSecureRoutingApplied(parameters.isSecureRoutingApplied());
		
		return res;
		
		
	}
	
	
}
