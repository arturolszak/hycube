package net.hycube.pastry.nexthopselection;

import net.hycube.nexthopselection.NextHopSelectionParameters;

public class PastryNextHopSelectionParameters implements NextHopSelectionParameters {
	
	protected boolean includeMoreDistantNodes;
	protected boolean skipTargetNode;
	protected boolean includeSelf;
	
	protected boolean pmhApplied;
	protected boolean preventPmh;
	
	protected boolean skipRandomNumOfNodesApplied;
	protected boolean secureRoutingApplied;

	
	
	public boolean isPMHApplied() {
		return pmhApplied;
	}

	public void setPMHApplied(boolean pmhApplied) {
		this.pmhApplied = pmhApplied;
	}
	
	public boolean isPreventPMH() {
		return preventPmh;
	}

	public void setPreventPMH(boolean preventPmh) {
		this.preventPmh = preventPmh;
	}

	public boolean isIncludeMoreDistantNodes() {
		return includeMoreDistantNodes;
	}

	public void setIncludeMoreDistantNodes(boolean includeMoreDistantNodes) {
		this.includeMoreDistantNodes = includeMoreDistantNodes;
	}
	
	public boolean isSkipTargetNode() {
		return skipTargetNode;
	}

	public void setSkipTargetNode(boolean skipTargetNode) {
		this.skipTargetNode = skipTargetNode;
	}
	
	public boolean isIncludeSelf() {
		return includeSelf;
	}

	public void setIncludeSelf(boolean includeSelf) {
		this.includeSelf = includeSelf;
	}

	public boolean isSkipRandomNumOfNodesApplied() {
		return skipRandomNumOfNodesApplied;
	}

	public void setSkipRandomNumOfNodesApplied(boolean skipRandomNumOfNodesApplied) {
		this.skipRandomNumOfNodesApplied = skipRandomNumOfNodesApplied;
	}
	
	public boolean isSecureRoutingApplied() {
		return secureRoutingApplied;
	}

	public void setSecureRoutingApplied(boolean secureRoutingApplied) {
		this.secureRoutingApplied = secureRoutingApplied;
	}
	
}
