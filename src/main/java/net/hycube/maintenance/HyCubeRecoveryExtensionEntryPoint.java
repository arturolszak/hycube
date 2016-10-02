package net.hycube.maintenance;

import net.hycube.common.EntryPoint;


public interface HyCubeRecoveryExtensionEntryPoint extends EntryPoint {

	public void callRecover();
	
	public void callRecoverNS();
	
	
}
