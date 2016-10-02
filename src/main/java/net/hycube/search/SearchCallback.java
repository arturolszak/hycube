package net.hycube.search;

import net.hycube.core.NodePointer;

public interface SearchCallback {

	public void searchReturned(int searchId, Object callbackArg, NodePointer[] result);
	
	
}
