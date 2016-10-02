package net.hycube.lookup;

import net.hycube.core.NodePointer;

public interface LookupCallback {

	public void lookupReturned(int lookupId, Object callbackArg, NodePointer result);
	
	
}
