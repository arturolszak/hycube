package net.hycube.dht;

import net.hycube.core.UnrecoverableRuntimeException;

public abstract class HyCubeRefreshPutCallback implements RefreshPutCallback {

	@Override
	public void refreshPutReturned(Object callbackArg, Object refreshPutResult) {
		if ( ! (refreshPutResult instanceof Boolean)) {
			throw new UnrecoverableRuntimeException("The type of the result is expected to be an array of type: " + Boolean.class.getName());
		}
		refreshPutReturned(callbackArg, (Boolean)refreshPutResult);
	}

	
	public abstract void refreshPutReturned(Object callbackArg, Boolean refreshPutResult);
	
	
}
