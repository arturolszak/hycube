package net.hycube.dht;

import net.hycube.core.UnrecoverableRuntimeException;

public abstract class HyCubePutCallback implements PutCallback {

	@Override
	public void putReturned(Object callbackArg, Object putResult) {
		if ( ! (putResult instanceof Boolean)) {
			throw new UnrecoverableRuntimeException("The type of the result is expected to be an array of type: " + Boolean.class.getName());
		}
		putReturned(callbackArg, (Boolean)putResult);
	}

	
	public abstract void putReturned(Object callbackArg, Boolean putResult);
	
	
}
