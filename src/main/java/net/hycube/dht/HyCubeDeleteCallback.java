package net.hycube.dht;

import net.hycube.core.UnrecoverableRuntimeException;

public abstract class HyCubeDeleteCallback implements DeleteCallback {

	@Override
	public void deleteReturned(Object callbackArg, Object deleteResult) {
		if ( ! (deleteResult instanceof Boolean)) {
			throw new UnrecoverableRuntimeException("The type of the result is expected to be an array of type: " + Boolean.class.getName());
		}
		deleteReturned(callbackArg, (Boolean)deleteResult);
	}

	
	public abstract void deleteReturned(Object callbackArg, Boolean deleteResult);
	
	
}
