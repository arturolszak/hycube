package net.hycube.dht;

import net.hycube.core.UnrecoverableRuntimeException;

public abstract class HyCubeGetCallback implements GetCallback {

	@Override
	public void getReturned(Object callbackArg, Object getResult) {
		if ( ! (getResult instanceof HyCubeResource[])) {
			throw new UnrecoverableRuntimeException("The type of the result is expected to be an array of type: " + HyCubeResource.class.getName());
		}
		getReturned(callbackArg, (HyCubeResource[])getResult);
	}

	
	public abstract void getReturned(Object callbackArg, HyCubeResource[] getResult);
	
	
}
