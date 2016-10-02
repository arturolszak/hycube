package net.hycube.backgroundprocessing;

import net.hycube.eventprocessing.Event;
import net.hycube.eventprocessing.EventProcessException;
import net.hycube.eventprocessing.ProcessEventProxy;


public class BackgroundProcessEventProxy implements ProcessEventProxy {

	public static enum BackgroundProcessEventProxyOperation {
		SCHEDULE,
		PROCESS,
	}

	
	protected BackgroundProcess bkgProcess;
	
	
	
	public BackgroundProcessEventProxy(BackgroundProcess bkgProcess) {
		this.bkgProcess = bkgProcess;
		
	}
	
	
	@Override
	public void processEvent(Event event) throws EventProcessException {
		if (!(event.getEventArg() instanceof BackgroundProcessEventProxyOperation)) {
			throw new EventProcessException("Error while processing the event. The event argument is expected to be an instance of " + BackgroundProcessEventProxyOperation.class.getName());
		}
		
		BackgroundProcessEventProxyOperation operation = (BackgroundProcessEventProxyOperation)event.getEventArg();
		switch (operation) {
			case PROCESS:
				bkgProcess.process();
				break;
			case SCHEDULE:
				bkgProcess.schedule();
				break;
		};
		
		
		
	};
	
	
}
