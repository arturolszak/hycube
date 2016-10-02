package net.hycube.eventprocessing;

public interface ProcessEventProxy {

	public void processEvent(Event event) throws EventProcessException;
			

}
