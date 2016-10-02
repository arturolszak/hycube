package net.hycube.eventprocessing;

public interface EventQueueProcessor {

	//public void initialize(BlockingQueue<Event> queue);
	
	public boolean isInitialized();
	
	public boolean isRunning();
	
	public boolean isPaused();

	public void start();

	public void stop();

	public void pause();

	public void resume();

	public void clear();

	
}