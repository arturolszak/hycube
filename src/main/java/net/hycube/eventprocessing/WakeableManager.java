package net.hycube.eventprocessing;

import java.util.concurrent.locks.Lock;

public interface WakeableManager {

	//returns the lock object used for synchronizing calls of addWakeable, removeWakeable, getWakeables and wakeup
	public Lock getWakeableManagerLock();

	//registers the wakeable object
	public boolean addWakeable(Wakeable wakeable);

	//should be called when a wakeable object is no longer going to be wakeable
	public boolean removeWakeable(Wakeable wakeable);

	//gets maximum sleep timeout for the wakeable objects (useful if the wakeable manager manages synchronized tasks and the wakeables should wake up automatically before the scheduled task execution time)
	public int getNextMaxSleepTime();
	
	public void discard();

	public void wakeup();
	
	

}