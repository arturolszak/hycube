package net.hycube.eventprocessing;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.hycube.logging.LogHelper;

public class EventQueueWakeableManager implements WakeableManager {

	public static final int WAKEABLE_SLEEP_TIMEOUT = 1000;
	
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(EventQueueWakeableManager.class);
	
	protected LinkedList<Wakeable> wakeables;
	protected HashMap<Wakeable, Integer> nonWakeablesAfter;
	protected Lock wakeableManagerLock;
	protected int availableSlots;
	
	public EventQueueWakeableManager(int availableSlots) {
		this(null, availableSlots);
	}
	
	public EventQueueWakeableManager(Lock wakeableManagerLock, int availableSlots) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Creating wakeable manager with " + availableSlots + " available slots. The lock object is: " + wakeableManagerLock);
		}
		
		this.wakeables = new LinkedList<Wakeable>();
		this.nonWakeablesAfter = new HashMap<Wakeable, Integer>();
		this.availableSlots = availableSlots;
		
		if (wakeableManagerLock != null) {
			this.wakeableManagerLock = wakeableManagerLock;
		}
		else {
			this.wakeableManagerLock = new ReentrantLock(true);
		}
	}
	
	//returns the lock object used for synchronizing calls of addWakeable, removeWakeable, getWakeables and wakeup
	/* (non-Javadoc)
	 * @see net.hycube.eventprocessing.WakeableManager#getWakeableManagerLock()
	 */
	@Override
	public Lock getWakeableManagerLock() {
		return wakeableManagerLock;
	}
	
	//registers the wakeable object
	/* (non-Javadoc)
	 * @see net.hycube.eventprocessing.WakeableManager#addWakeable(net.hycube.eventprocessing.Wakeable)
	 */
	@Override
	public boolean addWakeable(Wakeable wakeable) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Registering Wakeable object: " + wakeable.toString());
		}
		
		wakeableManagerLock.lock();
		try {
			if (!this.wakeables.contains(wakeable)) {
				this.wakeables.add(wakeable);
				this.nonWakeablesAfter.put(wakeable, Integer.valueOf(0));
				return true;
			}
			else return false;
		}
		finally {
			wakeableManagerLock.unlock();
		}
	}

	//should be called when a wakeable object is no longer going to be wakeable
	/* (non-Javadoc)
	 * @see net.hycube.eventprocessing.WakeableManager#removeWakeable(net.hycube.eventprocessing.Wakeable)
	 */
	@Override
	public boolean removeWakeable(Wakeable wakeable) {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Unregistering Wakeable object: " + wakeable.toString());
		}
		
		wakeableManagerLock.lock();
		try {
			this.nonWakeablesAfter.remove(wakeable);
			return this.wakeables.remove(wakeable);
		}
		finally {
			wakeableManagerLock.unlock();
		}
	}

	protected List<Wakeable> getWakeables() {
		wakeableManagerLock.lock();
		try {
			List<Wakeable> copyList = new LinkedList<Wakeable>(wakeables);
			return copyList;
		}
		finally {
			wakeableManagerLock.unlock();
		}
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.eventprocessing.WakeableManager#discard()
	 */
	@Override
	public void discard() {
		if (devLog.isDebugEnabled()) {
			devLog.debug("Discarding - unregistering all Wakeable objects.");
		}
		
		wakeableManagerLock.lock();
		try {
			this.nonWakeablesAfter.clear();
			this.wakeables.clear();
		}
		finally {
			wakeableManagerLock.unlock();
		}
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.eventprocessing.WakeableManager#wakeup()
	 */
	@Override
	public void wakeup() {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("Wakeup called.");
		}
		
		wakeableManagerLock.lock();
		try {
			if (!wakeables.isEmpty()) {
				int nonWakeablesAfterLastWakeable = nonWakeablesAfter.get(wakeables.getLast());
				nonWakeablesAfterLastWakeable++;
				nonWakeablesAfter.put(wakeables.getLast(), nonWakeablesAfterLastWakeable);
				
				int numWakeables = wakeables.size();
				int numNonWakeables = 0;
				for (int nonWakeableAfter : nonWakeablesAfter.values()) {
					numNonWakeables = numNonWakeables + nonWakeableAfter;
				}
				
				if (devLog.isDebugEnabled()) {
					devLog.debug("numWakeables: " + numWakeables + ", numNonWakeables: " + numNonWakeables + ", availableSlots: " + availableSlots);
				}
				
				while (numWakeables + numNonWakeables > availableSlots && numWakeables > 0) {
					
					Wakeable firstWakeable = wakeables.getFirst();
					
					if (devLog.isDebugEnabled()) {
						devLog.debug("Waking the first enqueued Wakeable: " + firstWakeable.toString());
					}
					
					numWakeables--;
					numNonWakeables = numNonWakeables - nonWakeablesAfter.get(firstWakeable);
					firstWakeable.wakeup();
					wakeables.remove(firstWakeable);
					nonWakeablesAfter.remove(firstWakeable);
					
					if (devLog.isDebugEnabled()) {
						devLog.debug("numWakeables: " + numWakeables + ", numNonWakeables: " + numNonWakeables + ", availableSlots: " + availableSlots);
					}
					
				}
				
			}
			else {
				//no Wakeable to wake up, do nothing
				if (devLog.isTraceEnabled()) {
					devLog.debug("No Wakeable to wake up, do nothing.");
				}
			}
		}
		finally {
			wakeableManagerLock.unlock();
		}

	}

	@Override
	public int getNextMaxSleepTime() {
		return WAKEABLE_SLEEP_TIMEOUT;
	}



}
