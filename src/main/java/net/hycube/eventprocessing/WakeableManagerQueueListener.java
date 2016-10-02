package net.hycube.eventprocessing;

import java.util.Collection;

public class WakeableManagerQueueListener<T> implements NotifyingQueueListener<T> {

	protected WakeableManager wakeableManager;
	
	public WakeableManagerQueueListener(WakeableManager wakeableManager) {
		this.wakeableManager = wakeableManager;
	}
	
	public void itemInserted(T item) {
		if (wakeableManager != null) wakeableManager.wakeup();
	}
	
	public void itemsInserted(Collection<? extends T> c) {
		if (wakeableManager != null) {
			for (int i = 0; i < c.size(); i++) wakeableManager.wakeup();
		}
	}
	
	public void discard() {
		this.wakeableManager = null;
	}
	
	public boolean isDiscarded() {
		return (this.wakeableManager == null);
	}
	
	public boolean isSet() {
		return (this.wakeableManager != null);
	}
	
}
