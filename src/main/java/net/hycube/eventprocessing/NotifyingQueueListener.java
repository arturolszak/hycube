package net.hycube.eventprocessing;

import java.util.Collection;

public interface NotifyingQueueListener<T> {
	
	public void itemInserted(T item);
	
	public void itemsInserted(Collection<? extends T> c);
	
	
}
