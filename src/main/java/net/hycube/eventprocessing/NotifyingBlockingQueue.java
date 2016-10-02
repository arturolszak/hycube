package net.hycube.eventprocessing;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public interface NotifyingBlockingQueue<T> extends NotifyingQueue<T>, BlockingQueue<T> {
	
	public void put(T e, boolean notify) throws InterruptedException;
	
	public boolean offer(T e, long timeout, TimeUnit unit, boolean notify) throws InterruptedException;
	
	public boolean offer(T e, boolean notify);
	
	public boolean add(T e, boolean notify);
	
	public boolean addAll(Collection<? extends T> c, boolean notify);
	
	
	
}
