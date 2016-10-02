package net.hycube.eventprocessing;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.locks.Lock;

public interface NotifyingQueue<T> extends Queue<T> {
	
	public boolean addListener(NotifyingQueueListener<T> listener);
	
	public boolean removeListener(NotifyingQueueListener<T> listener);
	
	public void removeAllListeners();
	
	public List<NotifyingQueueListener<T>> getListeners();
	
	public void setInsertNotifyLock(Lock insertNotifyLock);
	
	public void discard();
	
	
}
