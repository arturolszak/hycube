package net.hycube.eventprocessing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public class NotifyingLinkedBlockingQueue<T> extends LinkedBlockingQueue<T> implements NotifyingBlockingQueue<T> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7554279776327195699L;

	protected ArrayList<NotifyingQueueListener<T>> listeners;
	protected Object listenersLock = new Object();
	protected Lock insertNotifyLock;
	
	public NotifyingLinkedBlockingQueue() {
		this((Lock)null);
	}

	public NotifyingLinkedBlockingQueue(Collection<? extends T> c) {
		this(c, (Lock)null);
	}

	public NotifyingLinkedBlockingQueue(int capacity) {
		this(capacity, (Lock)null);
	}

	public NotifyingLinkedBlockingQueue(Lock insertNotifyLock) {
		super();
		this.insertNotifyLock = insertNotifyLock;
		listeners = new ArrayList<NotifyingQueueListener<T>>();
	}

	public NotifyingLinkedBlockingQueue(Collection<? extends T> c, Lock insertNotifyLock) {
		super(c);
		this.insertNotifyLock = insertNotifyLock;
		listeners = new ArrayList<NotifyingQueueListener<T>>();
	}

	public NotifyingLinkedBlockingQueue(int capacity, Lock insertNotifyLock) {
		super(capacity);
		this.insertNotifyLock = insertNotifyLock;
		listeners = new ArrayList<NotifyingQueueListener<T>>();
	}

	
	
	
	//Override LinkedBlockinQueue members that insert elements:

	@Override
	public void put(T e) throws InterruptedException {
		put(e, true);
	}

	@Override
	public boolean offer(T e, long timeout, TimeUnit unit) throws InterruptedException {
		return offer(e, timeout, unit, true);
	}

	@Override
	public boolean offer(T e) {
		return offer(e, true);
	}

	@Override
	public boolean add(T e) {
		return add(e, true);
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		return addAll(c, true);
	}

	
	
	
	//Implement NotifyingBlockingQueue members:
	
	@Override
	public void put(T e, boolean notify) throws InterruptedException {
		
		if (!notify) {
			super.put(e);
			return;
		}
		else {		
			if (insertNotifyLock != null) insertNotifyLock.lock();
			try {
				super.put(e);
				notifyListeners(e);
			}
			finally {
				if (insertNotifyLock != null) insertNotifyLock.unlock();
			}
		}
	}
	
	@Override
	public boolean offer(T e, long timeout, TimeUnit unit, boolean notify) throws InterruptedException {
		if (!notify) return super.offer(e, timeout, unit);
		else {
			if (insertNotifyLock != null) insertNotifyLock.lock();
			try {
				boolean success = super.offer(e, timeout, unit);
				if (success) notifyListeners(e);
				return success;
			}
			finally {
				if (insertNotifyLock != null) insertNotifyLock.unlock();
			}
		}
	}
	
	@Override
	public boolean offer(T e, boolean notify) {
		if (!notify) return super.offer(e);
		else {
			if (insertNotifyLock != null) insertNotifyLock.lock();
			try {
				boolean success = super.offer(e);
				if (success) notifyListeners(e);
				return success;
			}
			finally {
				if (insertNotifyLock != null) insertNotifyLock.unlock();
			}
		}
	}

	@Override
	public boolean add(T e, boolean notify) {
		if (!notify) return super.add(e);
		else {
			if (insertNotifyLock != null) insertNotifyLock.lock();
			try {
				boolean success = super.add(e);
				if (success) notifyListeners(e);
				return success;
			}
			catch (Exception ex) {
				throw new RuntimeException(ex);
			}
			finally {
				if (insertNotifyLock != null) insertNotifyLock.unlock();
			}
		}
	}

	@Override
	public boolean addAll(Collection<? extends T> c, boolean notify) {
		if (!notify) return super.addAll(c); 
		else {
			if (insertNotifyLock != null) insertNotifyLock.lock();
			try {
				boolean success = super.addAll(c);
				if (success) notifyListeners(c);
				return success;
			}
			finally {
				if (insertNotifyLock != null) insertNotifyLock.unlock();
			}
		}
	}

	
	
	

	//Implement NotifyingQueue members
	
	@Override
	public boolean addListener(NotifyingQueueListener<T> listener) {
		synchronized (listenersLock) {
			if (!this.listeners.contains(listener)) {
				this.listeners.add(listener);
				return true;
			}
			else return false;
		}
	}

	@Override
	public boolean removeListener(NotifyingQueueListener<T> listener) {
		synchronized (listenersLock) {
			return this.listeners.remove(listener);
		}
	}

	@Override
	public void removeAllListeners() {
		synchronized (listenersLock) {
			this.listeners.clear();
		}
	}


	@Override
	public List<NotifyingQueueListener<T>> getListeners() {
		synchronized (listenersLock) {
			ArrayList<NotifyingQueueListener<T>> copyList = new ArrayList<NotifyingQueueListener<T>>(listeners);
			return copyList;
		}
	}

	@Override
	public void discard() {
		removeAllListeners();
	}
	
	
	protected void notifyListeners(T e) {
		synchronized (listenersLock) {
			ArrayList<NotifyingQueueListener<T>> listenersCopy = new ArrayList<NotifyingQueueListener<T>>(listeners);
			for (NotifyingQueueListener<T> listener : listenersCopy) {
				listener.itemInserted(e);
			}
		}
	}
	
	protected void notifyListeners(Collection<? extends T> c) {
		synchronized (listenersLock) {
			ArrayList<NotifyingQueueListener<T>> listenersCopy = new ArrayList<NotifyingQueueListener<T>>(listeners);
			for (NotifyingQueueListener<T> listener : listenersCopy) {
				listener.itemsInserted(c);
			}
		}
	}

	@Override
	public void setInsertNotifyLock(Lock insertNotifyLock) {
		this.insertNotifyLock = insertNotifyLock;
	}
	
	
}
