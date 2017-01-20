/**
 * 
 */
package com.youzan.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/**
 * Blocking when try-lock. It is for the small size collection. It consists of
 * one {@link SortedSet}
 * 
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 *
 * 
 */
@ThreadSafe
public class ConcurrentSortedSet<T> implements java.io.Serializable {
    private static final long serialVersionUID = -4747846630389873940L;
    private static final Logger logger = LoggerFactory.getLogger(ConcurrentSortedSet.class);

    private SortedSet<T> set = null;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReadLock r = lock.readLock();
    private final WriteLock w = lock.writeLock();

    public ConcurrentSortedSet() {
        set = new TreeSet<>();
    }

    public void clear() {
        w.lock();
        try {
            set.clear();
        } finally {
            w.unlock();
        }
    }

    public int size() {
        r.lock();
        try {
            return set.size();
        } finally {
            r.unlock();
        }
    }

    public boolean addAll(Collection<? extends T> c) {
        if (c == null || c.isEmpty()) {
            return true;
        }
        w.lock();
        try {
            return set.addAll(c);
        } finally {
            w.unlock();
        }
    }

    /**
     * Replace inner-data into the specified target.
     * 
     * @param target
     *            the new data
     */
    public void swap(SortedSet<T> target) {
        if (target == null) {
            throw new IllegalArgumentException("Your input is null pointer!");
        }
        w.lock();
        final SortedSet<T> tmp = set;
        try {
            set = target;
        } finally {
            w.unlock();
        }
        tmp.clear();
    }

    public void add(T e) {
        if (e == null) {
            return;
        }
        w.lock();
        try {
            set.add(e);
        } finally {
            w.unlock();
        }
    }

    /**
     * @param e
     *            the element that need to be removed
     */
    public void remove(T e) {
        w.lock();
        try {
            set.remove(e);
        } finally {
            w.unlock();
        }
    }

    public boolean isEmpty() {
        r.lock();
        try {
            return set.isEmpty();
        } finally {
            r.unlock();
        }
    }

    public T[] newArray(T[] a) {
        r.lock();
        try {
            return set.toArray(a);
        } finally {
            r.unlock();
        }
    }

    /**
     * Never return null.
     * 
     * @return the new {@link SortedSet}
     */
    public SortedSet<T> newSortedSet() {
        r.lock();
        try {
            return new TreeSet<>(set);
        } finally {
            r.unlock();
        }
    }

    @Override
    public String toString() {
        r.lock();
        try {
            return set.toString();
        } finally {
            r.unlock();
        }
    }
}
