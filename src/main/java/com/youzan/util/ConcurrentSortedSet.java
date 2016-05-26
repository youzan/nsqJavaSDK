/**
 * 
 */
package com.youzan.util;

import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Blocking when try-lock. It is for the small size collection
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
@ThreadSafe
public final class ConcurrentSortedSet<T> {
    private static final long serialVersionUID = -4747846630389873940L;
    private static final Logger logger = LoggerFactory.getLogger(ConcurrentSortedSet.class);

    private SortedSet<T> set = null;
    private T[] array = null;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReadLock r = lock.readLock();
    private final WriteLock w = lock.writeLock();

    public ConcurrentSortedSet() {
        while (w.tryLock()) {
            try {
                set = new TreeSet<>();
                array = null;
            } finally {
                w.unlock();
            }
        }
    }

    @SuppressWarnings("hiding")
    public <T> T[] newArray(T[] a) {
        while (r.tryLock()) {
            try {
                return set.toArray(a);
            } finally {
                r.unlock();
            }
        }
        return null;
    }

    @SuppressWarnings({ "hiding", "unchecked" })
    public <T> T[] getArray() {
        while (r.tryLock()) {
            try {
                return (T[]) array;
            } finally {
                r.unlock();
            }
        }
        return null;
    }

    public void clear() {
        while (w.tryLock()) {
            try {
                set.clear();
            } finally {
                w.unlock();
            }
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

    @SuppressWarnings("unchecked")
    public boolean addAll(Collection<? extends T> c) {
        if (c == null || c.isEmpty()) {
            return true;
        }
        w.lock();
        try {
            set.addAll(c);
            Object[] a = set.toArray();
            array = (T[]) a;
            return true;
        } finally {
            w.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    public void swap(SortedSet<T> target) {
        if (target == null || target.isEmpty()) {
            throw new IllegalArgumentException("Your input is black!");
        }

        w.lock();
        final SortedSet<T> tmp = set;
        try {
            set = target;
            Object[] a = set.toArray();
            array = (T[]) a;
        } finally {
            w.unlock();
        }
        tmp.clear();
    }
}
