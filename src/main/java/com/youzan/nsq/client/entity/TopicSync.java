package com.youzan.nsq.client.entity;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * topic synchronization in topic class
 * Created by lin on 17/6/19.
 */
public class TopicSync {
    private final String topic;
    private final ReentrantReadWriteLock lock;

    public TopicSync(final String topic) {
        this.topic = topic;
        this.lock = new ReentrantReadWriteLock();
    }

    public String getTopicText() {
        return this.topic;
    }

    public void lock() {
        this.lock.writeLock().lock();
    }

    public void unlock() {
        this.lock.writeLock().unlock();
    }

    /**
     * try acquire write lock of current topic
     * @return {@code true} if the lock was free and was acquired
     * by the current thread, or the write lock was already held
     * by the current thread; and {@code false} otherwise.
     *
     */
    public boolean tryLock() {
        return this.lock.writeLock().tryLock();
    }

    public int hashCode() {
        return this.topic.hashCode();
    }
}
