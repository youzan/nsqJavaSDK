package com.youzan.nsq.client.entity;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * topic synchronization in topic class
 * Created by lin on 17/6/19.
 */
public class TopicSync {
    private final String topic;
    private final ReentrantReadWriteLock lock;
    private final Condition waitOnLock;

    public TopicSync(final String topic) {
        this.topic = topic;
        this.lock = new ReentrantReadWriteLock();
        this.waitOnLock = this.lock.writeLock().newCondition();
    }

    public String getTopicText() {
        return this.topic;
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

    /**
     * await for milliSec timeout
     * @param milliSec the time unit of the {@code time} argument
     * @return {@code false} if the waiting time detectably elapsed
     *         before return from the method, else {@code true}
     * @throws InterruptedException
     */
    public boolean await(long milliSec) throws InterruptedException {
        return this.waitOnLock.await(milliSec, TimeUnit.MILLISECONDS);
    }

    public void signalAll() {
        this.waitOnLock.signalAll();
    }

    public int hashCode() {
        return this.topic.hashCode();
    }
}
