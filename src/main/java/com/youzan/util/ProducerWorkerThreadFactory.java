package com.youzan.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;

/**
 * Producer worker thread factory
 * Created by lin on 17/11/20.
 */
public class ProducerWorkerThreadFactory implements ThreadFactory {
    private static final Logger logger = LoggerFactory.getLogger(ProducerWorkerThreadFactory.class);

    private final ThreadGroup group;
    private final String namePrefix;
    private int priority = Integer.MIN_VALUE;

    final static Thread.UncaughtExceptionHandler uncaughtExceptionHandler = new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            logger.error(t.getName(), e);
        }
    };

    public ProducerWorkerThreadFactory(String poolName, int priority) {
        if (null == poolName || poolName.isEmpty()) {
            throw new IllegalArgumentException();
        }
        group = Thread.currentThread().getThreadGroup();
        namePrefix = poolName + "-Producer-Worker-Pool-Thread";
        this.priority = priority;
    }

    @Override
    public Thread newThread(Runnable r) {

        final Thread t = new Thread(group, r, namePrefix, 0);
        if (t.isDaemon()) {
            t.setDaemon(false);
        }
        switch(priority){
            case Thread.MAX_PRIORITY :
            case Thread.MIN_PRIORITY:
            case Thread.NORM_PRIORITY:
                t.setPriority(priority);
                break;
            default: {
                t.setPriority(Thread.NORM_PRIORITY);
            }

        }
        Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
        return t;
    }
}
