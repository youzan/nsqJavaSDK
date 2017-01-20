package com.youzan.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {

    private static final Logger logger = LoggerFactory.getLogger(NamedThreadFactory.class);

    private final ThreadGroup group;

    private final AtomicInteger threadNumber = new AtomicInteger(1);

    private final String namePrefix;
    private int priority = Integer.MIN_VALUE;

    final static UncaughtExceptionHandler uncaughtExceptionHandler = new UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            logger.error(t.getName(), e);
        }
    };

    public NamedThreadFactory(String poolName, int priority) {
        if (null == poolName || poolName.isEmpty()) {
            throw new IllegalArgumentException();
        }
        group = Thread.currentThread().getThreadGroup();
        namePrefix = poolName + "-Pool-Thread-";
        this.priority = priority;
    }

    @Override
    public Thread newThread(Runnable r) {
        final StringBuilder sb = new StringBuilder(namePrefix.length() + 10);
        sb.append(namePrefix).append(String.valueOf(threadNumber.getAndIncrement()));

        final Thread t = new Thread(group, r, sb.toString(), 0);
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
