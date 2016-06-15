package com.youzan.util;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamedThreadFactory implements ThreadFactory {

    private static final Logger logger = LoggerFactory.getLogger(NamedThreadFactory.class);

    private final ThreadGroup group;

    private final AtomicInteger threadNumber = new AtomicInteger(1);

    private final String namePrefix;
    private final int priority;

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
        if (t.getPriority() != Thread.NORM_PRIORITY) {
            t.setPriority(Thread.NORM_PRIORITY);
        }
        Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
        return t;
    }

}
