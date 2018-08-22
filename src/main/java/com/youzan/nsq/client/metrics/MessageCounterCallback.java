package com.youzan.nsq.client.metrics;

import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.Message;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by lin on 18/8/14.
 */
public class MessageCounterCallback {
    private static final Logger logger = LoggerFactory.getLogger(MessageCounterCallback.class);
    private static ExecutorService accumulateExec = Executors.newSingleThreadExecutor();
    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> accumulateExec.shutdown()));
    }
    //total counter
    private AtomicLong cnt;
    //delta cnt map address -> delta cnt, between interval
    private ConcurrentHashMap<Address, AtomicLong> addr2Delta = new ConcurrentHashMap<>();
    private ReentrantReadWriteLock dumpDeltaLock = new ReentrantReadWriteLock();
    private CounterType type;
    private AtomicLong lastDeltaDump;

    public static MessageCounterCallback build(CounterType type) {
        return new MessageCounterCallback(type);
    }

    public MessageCounterCallback(CounterType type) {
        this.cnt = new AtomicLong(0);
        this.lastDeltaDump = new AtomicLong(System.currentTimeMillis());
        this.type = type;
    }

    public void apply(NSQConfig config, NSQMessage msg) {
        try {
            accumulateExec.submit(() -> accumulate(config, msg));
        }catch (Exception e) {
            logger.error("fail to submit message for accumulate, message: {}", msg);
        }
    }

    public void apply(NSQConfig config, Message msg, Address addr) {
        try {
            accumulateExec.submit(() -> accumulate(config, msg, addr));
        }catch (Exception e) {
            logger.error("fail to submit message for accumulate, message: {}", msg);
        }
    }

    public void accumulate(NSQConfig config, NSQMessage msg) {
        accumulate(msg.getAddress());
    }

    public void accumulate(NSQConfig config, Message msg, Address addr) {
        accumulate(addr);
    }

    private void accumulate(Address addr) {
        dumpDeltaLock.readLock().lock();
        addr2Delta.compute(addr, (s, atomicLong) -> {
            if(null == atomicLong) {
                return new AtomicLong(1L);
            } else {
                atomicLong.incrementAndGet();
                return atomicLong;
            }
        });
        this.cnt.incrementAndGet();
        dumpDeltaLock.readLock().unlock();
    }

    public long getCount() {
        return this.cnt.get();
    }

    //return reset delta counter, then reset delta counter to 0 and update lastDeltaDump timestamp
    public Map<Address, Long> dumpDelta() {
        dumpDeltaLock.writeLock().lock();
        try {
            Map<Address, Long> dump = new HashMap<>();
            this.addr2Delta.forEach((address, atomicLong) -> {
                long delta = atomicLong.get();
                if(delta > 0) {
                    dump.put(address, delta);
                }
            });
            //clean map after dump
            this.addr2Delta.clear();
            return dump;
        } finally {
          this.lastDeltaDump.set(System.currentTimeMillis());
          this.dumpDeltaLock.writeLock().unlock();
        }

    }

    public long getLastDeltaDump() {
        return this.lastDeltaDump.get();
    }

    public CounterType getType() {
        return this.type;
    }
}
