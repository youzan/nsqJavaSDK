/**
 *
 */
package com.youzan.nsq.client.core;

import com.youzan.nsq.client.core.command.Nop;
import com.youzan.nsq.client.core.command.Rdy;
import com.youzan.nsq.client.core.lookup.LookupService;
import com.youzan.nsq.client.core.lookup.LookupServiceImpl;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.Response;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.exception.NSQLookupException;
import com.youzan.nsq.client.network.frame.ErrorFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.nsq.client.network.frame.ResponseFrame;
import com.youzan.util.ConcurrentSortedSet;
import com.youzan.util.NamedThreadFactory;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The intersection between {@link  com.youzan.nsq.client.Producer} and {@link com.youzan.nsq.client.Consumer}.
 *
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public class NSQSimpleClient implements Client, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(NSQSimpleClient.class);

    private final Lock lock = new ReentrantLock();
    private final ConcurrentHashMap<String, ConcurrentSortedSet<Address>> topic_2_dataNodes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> topic_2_lastActiveTime = new ConcurrentHashMap<>();
    private final Set<Address> dataNodes = new HashSet<>();
    private final ConcurrentHashMap<Address, Long> dataNode_2_lastActiveTime = new ConcurrentHashMap<>();

    private volatile boolean started;
    private final ScheduledExecutorService scheduler = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory(this.getClass().getName(), Thread.MAX_PRIORITY));

    private final LookupService lookup;

    public NSQSimpleClient(final String lookupAddresses) {
        this.lookup = new LookupServiceImpl(lookupAddresses);
    }

    @Override
    public void start() {
        if (!started) {
            started = true;
            lookup.start();
            keepDataNodes();
        }
        started = true;
    }

    private void keepDataNodes() {
        final int delay = _r.nextInt(60) + 45; // seconds
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    newDataNodes();
                } catch (Exception e) {
                    logger.error("Exception", e);
                }
            }
        }, delay, _INTERVAL_IN_SECOND, TimeUnit.SECONDS);
    }

    private void newDataNodes() throws NSQLookupException {
        final long now = System.currentTimeMillis();
        final long duration = 3600L;
        final long allow = now - duration;
        final Set<String> expiredTopics = new HashSet<>();
        final Set<Address> expiredAddresses = new HashSet<>();
        for (Map.Entry<String, Long> pair : topic_2_lastActiveTime.entrySet()) {
            logger.debug("Topic Recentness: {} , AllowActive: {}", pair.getValue(), allow);
            if (pair.getValue().longValue() < allow) {
                expiredTopics.add(pair.getKey());
            }
        }
        for (Map.Entry<Address, Long> pair : dataNode_2_lastActiveTime.entrySet()) {
            if (pair.getValue().longValue() < allow) {
                expiredAddresses.add(pair.getKey());
            }
        }
        for (String topic : expiredTopics) {
            topic_2_lastActiveTime.remove(topic);
            topic_2_dataNodes.remove(topic);
        }
    }

    public void putTopic(String topic) {
        if (topic == null || topic.isEmpty()) {
            return;
        }
        final Long now = Long.valueOf(System.currentTimeMillis());
        topic_2_lastActiveTime.put(topic, now);
    }

    @Override
    public void incoming(final NSQFrame frame, final NSQConnection conn) throws NSQException {
        // TODO
        switch (frame.getType()) {
            case RESPONSE_FRAME: {
                final String resp = frame.getMessage();
                if (Response._HEARTBEAT_.getContent().equals(resp)) {
                    conn.command(Nop.getInstance());
                    return;
                } else {
                    conn.addResponseFrame((ResponseFrame) frame);
                }
                break;
            }
            case ERROR_FRAME: {
                final ErrorFrame err = (ErrorFrame) frame;
                try {
                    conn.addErrorFrame(err);
                } catch (Exception e) {
                    logger.error("Address: {}, Exception:", conn.getAddress(), e);
                }
            }
            default: {
                logger.error("Invalid frame-type from {} , frame: {}", conn.getAddress(), frame);
                break;
            }
        }
        return;
    }

    @Override
    public void backoff(NSQConnection conn) {
        conn.command(new Rdy(0));
    }

    @Override
    public ConcurrentSortedSet<Address> getDataNodes(String topic) {
        return topic_2_dataNodes.get(topic);
    }

    @Override
    public void clearDataNode(Address address) {
        if (address == null) {
            return;
        }

        if (topic_2_dataNodes.containsKey(address)) {
            topic_2_dataNodes.remove(address);
            final ConcurrentSortedSet<Address> nodes = topic_2_dataNodes.get(address);
            nodes.clear();
        }
    }

    @Override
    public boolean validateHeartbeat(NSQConnection conn) {
        final ChannelFuture future = conn.command(Nop.getInstance());
        if (future.awaitUninterruptibly(500, TimeUnit.MILLISECONDS)) {
            return future.isSuccess();
        }
        return false;
    }

    @Override
    public void close() {
        topic_2_dataNodes.clear();
        scheduler.shutdownNow();
    }

    void sleep(final long millisecond) {
        logger.debug("Sleep {} millisecond.", millisecond);
        try {
            Thread.sleep(millisecond);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Your machine is too busy! Please check it!");
        }
    }
}
