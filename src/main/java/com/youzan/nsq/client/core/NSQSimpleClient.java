package com.youzan.nsq.client.core;

import com.youzan.nsq.client.core.command.Nop;
import com.youzan.nsq.client.core.command.Rdy;
import com.youzan.nsq.client.core.lookup.LookupService;
import com.youzan.nsq.client.core.lookup.LookupServiceImpl;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.Response;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.exception.NSQInvalidTopicException;
import com.youzan.nsq.client.exception.NSQLookupException;
import com.youzan.nsq.client.network.frame.ErrorFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.nsq.client.network.frame.ResponseFrame;
import com.youzan.util.ConcurrentSortedSet;
import com.youzan.util.NamedThreadFactory;
import com.youzan.util.ThreadSafe;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The intersection between {@link  com.youzan.nsq.client.Producer} and {@link com.youzan.nsq.client.Consumer}.
 *
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
@ThreadSafe
public class NSQSimpleClient implements Client, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(NSQSimpleClient.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    //maintain a mapping from topic to producer broadcast addresses
    private final Map<Topic, ConcurrentSortedSet<Address>> topic_2_dataNodes = new HashMap<>();
    private final ConcurrentSortedSet<Address> dataNodes = new ConcurrentSortedSet<>();

    private volatile boolean started;
    private final ScheduledExecutorService scheduler = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory(this.getClass().getName(), Thread.MAX_PRIORITY));

    private final LookupService lookup;

    public NSQSimpleClient(final String lookupAddresses) {
        this.lookup = new LookupServiceImpl(lookupAddresses);
    }

    public NSQSimpleClient(final String[] lookupAddresses, final LookupAddressUpdate lookupUpdate) {
        this.lookup = new LookupServiceImpl(lookupAddresses, lookupUpdate);
    }

    @Override
    public void start() {
        lock.writeLock().lock();
        try {
            if (!started) {
                started = true;
                lookup.start();
                keepDataNodes();
            }
            started = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void keepDataNodes() {
        final int delay = _r.nextInt(60); // seconds
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
        lock.writeLock().lock();
        try {
            Set<Topic> brokenTopic = new HashSet<>();
            for (Map.Entry<Topic, ConcurrentSortedSet<Address>> pair : topic_2_dataNodes.entrySet()) {
                if (pair.getValue() == null) {
                    brokenTopic.add(pair.getKey());
                }
            }
            for (Topic topic : brokenTopic) {
                topic_2_dataNodes.remove(topic);
            }
        } finally {
            lock.writeLock().unlock();
        }

        // HTTP costs long time.
        final Set<Topic> topics = new HashSet<>();
        lock.writeLock().lock();
        try {
            for (Topic topic : topic_2_dataNodes.keySet()) {
                topics.add(topic);
            }
        } finally {
            lock.writeLock().unlock();
        }

        final Set<Address> newDataNodes = new HashSet<>();
        for (Topic topic : topics) {
            try {
                final SortedSet<Address> addresses = lookup.lookup(topic);
                if (!addresses.isEmpty()) {
                    newDataNodes.addAll(addresses);
                    lock.writeLock().lock();
                    try {
                        final ConcurrentSortedSet<Address> oldAddresses;
                        if (topic_2_dataNodes.containsKey(topic)) {
                            oldAddresses = topic_2_dataNodes.get(topic);
                            oldAddresses.swap(addresses);
                        } else {
                            oldAddresses = new ConcurrentSortedSet<>();
                            oldAddresses.addAll(addresses);
                        }
                        topic_2_dataNodes.put(topic, oldAddresses);
                    } finally {
                        lock.writeLock().unlock();
                    }
                } else {
                    logger.info("Having got an empty data nodes from topic: {}", topic);
                }
            } catch (Exception e) {
                logger.error("Exception", e);
            }
        }

        lock.writeLock().lock();
        try {
            dataNodes.clear();
            dataNodes.addAll(newDataNodes);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void putTopic(Topic topic) {
        if (topic == null) {
            return;
        }
        lock.writeLock().lock();
        try {
            if (!topic_2_dataNodes.containsKey(topic)) {
                final ConcurrentSortedSet<Address> empty = new ConcurrentSortedSet<>();
                topic_2_dataNodes.put(topic, empty);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removeTopic(Topic topic) {
        if (topic == null || null == topic.getTopicText() || topic.getTopicText().isEmpty()) {
            return;
        }
        lock.writeLock().lock();
        try {
            topic_2_dataNodes.remove(topic);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * remove multiple topics from simple client, invoker of this function needs to make sure pass in topics are valid
     * @param topics
     */
    public void removeTopics(final Collection<Topic> topics) {
        assert null != topics;
        lock.writeLock().lock();
        try {
            for(Topic topic:topics)
                topic_2_dataNodes.remove(topic);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void incoming(final NSQFrame frame, final NSQConnection conn) throws NSQException {
        if (frame == null) {
            logger.info("The frame is null because of SDK's bug in the {}", this.getClass().getName());
            return;
        }
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
                logger.warn("Error-Frame from {} , frame: {}", conn.getAddress(), frame);
                break;
            }
            case MESSAGE_FRAME: {
                logger.warn("Un-excepted a message frame in the simple client.");
                break;
            }
            default: {
                logger.warn("Invalid frame-type from {} , frame-type: {} , frame: {}", conn.getAddress(), frame.getType(), frame);
                break;
            }
        }
    }

    @Override
    public void backoff(NSQConnection conn) {
        conn.command(new Rdy(0));
    }

    public ConcurrentSortedSet<Address> getDataNodes(Topic topic) throws NSQException {
        SortedSet<Address> first = null;
        lock.readLock().lock();
        try {
            final ConcurrentSortedSet<Address> dataNodes = topic_2_dataNodes.get(topic);
            if (dataNodes != null && !dataNodes.isEmpty()) {
                return dataNodes;
            }
            first = lookup.lookup(topic);
        } finally {
            lock.readLock().unlock();
        }
        if (first != null && !first.isEmpty()) {
            final ConcurrentSortedSet<Address> old;
            lock.readLock().lock();
            try {
                old = topic_2_dataNodes.get(topic);
            } finally {
                lock.readLock().unlock();
            }
            final ConcurrentSortedSet<Address> dataNodes;
            if (old == null) {
                dataNodes = new ConcurrentSortedSet<>();
            } else {
                dataNodes = old;
            }
            dataNodes.addAll(first);
            lock.writeLock().lock();
            try {
                topic_2_dataNodes.put(topic, dataNodes);
                return dataNodes;
            } finally {
                lock.writeLock().unlock();
            }
        }
        throw new NSQInvalidTopicException(topic);
    }

    @Override
    public boolean validateHeartbeat(NSQConnection conn) {
        final ChannelFuture future = conn.command(Nop.getInstance());
        return future.awaitUninterruptibly(2000, TimeUnit.MILLISECONDS) && future.isSuccess();
    }

    @Override
    public Set<NSQConnection> clearDataNode(Address address) {
        dataNodes.remove(address);
        return null;
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            lookup.close();
            topic_2_dataNodes.clear();
            dataNodes.clear();
            scheduler.shutdownNow();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void close(NSQConnection conn) {
    }

}
