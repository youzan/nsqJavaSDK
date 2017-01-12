package com.youzan.nsq.client.core;

import com.youzan.nsq.client.core.command.Nop;
import com.youzan.nsq.client.core.command.Rdy;
import com.youzan.nsq.client.core.lookup.LookupService;
import com.youzan.nsq.client.core.lookup.LookupServiceImpl;
import com.youzan.nsq.client.entity.*;
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
import java.util.concurrent.ConcurrentHashMap;
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
    private final Map<Topic, IPartitionsSelector> topic_2_partitionsSelector = new ConcurrentHashMap<>();
    private final ConcurrentSortedSet<Address> dataNodes = new ConcurrentSortedSet<>();

    private volatile boolean started;
    private final ScheduledExecutorService scheduler = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory(this.getClass().getName(), Thread.MAX_PRIORITY));

    private final Role role;
    private final LookupService lookup;
    private final boolean useLocalLookupd;

    public NSQSimpleClient(Role role, boolean localLookupd) {
        this.role = role;
        this.lookup = new LookupServiceImpl(role);
        this.useLocalLookupd = localLookupd;
    }

    @Override
    public void start() {
        lock.writeLock().lock();
        try {
            if (!started) {
                started = true;
                LookupAddressUpdate.getInstance(true).keepListLookup();
                keepDataNodes();
            }
            started = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void keepDataNodes() {
        final int delay = _r.nextInt(40); // seconds
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    newDataNodes();
                } catch (NSQException e) {
                    logger.error("Error fetching data node. Process restarts in another round...", e);
                }
            }
        }, delay, _INTERVAL_IN_SECOND, TimeUnit.SECONDS);
    }

    private void newDataNodes() throws NSQException {
        //clear partitions does not valid(topic has no partition attached)
        lock.writeLock().lock();
        try {
            Set<Topic> brokenTopic = new HashSet<>();
            for (Map.Entry<Topic, IPartitionsSelector> pair : topic_2_partitionsSelector.entrySet()) {
                if (pair.getValue() == null) {
                    brokenTopic.add(pair.getKey());
                }
            }
            for (Topic topic : brokenTopic) {
                topic_2_partitionsSelector.remove(topic);
            }
        } finally {
            lock.writeLock().unlock();
        }

        //gather topics from topic_2_partitions
        final Set<Topic> topics = new HashSet<>();
        try {
            lock.readLock().lock();
            //not ant topic is subscribed. process ends
            if(this.role == Role.Consumer && topic_2_partitionsSelector.size() == 0) {
                logger.warn("No any subscribed topic is found, Consumer may not subscribe any topic. Data node updating process ends.");
                return;
            }
            for (Topic topic : topic_2_partitionsSelector.keySet()) {
                topics.add(topic);
            }
        } finally {
            lock.readLock().unlock();
        }

        //lookup gatheres topics
        final Set<Address> newDataNodes = new HashSet<>();
        for (Topic topic : topics) {
            try {
                final IPartitionsSelector aPs = lookup.lookup(topic, this.useLocalLookupd);
                if(null == aPs){
                    logger.warn("No fit partition data found for topic: {}.", topic.getTopicText());
                    continue;
                }
                //dump all partitions in one partitions selector
                Partitions[] allPartitions = aPs.dumpAllPartitions();
                for(Partitions aPartitions:allPartitions) {
                    //update partition mapping topic -> address
                    if (null != aPartitions && aPartitions.hasAnyDataNodes()) {
                        newDataNodes.addAll(aPartitions.getAllDataNodes());
                        try {
                            lock.writeLock().lock();
                            //topic partition update
                            topic_2_partitionsSelector.put(topic, aPs);
                        } finally {
                            lock.writeLock().unlock();
                        }
                    } else {
                        logger.info("Having got an empty data nodes from topic: {}", topic);
                    }
                }
            } catch(NSQLookupException e){
                logger.warn("Could not fetch lookup info for topic: {} at this moment, lookup info may not be ready.", topic);
            } catch (Exception e) {
                logger.error("Exception", e);
            }
        }

        if (0 == newDataNodes.size()) {
            logger.warn("No any data node found for NSQ client.");
            return;
        }

        try {
            lock.writeLock().lock();
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
        try {
            lock.writeLock().lock();
            if (!topic_2_partitionsSelector.containsKey(topic)) {
                final IPartitionsSelector empty = new SimplePartitionsSelector(null);
                topic_2_partitionsSelector.put(topic, empty);
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
            topic_2_partitionsSelector.remove(topic);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * remove multiple topics from simple client, invoker of this function needs to make sure pass in topics are valid
     *
     * @param topics
     */
    public void removeTopics(final Collection<Topic> topics) {
        assert null != topics;
        lock.writeLock().lock();
        try {
            for (Topic topic : topics)
                topic_2_partitionsSelector.remove(topic);
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
                if (conn.getConfig().isConsumerSlowStart()) {
                    int currentRdyCnt = RdySpectrum.decrease(conn, conn.getCurrentRdyCount(), conn.getCurrentRdyCount() - 1);
                    conn.setCurrentRdyCount(currentRdyCnt);
                }
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

    public Address[] getPartitionNodes(Topic topic, long topicShardingID, boolean write) throws NSQException {
        IPartitionsSelector aPs;
        List<Address> nodes = new ArrayList<>();
        lock.readLock().lock();
        try {
            aPs = topic_2_partitionsSelector.get(topic);
            if(null != aPs) {
                Partitions[] partitions;
                if(write)
                    partitions = new Partitions[]{aPs.choosePartitions()};
                else
                    partitions = aPs.dumpAllPartitions();
                //if partitions returned from choose partitions, means there is only one Partitions
                for(Partitions aPartitions : partitions) {
                    //what is a valid Partitions
                    if (null != aPartitions && aPartitions.hasAnyDataNodes()) {
                        //for partitions
                        if (write) {
                            if (aPartitions.hasPartitionDataNodes() && topicShardingID >= 0) {
                                int partitionNum = aPartitions.getPartitionNum();
                                int partitionId = topic.updatePartitionIndex(topicShardingID, partitionNum);
                                //index out of boundry
                                Address addr = aPartitions.getPartitionAddress(partitionId);
                                nodes.add(addr);
                                //all data nodes
                            } else if (topicShardingID < 0) {
                                List<Address> address = aPartitions.getAllDataNodes();
                                nodes.addAll(address);
                            }
                        } else {
                            if (aPartitions.hasPartitionDataNodes() && topic.hasPartition()) {
                                Address addr = aPartitions.getPartitionAddress(topic.getPartitionId());
                                nodes.add(addr);
                            } else {
                                //read from all data nodes
                                List<Address> address = aPartitions.getAllDataNodes();
                                nodes.addAll(address);
                            }
                        }
                    }
                }
                return nodes.toArray(new Address[0]);
            }
            aPs = lookup.lookup(topic, this.useLocalLookupd);
        } finally {
            lock.readLock().unlock();
        }
        //uses first to update existing Partitions
        if (null != aPs) {
            try {
                lock.writeLock().lock();
                topic_2_partitionsSelector.put(topic, aPs);
                for (Partitions aPartitions : aPs.dumpAllPartitions()) {
                    //for partitions
                    if (aPartitions.hasPartitionDataNodes() && topicShardingID >= 0) {
                        int partitionNum = aPartitions.getPartitionNum();
                        int partitionId = topic.updatePartitionIndex(topicShardingID, partitionNum);
                        //index out of boundry
                        Address addr = aPartitions.getPartitionAddress(partitionId);
                        nodes.add(addr);
                        //all data nodes
                    } else if (topicShardingID < 0) {
                        List<Address> address = aPartitions.getAllDataNodes();
                        nodes.addAll(address);
                    }
                }
            } finally {
                lock.writeLock().unlock();
            }
            return nodes.toArray(new Address[0]);
        }
        throw new NSQInvalidTopicException(topic.getTopicText());
    }

    @Override
    public boolean validateHeartbeat(NSQConnection conn) {
        final ChannelFuture future = conn.command(Nop.getInstance());
        return future.awaitUninterruptibly(2500, TimeUnit.MILLISECONDS) && future.isSuccess();
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
