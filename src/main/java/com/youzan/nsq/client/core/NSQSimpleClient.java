package com.youzan.nsq.client.core;

import com.youzan.nsq.client.core.command.Nop;
import com.youzan.nsq.client.core.command.Rdy;
import com.youzan.nsq.client.core.lookup.LookupService;
import com.youzan.nsq.client.core.lookup.LookupServiceImpl;
import com.youzan.nsq.client.entity.*;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.exception.NSQInvalidTopicException;
import com.youzan.nsq.client.exception.NSQLookupException;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.util.NamedThreadFactory;
import com.youzan.util.ThreadSafe;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The intersection between {@link  com.youzan.nsq.client.Producer} and {@link com.youzan.nsq.client.Consumer}.
 * Mainly works on maintaining topic to partition selectors.
 *
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
@ThreadSafe
public class NSQSimpleClient implements Client, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(NSQSimpleClient.class);
    private static final Logger PERF_LOG = LoggerFactory.getLogger(NSQSimpleClient.class.getName() + ".perf");

    private final static AtomicInteger CLIENT_ID = new AtomicInteger(0);
    private int lookupLocalID = -1;
    //topic to partition selectors
    private final ConcurrentMap<String, IPartitionsSelector> topic_2_partitionsSelector = new ConcurrentHashMap<>();
    private static final long TOPIC_PARTITION_TIMEOUT = 90L;

    private final Map<String, Long> ps_lastInvalidated = new ConcurrentHashMap<>();
    /*
     *topic synchronization lock for topicSyncMap updating, readLock applied upon the usage of topicSync for specified topic
     *writeLock applied upon the update of topicSyncMap
     */
    private final ReentrantReadWriteLock topicSyncLock = new ReentrantReadWriteLock();
    private final ConcurrentMap<String, TopicSync> topicSynMap = new ConcurrentHashMap<>();

    /*
     *lock for synchronization during start & close of simple client
     */
    private final ReentrantLock lock = new ReentrantLock();
    private volatile boolean started;

    /*
     *single schedule executor for maintaining topic to partition map
     */
    private final ScheduledExecutorService scheduler = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory(this.getClass().getName(), Thread.MAX_PRIORITY));

    /*
     * role of client current simple client nested
     */
    private final Role role;
    private final LookupService lookup;
    private final boolean useLocalLookupd;

    public NSQSimpleClient(Role role, boolean localLookupd) {
        this.role = role;
        this.lookupLocalID = CLIENT_ID.incrementAndGet();
        this.lookup = new LookupServiceImpl(role, this.lookupLocalID);
        this.useLocalLookupd = localLookupd;
    }

    /**
     * reset lookup local ID back to 0. As it is designed for test case, User do no need to use this function.
     */
    public static void resetLookupLocalID(){
        CLIENT_ID.set(0);
        logger.info("Global lookup local ID set back to 0.");
    }

    /*
     * simple client meta info string
     */
    private static final String SIMPLECLIENT_META_FORMAT = "%s: [" +
            "started: %b," +
            "\t" + "role: %s," +
            "\t" + "userLocalLookupd: %b," +
            "\t" + "lookupLocalID: %d";

    /**
     * return meta data of current simple client and {@link Client} current simple client belongs to.
     * @return string meta data of simple client.
     */
    public String toString() {
        String toString = String.format(SIMPLECLIENT_META_FORMAT, super.toString(), this.started, this.role.getRoleTxt(), this.useLocalLookupd, this.lookupLocalID);
        return toString;
    }

    @Override
    public void start() {
        if(!started && lock.tryLock()) {
            try {
                if (!started) {
                    started = true;
                    LookupAddressUpdate.getInstance(true).keepListLookup();
                    keepDataNodes();
                }
                started = true;
            }finally {
                lock.unlock();
            }
        }
    }

    /**
     * return lookupLocalId of current SimpleClient.
     * @return lookupLocalID
     */
    public int getLookupLocalID() {
        return this.lookupLocalID;
    }

    /**
     * schedule job to keep maintaining {@link this#topic_2_partitionsSelector}
     */
    private void keepDataNodes() {
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    newDataNodes();
                } catch (Throwable e) {
                    logger.error("Error fetching data node. Process restarts in another round...", e);
                }
            }
        }, 0, _INTERVAL_IN_SECOND, TimeUnit.SECONDS);
    }

    /**
     * logic for maintain topic to {@link IPartitionsSelector}
     * @throws NSQException
     */
    private void newDataNodes() throws NSQException {
        //gather topics from topic_2_partitions
        final Set<String> topics = new HashSet<>();
        //not ant topic is subscribed. process ends
        if(this.role == Role.Consumer && topicSynMap.size() == 0) {
            logger.warn("No any subscribed topic is found, Consumer may not subscribe any topic. Data node updating process ends.");
            return;
        }

        for (String topic : topicSynMap.keySet()) {
            topics.add(topic);
        }

        //lookup gatheres topics
        for (String topic : topics) {
            try {
                final IPartitionsSelector aPs = lookup.lookup(topic, this.useLocalLookupd, false);
                if(null == aPs){
                    logger.warn("No fit partition data found for topic: {}.", topic);
                    continue;
                }
                //dump all partitions in one partitions selector
                topic_2_partitionsSelector.put(topic, aPs);
            } catch(NSQLookupException e){
                logger.warn("Could not fetch lookup info for topic: {} at this moment, lookup info may not be ready.", topic);
            } catch (Exception e) {
                logger.error("Exception", e);
            }
        }
    }

    public void putTopic(String topic) {
        if (topic == null) {
            throw new IllegalArgumentException("Topic is not allowed to be empty.");
        }

        boolean syncExist;
        topicSyncLock.readLock().lock();
        try{
            syncExist = topicSynMap.containsKey(topic);
        }finally {
            topicSyncLock.readLock().unlock();
        }

        if(!syncExist) {
            topicSyncLock.writeLock().lock();
            try {
                if(!topicSynMap.containsKey(topic))
                    topicSynMap.put(topic, new TopicSync(topic));
            }finally {
                topicSyncLock.writeLock().unlock();
            }
        }
    }

    /**
     * remove topic resources in topic partitions selector map, topic synchronization map and topic subscribed.
     * during remove topics, put topic should be blocked.
     * @param expiredTopics expired topics to removed
     * @return topic set removed
     */
    public void removeTopics(Set<String> expiredTopics) {
        if (null != expiredTopics && expiredTopics.size() > 0) {
            topicSyncLock.writeLock().lock();
            try {
                for (String topic : expiredTopics) {
                    _removeTopic(topic);
                }
            }finally {
                topicSyncLock.writeLock().unlock();
            }
        }
    }

    private void _removeTopic(String topic) {
        topic_2_partitionsSelector.remove(topic);
        topicSynMap.remove(topic);
        logger.info("{} removed from consumer subscribe.", topic);
    }

    @Override
    public void incoming(final NSQFrame frame, final NSQConnection conn) throws NSQException {
        if (frame == null) {
            logger.error("The frame is null because of SDK's bug in the {}", this.getClass().getName());
            return;
        }
        switch (frame.getType()) {
            case RESPONSE_FRAME: {
                if (frame.isHeartBeat()) {
                    if(PERF_LOG.isDebugEnabled())
                        PERF_LOG.debug("heartbeat received from {}.", conn.getAddress());
                    ChannelFuture nopFuture = conn.command(Nop.getInstance());
                    nopFuture.addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if(future.isSuccess()) {
                                if (PERF_LOG.isDebugEnabled())
                                    PERF_LOG.debug("nop response to {}.", conn.getAddress());
                            }else{
                                logger.error("Fail to response to heartbeat from {}.", conn.getAddress());
                            }
                        }
                    });
                }
                break;
            }
            case ERROR_FRAME: {
                logger.warn("Error-Frame from {} , frame: {}", conn.getAddress(), frame);
                if (Role.Consumer == this.role && !conn.getConfig().isOrdered()) {
                    conn.declineExpectedRdy();
                }
                break;
            }
            default: {
                logger.warn("Invalid frame-type from {} , frame-type: {} , frame: {}", conn.getAddress(), frame.getType(), frame);
            }
        }
    }

    @Override
    public void backoff(NSQConnection conn) {
        conn.command(new Rdy(0));
    }

    /**
     * function try fetching nsqd tcp addresses for pass in topic, sharding ID, and if it is writing
     * @param topic             topic
     * @param topicShardingIDs  shartdingID, default value is {@link Message#NO_SHARDING}
     * @param write             write access control, {@link Boolean#TRUE} for write and otherwise read.
     * @return array of nsqd nodes associated with passin topic
     * @throws NSQException exception raised in get nsqd node from lookup or nsqd partition node not found
     */
    public Address[] getPartitionNodes(Topic topic, Object[] topicShardingIDs, boolean write) throws NSQException, InterruptedException {
        IPartitionsSelector aPs;
        List<Address> nodes = new ArrayList<>();

        while (true) {
            aPs = topic_2_partitionsSelector.get(topic.getTopicText());
            if (null != aPs) {
                Partitions[] partitions;
                if (write)
                    partitions = aPs.choosePartitions();
                else
                    partitions = aPs.dumpAllPartitions();
                //if partitions returned from choose partitions, means there is only one Partitions
                for (Partitions aPartitions : partitions) {
                    //what is a valid Partitions
                    if (null != aPartitions && aPartitions.hasAnyDataNodes()) {
                        //for partitions
                        if (write) {
                            if (aPartitions.hasPartitionDataNodes() && topicShardingIDs[0] != Message.NO_SHARDING) {
                                int partitionNum = aPartitions.getPartitionNum();
                                int partitionId = topic.calculatePartitionIndex(topicShardingIDs[0], partitionNum);
                                //index out of boundry
                                Address addr = aPartitions.getPartitionAddress(partitionId);
                                nodes.add(addr);
                                //all data nodes
                            } else {
                                List<Address> address = aPartitions.getAllDataNodes();
                                nodes.addAll(address);
                            }
                        } else {
                            if (aPartitions.hasPartitionDataNodes() && topicShardingIDs[0] != Message.NO_SHARDING) {
                                for (Object partitionId : topicShardingIDs) {
                                    long partIdLong = (long) partitionId;
                                    Address addr = aPartitions.getPartitionAddress((int) partIdLong);
                                    nodes.add(addr);
                                }
                            } else {
                                //read from all data nodes
                                List<Address> address = aPartitions.getAllDataNodes();
                                nodes.addAll(address);
                            }
                        }
                    }
                }
                return nodes.toArray(new Address[0]);
            } else {
                long start = 0L;
                if (PERF_LOG.isDebugEnabled())
                    start = System.currentTimeMillis();
                this.topicSyncLock.readLock().lock();
                try {
                    TopicSync ts = this.topicSynMap.get(topic.getTopicText());
                    //topic sync is removed by topic cleaner
                    if (null == ts) {
                        logger.warn("topic sync for {} does not exist. Try getting partition info in another round.", topic);
                        return null;
                    }

                    if (!ts.tryLock()) {
                        Thread.sleep(TOPIC_PARTITION_TIMEOUT);
                        logger.info("Try again for partition selector for topic {}", topic.getTopicText());
                        //partition selector is being updating, try again from top
                        continue;
                    }

                    //update topic 2 partition map
                    try {
                        aPs = lookup.lookup(topic.getTopicText(), this.useLocalLookupd, false);
                        if (null != aPs) {
                            topic_2_partitionsSelector.put(topic.getTopicText(), aPs);
                            for (Partitions aPartitions : aPs.dumpAllPartitions()) {
                                //for partitions
                                if (aPartitions.hasPartitionDataNodes() && topicShardingIDs[0] != Message.NO_SHARDING) {
                                    int partitionNum = aPartitions.getPartitionNum();

                                    if (write) {
                                        int partitionId = topic.calculatePartitionIndex(topicShardingIDs[0], partitionNum);
                                        //index out of boundry
                                        Address addr = aPartitions.getPartitionAddress(partitionId);
                                        nodes.add(addr);
                                    } else if ((long) topicShardingIDs[0] >= 0) {
                                        for (Object partitionId : topicShardingIDs) {
                                            long partIdLong = (long) partitionId;
                                            Address addr = aPartitions.getPartitionAddress((int) partIdLong);
                                            nodes.add(addr);
                                        }
                                    }
                                    //all data nodes
                                } else {
                                    List<Address> address = aPartitions.getAllDataNodes();
                                    nodes.addAll(address);
                                }
                            }
                            return nodes.toArray(new Address[0]);
                        }
                        throw new NSQInvalidTopicException(topic.getTopicText());
                    } finally {
                        ts.unlock();
                    }
                } finally {
                    this.topicSyncLock.readLock().unlock();
                    if (PERF_LOG.isDebugEnabled()) {
                        PERF_LOG.debug("Partition selector update for topic {} ends in {} milliSec.", topic.getTopicText(), System.currentTimeMillis() - start);
                    }
                }
            }
        }
    }

    /**
     * Invalidate pass in topic related partitions selector, in {@link com.youzan.nsq.client.Producer} side.
     * @param topic topic
     */
    public void invalidatePartitionsSelector(final String topic) {
        if(null == topic)
            return;
        long now = System.currentTimeMillis();
        Long lastInvalidated = ps_lastInvalidated.get(topic);
        if (null != lastInvalidated && now - lastInvalidated <= _INTERVAL_IN_SECOND * 2) {
            logger.info("Partition selector for {} has been invalidated in last {} seconds", topic, _INTERVAL_IN_SECOND * 2);
            return;
        } else {
            ps_lastInvalidated.put(topic, now);
        }

        TopicSync ts = topicSynMap.get(topic);
        //ts is updating, invalidate is ok to exit
        if(!ts.tryLock()) {
            logger.info("partitions selector for topic {} is updating, invalidation exit.");
            return;
        }

        try {
            //force lookup here, and leaving update mapping from topic to partition to newDataNodes process.
            IPartitionsSelector aPs = lookup.lookup(topic, this.useLocalLookupd, false);
            if (null != aPs)
                topic_2_partitionsSelector.put(topic, aPs);
        } catch (NSQException e){
            logger.warn("Could not fetch lookup info for topic: {} at this moment, lookup info may not be ready.", topic);
        } finally {
            ts.unlock();
        }
    }

    @Override
    public boolean validateHeartbeat(NSQConnection conn) {
        final ChannelFuture future = conn.command(Nop.getInstance());
        return null != future && future.awaitUninterruptibly(2000, TimeUnit.MILLISECONDS) ? future.isSuccess() : false;
    }

    @Override
    public Set<NSQConnection> clearDataNode(Address address) {
        return null;
    }

    @Override
    public void close() {
        if(lock.tryLock()) {
            try {
                lookup.close();
                scheduler.shutdownNow();
                topic_2_partitionsSelector.clear();
                ps_lastInvalidated.clear();
                topicSynMap.clear();
            } finally {
                lock.unlock();
            }
        }
    }

    public void close(NSQConnection conn) {
    }

}
