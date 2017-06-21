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
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The intersection between {@link  com.youzan.nsq.client.Producer} and {@link com.youzan.nsq.client.Consumer}.
 *
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
@ThreadSafe
public class NSQSimpleClient implements Client, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(NSQSimpleClient.class);
    private static final Logger PERF_LOG = LoggerFactory.getLogger(NSQSimpleClient.class.getName() + ".perf");

    private final static AtomicInteger CLIENT_ID = new AtomicInteger(0);
    private int lookupLocalID = -1;
    private static final IPartitionsSelector EMPTY = new SimplePartitionsSelector(null);
    //maintain a mapping from topic to producer broadcast addresses
    private final Set<String> topicSubscribed = new HashSet<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<String, TopicSync> topicSynMap = new ConcurrentHashMap<>();
    private final Map<String, IPartitionsSelector> topic_2_partitionsSelector = new ConcurrentHashMap<>();
    private final long TOPIC_PARTITION_TIMEOUT = 500L;

    private final Map<String, Long> ps_lastInvalidated = new ConcurrentHashMap<>();

    private volatile boolean started;
    private final ScheduledExecutorService scheduler = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory(this.getClass().getName(), Thread.MAX_PRIORITY));

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

    private static final String SIMPLECLIENT_META_FORMAT = "%s: [" +
            "started: %b," +
            "\t" + "role: %s," +
            "\t" + "userLocalLookupd: %b," +
            "\t" + "lookupLocalID: %d," +
            "\t" + "topic size: %d," + "]";

    /**
     * return meta data of current simple client and {@link Client} current simple client belongs to.
     * @return string meta data of simple client.
     */
    public String toString() {
        String toString = String.format(SIMPLECLIENT_META_FORMAT, super.toString(), this.started, this.role.getRoleTxt(), this.useLocalLookupd, this.lookupLocalID, this.topicSubscribed.size());
        return toString;
    }

    @Override
    public synchronized void start() {
        if (!started) {
            started = true;
            LookupAddressUpdate.getInstance(true).keepListLookup();
            keepDataNodes();
        }
        started = true;
    }

    /**
     * return lookupLocalId of current SimpleClient.
     * @return lookupLocalID
     */
    public int getLookupLocalID() {
        return this.lookupLocalID;
    }

    private void keepDataNodes() {
        //delay in data node loop, 1 is a minimum for {@link LookupAddressUpdate} to kickoff
//        final int delay = _r.nextInt(3) + 1; // seconds
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    newDataNodes();
                } catch (NSQException e) {
                    logger.error("Error fetching data node. Process restarts in another round...", e);
                }
            }
        }, 0, _INTERVAL_IN_SECOND, TimeUnit.SECONDS);
//        logger.info("Data node maintain loop starts in {} seconds.", delay);
    }

    private void newDataNodes() throws NSQException {
        //clear partitions does not valid(topic has no partition attached)
        //only newDataNodes could remove
//        lock.writeLock().lock();
//        try {
//            Set<Topic> brokenTopic = new HashSet<>();
//            for (Map.Entry<Topic, IPartitionsSelector> pair : topic_2_partitionsSelector.entrySet()) {
//                if (pair.getValue() == null) {
//                    brokenTopic.add(pair.getKey());
//                }
//            }
//            for (Topic topic : brokenTopic) {
//                topic_2_partitionsSelector.remove(topic);
//                logger.info("{} removed from topic to partitions selector mapping.");
//            }
//        } finally {
//            lock.writeLock().unlock();
//        }

        //gather topics from topic_2_partitions
        final Set<String> topics = new HashSet<>();
        lock.readLock().lock();
        try {
            //not ant topic is subscribed. process ends
            if(this.role == Role.Consumer && topicSubscribed.size() == 0) {
                logger.warn("No any subscribed topic is found, Consumer may not subscribe any topic. Data node updating process ends.");
                return;
            }

            for (String topic : topicSubscribed) {
                topics.add(topic);
            }
        } finally {
            lock.readLock().unlock();
        }

        //lookup gatheres topics
        for (String topic : topics) {
            TopicSync ts = topicSynMap.get(topic);
            if (null == ts) {
                logger.info("Partitions selector lock for {} do not exist", topic);
                continue;
            }
            //another process, producer or consumer is updating this partitions slelector, ok to skip
            if (!ts.tryLock()) {
                logger.info("Skip partitions selector for {} as it is hold by client", topic);
                continue;
            }

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
            }finally {
                ts.unlock();
            }
        }
    }

    public void putTopic(String topic) {
        if (topic == null) {
            throw new IllegalArgumentException("Topic is not allowed to be empty.");
        }

        lock.readLock().lock();
        try{
            if (topicSubscribed.contains(topic)) {
                return;
            }
        }finally {
            lock.readLock().unlock();
        }

        lock.writeLock().lock();
        try {
            if(!topicSubscribed.contains(topic)) {
                topicSubscribed.add(topic);
                if (!topicSynMap.containsKey(topic)) {
                    topicSynMap.put(topic, new TopicSync(topic));
                }

                if (!topic_2_partitionsSelector.containsKey(topic)) {
                    topic_2_partitionsSelector.put(topic, EMPTY);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * remove topic resources in topic partitions selector map, topic synchronization map and topic subscribed.
     * during remove topics, put topic should be blocked.
     * @param expiredTopics
     */
    public void removeTopics(Set<String> expiredTopics) {
        lock.writeLock().lock();
        try{
            for(String topic : expiredTopics) {
                _removeTopic(topic);
            }
        }finally {
            lock.writeLock().unlock();
        }
    }

    private void _removeTopic(String topic) {
        int retry = 3;
        TopicSync ts = topicSynMap.get(topic);
        while (!ts.tryLock()) {
            if (retry-- > 0) {
                try {
                    Thread.sleep(TOPIC_PARTITION_TIMEOUT);
                } catch (InterruptedException e) {
                    logger.error("interrupted while waiting for removing partitions selector for {}.", topic);
                }
            } else {
                logger.info("Remove partitions selector for {} out of retry.", topic);
                return;
            }
            //partitions selector is being updated
        }
        try {
            topicSubscribed.remove(topic);
            topic_2_partitionsSelector.remove(topic);
            logger.info("{} removed from consumer subscribe.", topic);
        }finally {
            //place it in finally block
            topicSynMap.remove(topic);
            ts.unlock();
        }
    }

    @Override
    @Deprecated
    public void incoming(final NSQFrame frame, final NSQConnection conn) throws NSQException {
        if (frame == null) {
            logger.info("The frame is null because of SDK's bug in the {}", this.getClass().getName());
            return;
        }
        switch (frame.getType()) {
            case RESPONSE_FRAME: {
                final String resp = frame.getMessage();
                if (Response._HEARTBEAT_.getContent().equals(resp)) {
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
                if (Role.Consumer == this.role && !conn.getConfig().isOrdered() && conn.getConfig().isConsumerSlowStart()) {
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

    /**
     * function try fetch nsqd tcp addresses for pass in topic, sharding ID, and access(r/w)
     * @param topic             topic
     * @param topicShardingIDs  shartdingID, default value is {@link Message#NO_SHARDING}
     * @param write             access control(r/w)
     * @return array of nsqd
     * @throws NSQException exception raised in get nsqd node from lookup or nsqd partition node not found
     */
    public Address[] getPartitionNodes(Topic topic, Object[] topicShardingIDs, boolean write) throws NSQException, InterruptedException {
        IPartitionsSelector aPs;
        List<Address> nodes = new ArrayList<>();

        QueryPS:
        while (true) {
            aPs = topic_2_partitionsSelector.get(topic.getTopicText());
            if (aPs != EMPTY && null != aPs) {
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
                TopicSync ts = this.topicSynMap.get(topic.getTopicText());

                assert null != ts;
                if (null == ts) {
                    logger.error("topic partitions selector synchronization for {} is null.", topic.getTopicText());
                }

                if (!ts.tryLock()) {
                    Thread.sleep(TOPIC_PARTITION_TIMEOUT);
                    //access to topic 2 partition map
                    logger.info("Try again for partition selector for topic {}", topic.getTopicText());
                    continue QueryPS;
                }

                //update topic 2 partition map
                long start = 0L;
                try {
                    if(PERF_LOG.isDebugEnabled())
                        start = System.currentTimeMillis();
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
                    if(PERF_LOG.isDebugEnabled()) {
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
        if (now - lastInvalidated <= _INTERVAL_IN_SECOND * 2) {
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
        return future.awaitUninterruptibly(2000, TimeUnit.MILLISECONDS) && future.isSuccess();
    }

    @Override
    public Set<NSQConnection> clearDataNode(Address address) {
        return null;
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            lookup.close();
            topic_2_partitionsSelector.clear();
            ps_lastInvalidated.clear();
            topicSubscribed.clear();
            topicSynMap.clear();
            scheduler.shutdownNow();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void close(NSQConnection conn) {
    }

}
