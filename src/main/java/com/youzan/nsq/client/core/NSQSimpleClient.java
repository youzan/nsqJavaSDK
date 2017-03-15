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
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    //maintain a mapping from topic to producer broadcast addresses
    private final Set<Topic> topicSubscribed = new HashSet<>();
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

    /**
     * return lookupLocalId of current SimpleClient.
     * @return lookupLocalID
     */
    public int getLookupLocalID() {
        return this.lookupLocalID;
    }

    private void keepDataNodes() {
        //delay in data node loop, 1 is a minimum for {@link LookupAddressUpdate} to kickoff
        final int delay = _r.nextInt(3) + 1; // seconds
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
        logger.info("Data node maintain loop starts in {} seconds.", delay);
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
                logger.info("{} removed from topic to partitions selector mapping.");
            }
        } finally {
            lock.writeLock().unlock();
        }

        //gather topics from topic_2_partitions
        final Set<Topic> topics = new HashSet<>();
        try {
            lock.readLock().lock();
            //not ant topic is subscribed. process ends
            if(this.role == Role.Consumer && topicSubscribed.size() == 0) {
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
                final IPartitionsSelector aPs = lookup.lookup(topic, this.useLocalLookupd, false);
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

        if (0 == newDataNodes.size() && this.role == Role.Consumer) {
            logger.warn("No any data node found for NSQ client {} in this try.", this.toString());
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
            if(!topicSubscribed.contains(topic)) {
                topicSubscribed.add(topic);
                if (!topic_2_partitionsSelector.containsKey(topic)) {
                    topic_2_partitionsSelector.put(topic, EMPTY);
                }
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
            topicSubscribed.remove(topic);
            logger.info("{} removed from consumer subscribe.", topic);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * remove multiple topics from simple client, invoker of this function needs to make sure pass in topics are valid.
     *
     * @param topics {@link Collection} of {@link Topic} to be removed.
     */
    public void removeTopics(final Collection<Topic> topics) {
        assert null != topics;
        lock.writeLock().lock();
        try {
            for (Topic topic : topics) {
                topic_2_partitionsSelector.remove(topic);
                topicSubscribed.remove(topic);
                logger.info("{} removed from consumer subscribe.", topic);
            }
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

    public Address[] getPartitionNodes(Topic topic, Object topicShardingID, boolean write) throws NSQException {
        IPartitionsSelector aPs;
        List<Address> nodes = new ArrayList<>();
        lock.readLock().lock();
        try {
            aPs = topic_2_partitionsSelector.get(topic);
            if(aPs != EMPTY && null != aPs) {
                Partitions[] partitions;
                if(write)
                    partitions = aPs.choosePartitions();
                else
                    partitions = aPs.dumpAllPartitions();
                //if partitions returned from choose partitions, means there is only one Partitions
                for(Partitions aPartitions : partitions) {
                    //what is a valid Partitions
                    if (null != aPartitions && aPartitions.hasAnyDataNodes()) {
                        //for partitions
                        if (write) {
                            if (aPartitions.hasPartitionDataNodes() && topicShardingID != Message.NO_SHARDING) {
                                int partitionNum = aPartitions.getPartitionNum();
                                int partitionId = topic.updatePartitionIndex(topicShardingID, partitionNum);
                                //index out of boundry
                                Address addr = aPartitions.getPartitionAddress(partitionId);
                                nodes.add(addr);
                                //all data nodes
                            } else {
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
            aPs = lookup.lookup(topic, this.useLocalLookupd, false);
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
                    if (aPartitions.hasPartitionDataNodes() && topicShardingID != Message.NO_SHARDING) {
                        int partitionNum = aPartitions.getPartitionNum();
                        int partitionId = topic.updatePartitionIndex(topicShardingID, partitionNum);
                        //index out of boundry
                        Address addr = aPartitions.getPartitionAddress(partitionId);
                        nodes.add(addr);
                        //all data nodes
                    } else {
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

    /**
     * Invalidate pass in topic related partitions selector, in {@link com.youzan.nsq.client.Producer} side.
     * @param topic topic
     * @param address address to remove from dataNodes
     */
    public void invalidatePartitionsSelector(final Topic topic, final Address address) {
        if(null == topic || null == address)
            return;
        try {
            //force lookup here, and leaving update mapping from topic to partition to newDataNodes process.
            lookup.lookup(topic, this.useLocalLookupd, true);
        }catch (NSQException e){
            logger.warn("Could not fetch lookup info for topic: {} at this moment, lookup info may not be ready.", topic);
        }

        //remove directly, as getPartitionNode will add topic back.
        topic_2_partitionsSelector.put(topic, EMPTY);
        dataNodes.remove(address);
    }

    /**
     * Invalidate pass in topic related partitions selector, in {@link com.youzan.nsq.client.Consumer} side.
     * @param topic topic
     */
    public void invalidatePartitionsSelector(final Topic topic) {
        if(null == topic)
            return;
        try {
            //force lookup here, and leaving update mapping from topic to partition to newDataNodes process.
            lookup.lookup(topic, this.useLocalLookupd, true);
        }catch (NSQException e){
            logger.warn("Could not fetch lookup info for topic: {} at this moment, lookup info may not be ready.", topic);
        }

        topic_2_partitionsSelector.put(topic, EMPTY);
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
            dataNodes.clear();
            topic_2_partitionsSelector.clear();
            topicSubscribed.clear();
            scheduler.shutdownNow();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void close(NSQConnection conn) {
    }

}
