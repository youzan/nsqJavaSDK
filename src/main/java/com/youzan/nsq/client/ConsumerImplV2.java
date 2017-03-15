package com.youzan.nsq.client;

import com.youzan.nsq.client.core.LookupAddressUpdate;
import com.youzan.nsq.client.core.NSQConnection;
import com.youzan.nsq.client.core.NSQSimpleClient;
import com.youzan.nsq.client.core.RdySpectrum;
import com.youzan.nsq.client.core.command.*;
import com.youzan.nsq.client.core.pool.consumer.FixedPool;
import com.youzan.nsq.client.entity.*;
import com.youzan.nsq.client.exception.*;
import com.youzan.nsq.client.network.frame.ErrorFrame;
import com.youzan.nsq.client.network.frame.MessageFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.nsq.client.network.frame.NSQFrame.FrameType;
import com.youzan.nsq.client.network.frame.OrderedMessageFrame;
import com.youzan.util.HostUtil;
import com.youzan.util.IOUtil;
import com.youzan.util.NamedThreadFactory;
import com.youzan.util.ThreadSafe;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <pre>
 * Expose to Client Code. Connect to one cluster(includes many brokers).
 * </pre>
 * <p>
 * Use JDK7.
 * The logical preciseness is more important than the performance.
 *
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public class ConsumerImplV2 implements Consumer {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerImplV2.class);
    private static final Logger PERF_LOG = LoggerFactory.getLogger(ConsumerImplV2.class.getName() + ".perf");

    private static final int MAX_CONSUME_RETRY = 3;
    private final Object lock = new Object();
    private boolean started = false;
    private boolean closing = false;
    private volatile long lastConnecting = 0L;


    private final AtomicInteger received = new AtomicInteger(0);
    private final AtomicInteger success = new AtomicInteger(0);
    private final AtomicInteger finished = new AtomicInteger(0);
    private final AtomicInteger re = new AtomicInteger(0); // have done reQueue
    private final AtomicInteger queue4Consume = new AtomicInteger(0); // have done reQueue


    /*-
     * =========================================================================
     * Topic set for containing topics user subscribes.
     *
     * =========================================================================
     */
    private final SortedSet<Topic> topics = new TreeSet<>();

    // client subscribes
    /*-
      address_2_pool maps NSQd address to connections to topics specified by consumer.
      Basically, consumer subscribe multi-topics to one NSQd, hence there is a mapping
      between NSQd address and connections to topics
     */
    private final ConcurrentHashMap<Address, FixedPool> address_2_pool = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory(this.getClass().getSimpleName(), Thread.NORM_PRIORITY));
    /*-
      * =========================================================================
      *                          Client delegates to me
      */
    private final MessageHandler handler;
    private final int WORKER_SIZE = Runtime.getRuntime().availableProcessors() * 4;
    private final ExecutorService executor = Executors.newFixedThreadPool(WORKER_SIZE,
            new NamedThreadFactory(this.getClass().getSimpleName() + "-ClientBusiness", Thread.MAX_PRIORITY));
    /*-
     *                           Client delegates to me
     * =========================================================================
     */
    private final Rdy DEFAULT_RDY;
    private final Rdy MEDIUM_RDY;
    private final Rdy LOW_RDY = new Rdy(1);
    private volatile Rdy currentRdy = LOW_RDY;
    private volatile boolean autoFinish = true;


    private final NSQSimpleClient simpleClient;
    private final NSQConfig config;

    /**
     * @param config  NSQConfig
     * @param handler the client code sets it
     */
    public ConsumerImplV2(NSQConfig config, MessageHandler handler) {
        this.config = config;
        this.handler = handler;
        this.simpleClient = new NSQSimpleClient(Role.Consumer, this.config.getUserSpecifiedLookupAddress());

        int messagesPerBatch;
        if (!config.isConsumerSlowStart())
            messagesPerBatch = config.getRdy();
        else
            messagesPerBatch = 1;
        DEFAULT_RDY = new Rdy(Math.max(messagesPerBatch, 1));
        MEDIUM_RDY = new Rdy(Math.max((int) (messagesPerBatch * 0.5D), 1));
        currentRdy = DEFAULT_RDY;
        logger.info("Initialize the Rdy, that is {}", currentRdy);
    }

    @Override
    public void subscribe(String... topics) {
        subscribeTopics(topics);
    }

    @Override
    public void subscribe(Topic... topics) {
        if (topics == null) {
            return;
        }
        for (Topic topic : topics) {
            //copy a new topic in consumer, as address 2 partition mapping need maintained in topic, and we do not
            //expect to expose that to user.
            Topic topicCopy = Topic.newInstacne(topic);
            this.topics.add(topicCopy);
            simpleClient.putTopic(topicCopy);
        }
    }

    private void subscribeTopics(String... topics) {
        if (topics == null) {
            return;
        }
        for (String topicStr : topics) {
            Topic topic = new Topic(topicStr);
            this.topics.add(topic);
            simpleClient.putTopic(topic);
        }
    }

    @Override
    public void start() throws NSQException {
        if (this.config.getConsumerName() == null || this.config.getConsumerName().isEmpty()) {
            throw new IllegalArgumentException("Consumer Name is blank! Please check it!");
        }
        if (this.topics.isEmpty()) {
            logger.warn("No topic subscribed.");
        }
        synchronized (lock) {
            if (!this.started) {
                //set subscribe status
                // create the pools
                //simple client starts and LookupAddressUpdate instance initialized there.
                this.simpleClient.start();
                if(this.config.getUserSpecifiedLookupAddress()) {
                    LookupAddressUpdate.getInstance().setUpDefaultSeedLookupConfig(this.simpleClient.getLookupLocalID(), this.config.getLookupAddresses());
                }

                // -----------------------------------------------------------------
                //                       First, async keep
                keepConnecting();
                connect();
                // -----------------------------------------------------------------
            }
            this.started = true;
        }
        logger.info("The consumer has been started.");
    }

    /**
     * schedule action
     */
    private void keepConnecting() {
        final int delay = _r.nextInt(60); // seconds
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                if (closing) {
                    return;
                }
                try {
                    connect();
                } catch (Exception e) {
                    logger.error("Exception", e);
                }
                logger.info("Client received {} messages , success {} , finished {} , queue4Consume {}, reQueue explicitly {}. The values do not use a lock action.", received, success, finished, queue4Consume, re);
            }
        }, delay, _INTERVAL_IN_SECOND, TimeUnit.SECONDS);
    }

    /**
     * Connect to all the brokers with the config, making sure the new is OK
     * and the old is clear.
     */
    private void connect() throws NSQException {
        //which equals to: System.currentTimeMillis() - lastConnecting < TimeUnit.SECONDS.toMillis(_INTERVAL_IN_SECOND))
        //rest logic performs only when time elapse larger than _INTERNAL_IN_SECOND
        if (System.currentTimeMillis() < lastConnecting + TimeUnit.SECONDS.toMillis(_INTERVAL_IN_SECOND)) {
            return;
        }
        lastConnecting = System.currentTimeMillis();
        if (!this.started) {
            if (closing) {
                logger.info("You have closed the consumer sometimes ago!");
            }
            return;
        }
        if (this.topics.isEmpty()) {
            logger.error("Are you kidding me? You did not subscribe any topic. Please check it right now!");
            // Still check the resources , do not return right now
        }

        //broken set to collect Address of connection which is not connected
        final Set<Address> broken = new HashSet<>();
        final ConcurrentHashMap<Address, Set<Topic>> address_2_topics = new ConcurrentHashMap<>();
        final Set<Address> targetAddresses = new TreeSet<>();
        /*-
         * =====================================================================
         *                    Clean up the broken connections
         * =====================================================================
         */
        for (final FixedPool p : address_2_pool.values()) {
            final List<NSQConnection> connections = p.getConnections();
            for (final NSQConnection c : connections) {
                try {
                    if (!c.isConnected()) {
                        c.close();
                        broken.add(c.getAddress());
                    }
                } catch (Exception e) {
                    logger.error("While detecting broken connections, Exception:", e);
                }
            }
        }
        /*-
         * =====================================================================
         *                                First Step:
         *                          干掉Broken Brokers.
         * =====================================================================
         */
        for (Address address : broken) {
            clearDataNode(address);
        }
        /*-
         * =====================================================================
         *                            Get the relationship
         * =====================================================================
         */
        for (Topic topic : topics) {
            // maybe a exception occurs
            int maxRetry = MAX_CONSUME_RETRY;
            int idx = 0;
            Address[] partitionDataNodes = null;
            while(null == partitionDataNodes) {
                try {
                    partitionDataNodes = simpleClient.getPartitionNodes(topic, Message.NO_SHARDING, false);
                } catch (NSQLookupException lookupe) {
                    logger.warn("Hit a invalid lookup address, retry another. Has retried: {}", idx);
                    if(idx++ >= MAX_CONSUME_RETRY){
                        throw lookupe;
                    }
                }
            }
            final List<Address> dataNodeLst = Arrays.asList(partitionDataNodes);
            for (Address a : dataNodeLst) {
                final Set<Topic> tmpTopics;
                if (address_2_topics.containsKey(a)) {
                    tmpTopics = address_2_topics.get(a);
                } else {
                    tmpTopics = new TreeSet<>();
                    address_2_topics.put(a, tmpTopics);
                }
                tmpTopics.add(topic);
            }
            targetAddresses.addAll(dataNodeLst);
        }
        logger.debug("address_2_topics: {}", address_2_topics);

        final Set<Address> oldAddresses = new TreeSet<>(this.address_2_pool.keySet());
        if (targetAddresses.isEmpty() && oldAddresses.isEmpty()) {
            logger.debug("No new addresses and old addresses so that no need to connect.");
            return;
        }
        logger.debug("Prepare to connect new data-nodes(NSQd): {} , old data-nodes(NSQd): {}", targetAddresses,
                oldAddresses);
        if (targetAddresses.isEmpty()) {
            logger.error("Get the current new DataNodes (NSQd) but it is empty. It will create a new pool next time! Now begin to clear up old data-nodes(NSQd) {}", oldAddresses);
        }
        /*-
         * =====================================================================
         *                                Step :
         *                    以old data-nodes为主的差集: 删除Brokers
         *                           <<<比新建优先级高>>>
         * =====================================================================
         */
        final Set<Address> except2 = new HashSet<>(oldAddresses);
        except2.removeAll(targetAddresses);
        if (!except2.isEmpty()) {
            logger.debug("Delete unused data-nodes: {}", except2);
            for (Address address : except2) {
                clearDataNode(address);
            }
        }
        /*-
         * =====================================================================
         *                                Step :
         *                    以new data-nodes为主的差集: 新建Brokers
         * =====================================================================
         */
        final Set<Address> except1 = new HashSet<>(targetAddresses);
        except1.removeAll(oldAddresses);
        if (!except1.isEmpty()) {
            logger.debug("Get new data-nodes: {}", except1);
            for (Address address : except1) {
                try {
                    final Set<Topic> topics = address_2_topics.get(address);
                    connect(address, topics);
                } catch (Exception e) {
                    logger.error("Exception", e);
                    clearDataNode(address);
                }
            }
        }
        /*-
         * =====================================================================
         *                                Last Step:
         *                          Clean up local resources
         * =====================================================================
         */
        except1.clear();
        except2.clear();
        oldAddresses.clear();
        broken.clear();
        address_2_topics.clear();
        targetAddresses.clear();
    }

    /**
     * create TCP connections
     * one connection to one topic
     * subscribe and get all the connections
     * init consumer connection
     *
     * @param address data-node(NSQd)
     * @param topics  client cares about the specified topics
     */
    private void connect(Address address, Set<Topic> topics) throws Exception {
        if (topics == null || topics.isEmpty()) {
            return;
        }
        synchronized (lock) {
            if (closing) {
                return;
            }
            final int topicSize = topics.size();
            //set connection size to 1 for consumer, 1 connection per topic/partition(dataNode)
            //one topic may has more than one partition, they are managed
            //calculate connection size
            int connectionSizeCal = topics.size();
//            try {
//                for (Topic aTopic : topics) {
//                    //what happen if there is no mapping 2 partition(when connect to old nsqd)
//                    SortedMap<Address, SortedSet<Integer>> addr2Partitions = aTopic.getNsqdAddr2Partition();
//                    Set<Integer> partitionSet = null;
//                    if(null != addr2Partitions)
//                        partitionSet = addr2Partitions.get(address);
//                    //if address is a old nsqd, should not return any partition info
//                    if (null != partitionSet)
//                        connectionSizeCal += partitionSet.size();
//                    else {
//                        //for old nsqd
//                        connectionSizeCal += 1;
//                    }
//                }
//            } catch (NullPointerException npe) {
//                logger.warn("Address to partition mapping is updated in topics. Connect process will try later.");
//                return;
//            }
            final int connectionSize = connectionSizeCal;
            if (connectionSize <= 0) {
                return;
            }

            final Topic[] topicArray = new Topic[topicSize];
            topics.toArray(topicArray);
            if (topics.size() != topicArray.length) {
                // concurrent problem
                return;
            }

            final FixedPool pool = new FixedPool(address, connectionSize, this, config);
            address_2_pool.put(address, pool);
            pool.prepare(this.config.isOrdered());
            List<NSQConnection> connections = pool.getConnections();
            if (connections == null || connections.isEmpty()) {
                logger.info("TopicSize: {} , Address: {} , Connection-Size: {} . The pool is empty.", topicSize, address, connectionSize);
                return;
            }
            logger.info("TopicSize: {} , Address: {} , Connection-Size: {} , Topics: {}", topicSize, address, connectionSize, topics);
            int connectionIdx = 0;
            try {
                for (int i = 0; i < topicSize; i++) {
                    final Topic topic = topicArray[i];

                    // init( connection, topic ) , let it be a consumer connection
                    final NSQConnection connection = connections.get(connectionIdx++);
                    try {
                        connection.init(topic);
                    } catch (Exception e) {
                        connection.close();
                        if (!closing) {
                            throw new NSQNoConnectionException("Creating a connection and having a negotiation fails!", e);
                        }
                    }
                    if (!connection.isConnected()) {
                        connection.close();
                        if (!closing) {
                            throw new NSQNoConnectionException("Pool failed in connecting to NSQd! Closing: !" + closing);
                        }
                    } else {
                        Sub command;
                        if (address.getPartition() > -1)
                            command = createSubCmd(address.getPartition(), topic, this.config.getConsumerName());
                        else
                            command = createSubCmd(topic, this.config.getConsumerName());
                        final NSQFrame frame = connection.commandAndGetResponse(command);
                        handleResponse(frame, connection);
                        //as there is no success response from nsq, command is enough here
                        connection.command(currentRdy);
                    }
                    logger.info("Done. Current connection index: {}", i);
                }
            }catch(NullPointerException npe) {
                logger.warn("Address to partition mapping is updating in topics. Connect process try later.");
                clearDataNode(address);
            }
        }
    }

    private Sub createSubCmd(final Topic topic, String channel) {
        if (this.config.isOrdered())
            return new SubOrdered(topic, channel);
        else
            return new Sub(topic, channel);
    }

    private Sub createSubCmd(int partitionIdOverride, final Topic topic, String channel) {
        if (this.config.isOrdered())
            return new SubOrdered(topic, channel, partitionIdOverride);
        else
            return new Sub(topic, channel);
    }

    /**
     * No any exception
     * The method does not close the TCP-Connection
     *
     * @param address the data-node(NSQd)'s address
     */
    public Set<NSQConnection> clearDataNode(Address address) {
        if (address == null) {
            return null;
        }
        if (!address_2_pool.containsKey(address)) {
            return null;
        }
        final Set<NSQConnection> clearConnections = new HashSet<>();
        final FixedPool pool = address_2_pool.get(address);
        address_2_pool.remove(address);
        if (pool != null) {
            final List<NSQConnection> connections = pool.getConnections();
            if (connections != null) {
                for (NSQConnection connection : connections) {
                    clearConnections.add(connection);
                    try {
                        cleanClose(connection);
                    } catch (Exception e) {
                        logger.error("It can not backoff the connection! Exception:", e);
                    }
                }
            }
            pool.close();
        }
        return clearConnections;
    }

    @Override
    public void incoming(final NSQFrame frame, final NSQConnection conn) throws NSQException {
        if (frame == null) {
            return;
        }
        if (conn == null) {
            logger.warn("The consumer connection is closed and removed. {}", frame);
            return;
        }
        if (frame.getType() == FrameType.MESSAGE_FRAME) {
            received.incrementAndGet();
            final MessageFrame msg = (MessageFrame) frame;
            final NSQMessage message = createNSQMessage(msg, conn);
            //gather trace info
            //this.trace.handleMessage(message);
            if (TraceLogger.isTraceLoggerEnabled() && conn.getAddress().isHA())
                TraceLogger.trace(this, conn, message);
            if (this.config.isOrdered()
                    && !conn.checkOrder(message.getInternalID(), message.getDiskQueueOffset(), message)) {
                //order problem
                throw new NSQInvalidMessageException("Invalid internalID or diskQueueOffset in order mode.");
            }
            processMessage(message, conn);
        } else {
            simpleClient.incoming(frame, conn);
        }
    }

    private void processMessage(final NSQMessage message, final NSQConnection connection) {
        if (logger.isDebugEnabled()) {
            logger.debug(message.toString());
        }

        if (handler == null) {
            logger.error("No MessageHandler then drop the message {}", message);
            return;
        }
        try {
             executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        consume(message, connection);
                        success.incrementAndGet();
                    } catch (Exception e) {
                        IOUtil.closeQuietly(connection);
                        logger.error("Exception", e);
                    } finally {
                        queue4Consume.decrementAndGet();
                    }
                }
            });
            queue4Consume.incrementAndGet();
        } catch (RejectedExecutionException re) {
            try {
                backoff(connection);
                connection.command(new ReQueue(message.getMessageID(), 3));
                logger.info("Do a re-queue. MessageID:{}", message.getMessageID());
                resumeRateLimiting(connection, 0);
            } catch (Exception e) {
                logger.error("SDK can not handle it MessageID:{}, Exception:", message.getMessageID(), e);
            }
        }
        resumeRateLimiting(connection, 1000);
        if (!this.config.isOrdered() && this.config.isConsumerSlowStart()) {
            int newCount = RdySpectrum.increase(connection, connection.getCurrentRdyCount(), this.config.getRdy());
            connection.setCurrentRdyCount(newCount);
        }
    }

    private void resumeRateLimiting(final NSQConnection conn, final int delayInMillisecond) {
        if (executor instanceof ThreadPoolExecutor) {
            final ThreadPoolExecutor pools = (ThreadPoolExecutor) executor;
            final double threshold = pools.getActiveCount() / (1.0D * pools.getPoolSize());
            if (delayInMillisecond <= 0) {
                logger.info("Current status is not good. threshold: {}", threshold);
                boolean willSleep = false; // too , too busy
                if (threshold >= 0.9D) {
                    if (currentRdy == LOW_RDY) {
                        willSleep = true;
                    }
                    currentRdy = LOW_RDY;
                } else if (threshold >= 0.5D) {
                    currentRdy = MEDIUM_RDY;
                } else {
                    currentRdy = DEFAULT_RDY;
                }
                // Ignore the data-race
                ChannelFuture future = conn.command(currentRdy);
                if (willSleep) {
                    sleep(100);
                }
            } else {
                if (currentRdy != DEFAULT_RDY) {
                    logger.info("Current threshold state: {}", threshold);
                    scheduler.schedule(new Runnable() {
                        @Override
                        public void run() {
                            // restore the state
                            if (threshold <= 0.3D) {
                                currentRdy = DEFAULT_RDY;
                                conn.command(currentRdy);
                            }
                        }
                    }, delayInMillisecond, TimeUnit.MILLISECONDS);
                }
            }
        } else {
            logger.error("Initializing the executor is wrong.");
        }
        assert currentRdy != null;
    }

    /**
     * @param message    a NSQMessage
     * @param connection a NSQConnection
     */
    private void consume(final NSQMessage message, final NSQConnection connection) {
        boolean ok;
        boolean retry;
        long start = System.currentTimeMillis();
        try {
            handler.process(message);
            ok = true;
            retry = false;
        } catch (RetryBusinessException e) {
            ok = false;
            retry = true;
        } catch (Exception e) {
            ok = false;
            retry = false;
            logger.error("Client business has one error. Original message: {}. Exception:", message.getReadableContent(), e);
        }
        if (!ok && retry) {
            logger.warn("Client has told SDK to do again. {}", message);
            try {
                handler.process(message);
                ok = true;
            } catch (Exception e) {
                ok = false;
                retry = false;
                logger.error("Client business has required SDK to do again, but still has one error. Original message: {}. Exception:", message.getReadableContent(), e);
            }
        }
        long end = System.currentTimeMillis() - start;
        if(PERF_LOG.isDebugEnabled())
            PERF_LOG.debug("Message handler took {} milliSec to finish consuming message for connection {}. Success:{}, Retry:{}", end, connection.getAddress(), ok, retry);

        // The client commands ReQueue into NSQd.
        final Integer nextConsumingWaiting = message.getNextConsumingInSecond();
        // It is too complex.
        NSQCommand cmd = null;
        if (autoFinish) {
            // Either Finish or ReQueue
            if (ok) {
                // Finish
                cmd = new Finish(message.getMessageID());
                // do not log
            } else {
                // an error occurs
                if (nextConsumingWaiting != null) {
                    //check if max retry times reaches before requeue
                    // Post
                    if (!this.config.isOrdered() && message.getReadableAttempts() > this.config.getMaxRequeueTimes()) {
                        logger.warn("Message {} has reached max retry limitation: {}. it will be published to NSQ again for consume.", message, this.config.getMaxRequeueTimes());
                        //TODO: set up temporary publish
                        Producer producer = null;
                        try {
                            producer = compensationPublish(connection.getTopic(), message);
                            cmd = new Finish(message.getMessageID());
                        } catch (NSQException e) {
                            logger.error("Fail to publish compensation message for incoming message {}. Retry in next round.");
                        } finally {
                            if(null != producer)
                                producer.close();
                        }
                    }else {
                        // ReQueue
                        cmd = new ReQueue(message.getMessageID(), nextConsumingWaiting.intValue());
                        final byte[] id = message.getMessageID();
                        logger.info("Do a re-queue by SDK that is a default behavior. MessageID: {} , Hex: {}", id, message.newHexString(id));
                    }
                } else {
                    if (!this.config.isOrdered() && this.config.isConsumerSlowStart() && end > this.config.getMsgTimeoutInMillisecond()) {
                        logger.warn("It tooks {} milliSec to for message to be consumed by message handler, and exceeds message timeout in nsq config. Fin not be invoked as requeue from NSQ server is on its way.", end);
                        int currentRdyCnt = RdySpectrum.decrease(connection, connection.getCurrentRdyCount(), connection.getCurrentRdyCount() - 1);
                        connection.setCurrentRdyCount(currentRdyCnt);
                    } else {
                        // Finish: client explicitly sets NextConsumingInSecond is null
                        cmd = new Finish(message.getMessageID());
                        final byte[] id = message.getMessageID();
                        logger.info("Do a Finish by SDK, given that client process handler has failed and next consuming time elapse not specified. MessageID: {} , Hex: {}", id, message.newHexString(id));
                    }
                }
            }
        } else {
            // Client code does finish explicitly.
            // Maybe ReQueue, but absolutely no Finish
            if (!ok) {
                if (nextConsumingWaiting != null) {
                    // ReQueue
                    cmd = new ReQueue(message.getMessageID(), nextConsumingWaiting.intValue());
                    final byte[] id = message.getMessageID();
                    logger.info("Client does a re-queue explicitly. MessageID: {} , Hex: {}", id, message.newHexString(id));
                }
            } else {
                // ignore actions
                cmd = null;
            }
        }
        if (cmd != null) {
            final String cmdStr = cmd.toString();
            ChannelFuture future = connection.command(cmd);
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if(!future.isSuccess()){
                        logger.error("Fail to send {}. Message {} will be delivered to consumer in another round.", cmdStr, message.getMessageID());
                    }else if(PERF_LOG.isDebugEnabled()) {
                        PERF_LOG.debug("Command {} to {} for message {} sent.", cmdStr, connection.getAddress(), message.getMessageID());
                    }
                }
            });
            if (cmd instanceof Finish) {
                finished.incrementAndGet();
            } else {
                re.incrementAndGet();
            }
        }
        // Post
        //log warn
        if (!ok) {
            logger.warn("Exception occurs but you don't catch it! Please check it right now!!! {} , Original message: {}.", message, message.getReadableContent());
        }
    }

    private Producer compensationPublish(final Topic topic, final NSQMessage msg) throws NSQException {
        if(null == topic) {
            logger.warn("Could not create producer, pass in topic is null.");
            return null;
        }
        logger.info("Compensation publish to {}", topic);
        Producer producer = new ProducerImplV2(this.config);
        producer.start();
        Message newMsg = Message.create(topic, msg.getTraceID(), msg.getReadableContent());
        producer.publish(newMsg);
        logger.info("Compensation publish finished.");
        return producer;
    }

    @Override
    @ThreadSafe
    public void backoff(NSQConnection conn) {
        synchronized (conn) {
            simpleClient.backoff(conn);
        }
    }

    @Override
    public void close() {
        // ===================================================================
        //                        Gracefully Close
        // ===================================================================
        // Application will close the Netty IO EventLoop
        // Application will close the worker executor of the consumer
        // Application will all of the TCP-Connections
        // ===================================================================
        synchronized (lock) {
            started = false;
            closing = true;
            final Set<NSQConnection> connections = cleanClose();
            IOUtil.closeQuietly(simpleClient);
            scheduler.shutdownNow();
            executor.shutdown();
            try {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    logger.warn("Client handles a message over 10 sec.");
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            close(connections);
        }
        logger.info("The consumer has been closed.");
        LookupAddressUpdate.getInstance().closed();
    }


    private void disconnectServer(NSQConnection connection) {
        synchronized (connection) {
            if (!connection.isConnected()) {
                // It has been closed
                return;
            }
            try {
                final NSQFrame frame = connection.commandAndGetResponse(Close.getInstance());
                if (frame != null) {
                    handleResponse(frame, connection);
                }
            } catch (Exception e) {
                if (connection.isConnected()) {
                    logger.error("Sending CLS to the connection {} occurs an error. Exception:", connection, e);
                }
            } finally {
                if (connection.isConnected()) {
                    IOUtil.closeQuietly(connection);
                }
            }
        }
    }

    private void close(Set<NSQConnection> connections) {
        if (connections != null) {
            for (NSQConnection connection : connections) {
                disconnectServer(connection);
            }
        }
    }

    @ThreadSafe
    public void close(NSQConnection connection) {
        synchronized (connection) {
            try {
                cleanClose(connection);
                disconnectServer(connection);
            } catch (TimeoutException e) {
                logger.error("Backoff connection {} to the server, TimeoutException", connection);
                if (connection.isConnected()) {
                    IOUtil.closeQuietly(connection);
                }
            }
        }
    }

    private Set<NSQConnection> cleanClose() {
        final Set<NSQConnection> connections = new HashSet<>();
        final Set<Address> addresses = address_2_pool.keySet();
        if (!addresses.isEmpty()) {
            for (Address address : addresses) {
                Set<NSQConnection> tmp = clearDataNode(address);
                connections.addAll(tmp);
            }
        }
        address_2_pool.clear();
        topics.clear();
        return connections;
    }

    @ThreadSafe
    private void cleanClose(NSQConnection connection) throws TimeoutException {
        synchronized (connection) {
            if (connection.isConnected()) {
                backoff(connection);
            }
        }
    }

    private void handleResponse(NSQFrame frame, NSQConnection connection) {
        if (frame == null) {
            logger.warn("SDK bug: the frame is null.");
            return;
        }
        if (frame.getType() == FrameType.RESPONSE_FRAME) {
            return;
        }
        if (frame.getType() == FrameType.ERROR_FRAME) {
            final ErrorFrame err = (ErrorFrame) frame;
            logger.info("Connection: {} got one error {} , that is {}", connection, err, err.getError());
            switch (err.getError()) {
                case E_FAILED_ON_NOT_LEADER: {
                    //NSQ node return from lookup is not a leader. This caused by a expired cached lookup from {@link LookupAddressUpdate}
                }
                case E_FAILED_ON_NOT_WRITABLE: {
                }
                case E_TOPIC_NOT_EXIST: {
                    Address address = connection.getAddress();
                    for(Topic aTopic : this.topics) {
                        this.simpleClient.invalidatePartitionsSelector(aTopic);
                        logger.info("Partitions info for {} invalidated and related lookupd address force updated.");
                    }
                    this.simpleClient.clearDataNode(address);
                    clearDataNode(address);
                    logger.info("NSQInvalidDataNode. {}", frame);
                }
                default: {
                    logger.info("Unknown error type in ERROR_FRAME! {}", frame);
                }
            }
        }
    }

    @Override
    public boolean validateHeartbeat(NSQConnection conn) {
        if (!conn.isConnected()) {
            return false;
        }
        currentRdy = DEFAULT_RDY;
        final ChannelFuture future = conn.command(currentRdy);
        if (future.awaitUninterruptibly(config.getQueryTimeoutInMillisecond(), TimeUnit.MILLISECONDS)) {
            return future.isSuccess();
        }
        return false;
    }

    @Override
    public void finish(NSQMessage message) throws NSQException {
        if (message == null) {
            return;
        }
        final FixedPool pool = address_2_pool.get(message.getAddress());
        final List<NSQConnection> connections = pool.getConnections();
        if (connections != null) {
            for (NSQConnection c : connections) {
                if (c.getId() == message.getConnectionID().intValue()) {
                    synchronized (c) {
                        if (c.isConnected()) {
                            c.command(new Finish(message.getMessageID()));
                            finished.incrementAndGet();
                            // It is OK.
                            return;
                        }
                    }
                    break;
                }
            }
        }
        throw new NSQNoConnectionException(
                "The connection is broken so that can not retry. Please wait next consuming.");
    }

    @Override
    public void setAutoFinish(boolean autoFinish) {
        this.autoFinish = autoFinish;
    }

    private void sleep(final long millisecond) {
        logger.debug("Sleep {} millisecond.", millisecond);
        try {
            Thread.sleep(millisecond);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Your machine is too busy! Please check it!");
        }
    }

    private NSQMessage createNSQMessage(final MessageFrame msgFrame, final NSQConnection conn) {
        if (config.isOrdered() && msgFrame instanceof OrderedMessageFrame) {
            OrderedMessageFrame orderedMsgFrame = (OrderedMessageFrame) msgFrame;
            return new NSQMessage(orderedMsgFrame.getTimestamp(), orderedMsgFrame.getAttempts(), orderedMsgFrame.getMessageID(),
                    orderedMsgFrame.getInternalID(), orderedMsgFrame.getTractID(),
                    orderedMsgFrame.getDiskQueueOffset(), orderedMsgFrame.getDiskQueueDataSize(),
                    msgFrame.getMessageBody(), conn.getAddress(), conn.getId());
        } else
            return new NSQMessage(msgFrame.getTimestamp(), msgFrame.getAttempts(), msgFrame.getMessageID(),
                    msgFrame.getInternalID(), msgFrame.getTractID(), msgFrame.getMessageBody(), conn.getAddress(), conn.getId());
    }

    /**
     * fetch topics set, for test purpose.
     *
     * @return topics set
     */
    private SortedSet<Topic> getTopics() {
        return this.topics;
    }

    public String toString() {
        String ipStr = "";
        try {
            ipStr = HostUtil.getLocalIP();
        } catch (IOException e) {
            logger.warn(e.getLocalizedMessage());
        }
        return "[Consumer] at " + ipStr;
    }
}
