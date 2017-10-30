package com.youzan.nsq.client;

import com.youzan.nsq.client.configs.ConfigAccessAgent;
import com.youzan.nsq.client.core.*;
import com.youzan.nsq.client.core.command.*;
import com.youzan.nsq.client.entity.*;
import com.youzan.nsq.client.exception.*;
import com.youzan.nsq.client.network.frame.*;
import com.youzan.nsq.client.network.frame.NSQFrame.FrameType;
import com.youzan.nsq.client.network.netty.NSQClientInitializer;
import com.youzan.util.HostUtil;
import com.youzan.util.IOUtil;
import com.youzan.util.NamedThreadFactory;
import com.youzan.util.ThreadSafe;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * TODO: a description
 * <pre>
 * Expose to Client Code. Connect to one cluster(includes many brokers).
 * </pre>
 * <p>
 * Use JDK7.
 * The logical preciseness is more important than the performance.
 *
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public class ConsumerImplV2 implements Consumer, IConsumeInfo {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerImplV2.class);
    private static final Logger LOG_CONSUME_POLICY = LoggerFactory.getLogger(ConsumerImplV2.class.getName() + ".consume.policy");
    private static final Logger PERF_LOG = LoggerFactory.getLogger(ConsumerImplV2.class.getName() + ".perf");

    private final static AtomicInteger CONN_ID_GENERATOR = new AtomicInteger(0);
    //max connect retry allowed
    private static final int MAX_CONSUME_RETRY = 3;

    //consumer start&close synchronization and flags
    private final ReentrantReadWriteLock cLock = new ReentrantReadWriteLock();
    protected AtomicBoolean started = new AtomicBoolean(Boolean.FALSE);
    protected AtomicBoolean closing = new AtomicBoolean(Boolean.FALSE);

    private volatile long lastConnecting = 0L;
    private volatile long lastSuccess = 0L;
    private volatile float consumptionRate = 0f;

    private final AtomicInteger received = new AtomicInteger(0);
    private final AtomicInteger success = new AtomicInteger(0);
    private final AtomicInteger finished = new AtomicInteger(0);
    private final AtomicInteger re = new AtomicInteger(0); // have done reQueue
    private final AtomicInteger queue4Consume = new AtomicInteger(0); // have done reQueue
    private final AtomicInteger skipped = new AtomicInteger(0);

    //connection manager
    protected ConnectionManager conMgr = new ConnectionManager(this);

    //netty component for consumer
    private final Bootstrap bootstrap = new Bootstrap();

    /*
     * topics' partitions maintaining a sorted set of partitions number, example: {-1, 0, 2, 3}
     */
    protected final HashMap<String, SortedSet<Long>> topics2Partitions = new HashMap<>();
    /*
     * nsqd address to connection map in effect.
     */
    protected final ConcurrentHashMap<Address, NSQConnection> address_2_conn = new ConcurrentHashMap<>();

    /*
     * schedule executor for updating nsqd connections in effect
     */
    private final ScheduledExecutorService scheduler = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory(this.getClass().getSimpleName(), Thread.NORM_PRIORITY));
    /*
     * message handler
     */
    private volatile MessageHandler handler;

    /*
     * message handler executor
     */
    private final ExecutorService executor;

    /*
     * auto finish flag
     */
    private volatile boolean autoFinish = true;
    /*
     *simple client
     */
    private final NSQSimpleClient simpleClient;
    /*
     * config for consumer
     */
    private final NSQConfig config;


    /**
     * Initialize Consumer with passin {@link NSQConfig}, consumer keep reference to passin config object.
     * @param config
     */
    public ConsumerImplV2(NSQConfig config) {
        this.config = config;
        this.simpleClient = new NSQSimpleClient(Role.Consumer, this.config.getUserSpecifiedLookupAddress());

        //initialize netty component
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup(config.getNettyPoolSize());
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeoutInMillisecond());
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new NSQClientInitializer());
        //initialize consumer worker size
        executor = Executors.newFixedThreadPool(this.config.getConsumerWorkerPoolSize(),
                new NamedThreadFactory(this.getClass().getSimpleName() + "-ClientBusiness", Thread.MAX_PRIORITY));
    }

    /**
     * Consumer constructor, with {@link NSQConfig}, and {@link MessageHandler}
     * @param config
     *                NSQConfig config to initialize consumer
     * @param handler
     *                the client message handler code sets it
     */
    public ConsumerImplV2(NSQConfig config, MessageHandler handler) {
        this(config);
        this.handler = handler;
    }

    public void setMessageHandler(final MessageHandler handler) {
        if(this.started.get()) {
            throw new IllegalStateException("Consumer has started.");
        }
        this.handler = handler;
    }

    @Override
    public NSQConfig getConfig() {
        return this.config;
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
            //set copy partition true here, as consumer may need to connect to specified partition
            Topic topicCopy = Topic.newInstacne(topic, true);
            SortedSet<Long> partitionsSet;
            if(topics2Partitions.containsKey(topicCopy.getTopicText())) {
                partitionsSet = topics2Partitions.get(topicCopy);
            } else {
                partitionsSet = new TreeSet<>();
                topics2Partitions.put(topicCopy.getTopicText(), partitionsSet);
                simpleClient.putTopic(topicCopy.getTopicText());
            }
            //add partition id to sorted set
            partitionsSet.add((long) topicCopy.getPartitionId());
        }
    }

    private void subscribeTopics(String... topics) {
        if (topics == null) {
            return;
        }
        for (String topicStr : topics) {
            Topic topic = new Topic(topicStr);
            subscribe(topic);
        }
    }

    private boolean validateLookupdSource() {
        if(this.config.getUserSpecifiedLookupAddress()) {
            String[] lookupdAddresses = this.config.getLookupAddresses();
            if(null == lookupdAddresses || lookupdAddresses.length == 0) {
                logger.error("Seed lookupd addresses is not specified in NSQConfig. Seed lookupd addresses: {}", lookupdAddresses);
                return false;
            }
        } else {
            try {
                ConfigAccessAgent.getInstance();
            } catch (ConfigAccessAgentException e) {
                logger.error("ConfigAccessAgent fail to initialize.");
                return false;
            }
            String[] configRemoteURLS = ConfigAccessAgent.getConfigAccessRemotes();
            String configRemoteEnv = ConfigAccessAgent.getEnv();
            if(null == configRemoteURLS || configRemoteURLS.length == 0 || null == configRemoteEnv) {
                logger.error("Config remote URLs or env is not specified in NSQConfig. URLs: {}, env: {}", configRemoteURLS, configRemoteEnv);
                return false;
            }
        }
        return true;
    }

    @Override
    public void start() throws NSQException {
        //validates config
        if (this.config.getConsumerName() == null || this.config.getConsumerName().isEmpty()) {
            throw new IllegalArgumentException("Consumer Name is blank! Please check it!");
        }
        //validate message not null
        if (null == this.handler) {
            throw new IllegalArgumentException("Message handler is null");
        }
        //validate there is topics for subscribe
        if (this.topics2Partitions.isEmpty()) {
            logger.warn("No topic subscribed.");
        }
        //start consumer
        if (this.started.compareAndSet(Boolean.FALSE, Boolean.TRUE) && cLock.writeLock().tryLock()) {
            String configJsonStr = NSQConfig.parseNSQConfig(this.config);
            logger.info("Config for consumer {}: {}", this, configJsonStr);
            try {
                //validate that consumer have right lookup address source
                if (!validateLookupdSource()) {
                    throw new IllegalArgumentException("Consumer could not start with invalid lookupd address sources.");
                }
                if (this.config.getUserSpecifiedLookupAddress()) {
                    LookupAddressUpdate.getInstance().setUpDefaultSeedLookupConfig(this.simpleClient.getLookupLocalID(), this.config.getLookupAddresses());
                }
                this.simpleClient.start();

                keepConnecting();
                //start connection manager
                conMgr.start();
                logger.info("The consumer {} has been started.", this);
            }finally {
                cLock.writeLock().unlock();
            }
        }
    }

    /**
     * update consumption rate according to message consumption last _INTERVAL_IN_SECOND
     */
    private void updateConsumptionRate() {
        consumptionRate = ((long)success.get() - lastSuccess) / _INTERVAL_IN_SECOND;
        lastSuccess = success.get();
    }

    public boolean isConsumptionEstimateElapseTimeout() {
        return consumptionRate * queue4Consume.get() *1000 >= this.config.getMsgTimeoutInMillisecond();
    }

    /**
     * keep updating topics' connections according to simple clients' topics to partitions selectors
     */
    private void keepConnecting() {
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    connect();
                    updateConsumptionRate();
                } catch (Throwable e) {
                    logger.error("Throwable in keep connection process:", e);
                }
                logger.info("Client received {} messages , success {} , finished {} , queue4Consume {}, reQueue explicitly {}. The values do not use a lock action.", received, success, finished, queue4Consume, re);
            }
        }, 0, _INTERVAL_IN_SECOND, TimeUnit.SECONDS);
    }

    /**
     * Connect to all the brokers with the config, making sure the new is OK
     * and the old is clear.
     */
    protected void connect() throws NSQException {
        //which equals to: System.currentTimeMillis() - lastConnecting < TimeUnit.SECONDS.toMillis(_INTERVAL_IN_SECOND))
        //rest logic performs only when time elapse larger than _INTERNAL_IN_SECOND
        if (System.currentTimeMillis() < lastConnecting + TimeUnit.SECONDS.toMillis(_INTERVAL_IN_SECOND)) {
            return;
        }

        lastConnecting = System.currentTimeMillis();
        if (!this.started.get()) {
            if (closing.get()) {
                logger.info("Consumer has been closed sometimes ago!");
            }
            return;
        }

        if (this.topics2Partitions.isEmpty()) {
            logger.error("No topic subscribed. Please check it right now!");
        }

        //broken set to collect Address of connection which is not connected
        final Set<ConnectionManager.NSQConnectionWrapper> broken = new HashSet<>();
        //final ConcurrentHashMap<Address, Set<String>> address_2_topics = new ConcurrentHashMap<>();
        final Set<Address> targetAddresses = new TreeSet<>();
        /*-
         * =====================================================================
         *                    Clean up the broken connections
         * =====================================================================
         */
        for (final NSQConnection c : address_2_conn.values()) {
            try {
                if (!c.isConnected()) {
                    //close it directly, as it is broken
                    c.close();
                    broken.add(new ConnectionManager.NSQConnectionWrapper(c));
                }
            } catch (Exception e) {
                logger.error("While detecting broken connections, Exception:", e);
            }
        }
        /*-
         * =====================================================================
         *                                First Step:
         *                          干掉Broken Brokers.
         * =====================================================================
         */
        if(broken.size() > 0) {
            Map<String, List<ConnectionManager.NSQConnectionWrapper>> topic2ConWrappers = new HashMap<>();
            for (ConnectionManager.NSQConnectionWrapper conWrapper : broken) {
                if (!topic2ConWrappers.containsKey(conWrapper.getTopic())) {
                    topic2ConWrappers.put(conWrapper.getTopic(), new ArrayList<ConnectionManager.NSQConnectionWrapper>());
                }
                List<ConnectionManager.NSQConnectionWrapper> conWrappers = topic2ConWrappers.get(conWrapper.getTopic());
                conWrappers.add(conWrapper);
                clearDataNode(conWrapper);
            }
            conMgr.remove(topic2ConWrappers);
        }
        /*-
         * =====================================================================
         *                            Get the relationship
         * =====================================================================
         */
        for (String topic : topics2Partitions.keySet()) {
            int idx = 0;
            Address[] partitionDataNodes = null;
            while(null == partitionDataNodes) {
                try {
                    Object[] shardingIDs;
                    SortedSet<Long> partitionSet = topics2Partitions.get(topic);
                    long top = partitionSet.first();
                    if(top >= 0)
                        shardingIDs = partitionSet.toArray(new Long[0]);
                        //convert partition ID to long type directly.
                    else shardingIDs = new Object[]{Message.NO_SHARDING};
                    partitionDataNodes = simpleClient.getPartitionNodes(new Topic(topic), shardingIDs, false);
                } catch (NSQLookupException lookupe) {
                    logger.warn("Hit a invalid lookup address, retry another. Has retried: {}", idx);
                    if(idx++ >= MAX_CONSUME_RETRY){
                        throw lookupe;
                    }
                } catch (InterruptedException e) {
                    logger.warn("Thread interrupted waiting for partition selector update, Topic {}. Ignore if SDK is shutting down.", topic);
                    //retry for now
                }
            }
            final List<Address> dataNodeLst = Arrays.asList(partitionDataNodes);
            targetAddresses.addAll(dataNodeLst);
        }
        logger.debug("subscribe target NSQd nodes: {}", targetAddresses);

        final Set<Address> oldAddresses = new TreeSet<>(this.address_2_conn.keySet());
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
        final Set<NSQConnection> conns2ClsSet = new HashSet<>();
        except2.removeAll(targetAddresses);
        if (!except2.isEmpty()) {
            logger.info("Delete unused data-nodes: {}", except2);
            for (Address address : except2) {
                conns2ClsSet.addAll(clearDataNode(address));
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
            logger.info("Get new data-nodes: {}", except1);
            for (Address address : except1) {
                try {
                    connect(address);
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
        //close connections need expiration
        if(conns2ClsSet.size() > 0) {
            logger.info("Close nsqd connections need expire: {}", conns2ClsSet);
            for (NSQConnection conn2Close : conns2ClsSet) {
                conn2Close.close();
            }
            logger.info("Done expiring nsqd connections.");
        }
        except1.clear();
        except2.clear();
        oldAddresses.clear();
        broken.clear();
        targetAddresses.clear();
    }

    protected void connect(Address address) throws Exception {
        if (null == address) {
            return;
        }
        cLock.readLock().lock();
        try {
            if (closing.get()) {
                return;
            }

            //connect to address
            final ChannelFuture future = bootstrap.connect(address.getHost(), address.getPort());
            // Wait until the connection attempt succeeds or fails.
            if (!future.awaitUninterruptibly(config.getConnectTimeoutInMillisecond(), TimeUnit.MILLISECONDS)) {
                throw new NSQNoConnectionException(future.cause());
            }
            final Channel channel = future.channel();
            if (!future.isSuccess()) {
                if (channel != null) {
                    channel.close();
                }
                throw new NSQNoConnectionException("Connect " + address + " is wrong.", future.cause());
            }

            final NSQConnection conn = new NSQConnectionImpl(CONN_ID_GENERATOR.incrementAndGet(), address, channel,
                    config);
            address_2_conn.put(address, conn);

            // Netty async+sync programming
            channel.attr(NSQConnection.STATE).set(conn);
            channel.attr(Client.STATE).set(this);
            channel.attr(Client.ORDERED).set(this.config.isOrdered());
            channel.attr(NSQConnection.EXTEND_SUPPORT).set(conn.isExtend());

            Topic topic = new Topic(address.getTopic(), address.getPartition());
            try {
                conn.init(topic);
            } catch (Exception e) {
                conn.close();
                if (!closing.get()) {
                    throw new NSQNoConnectionException("Creating a connection and having a negotiation fails!", e);
                }
            }

            if (!conn.isConnected()) {
                conn.close();
                if (!closing.get()) {
                    throw new NSQNoConnectionException("Pool failed in connecting to NSQd! Closing: !" + closing);
                }
            } else {
                Sub command = createSubCmd(topic, this.config.getConsumerName());
                final NSQFrame frame = conn.commandAndGetResponse(command);
                if (handleResponse(frame, conn)) {
                    //as there is no success response from nsq, command is enough here
                    conn.subSent();
                    this.conMgr.subscribe(topic.getTopicText(), conn);
                }
            }
        }finally {
            cLock.readLock().unlock();
        }
    }

    private Sub createSubCmd(final Topic topic, String channel) {
        if (this.config.isOrdered())
            return new SubOrdered(topic, channel);
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
        if (!address_2_conn.containsKey(address)) {
            return null;
        }
        final Set<NSQConnection> clearConnections = new HashSet<>();
        final NSQConnection conn = address_2_conn.get(address);
        address_2_conn.remove(address);
        if (conn != null) {
                clearConnections.add(conn);
                try {
                    backoff(conn);
                } catch (Exception e) {
                    logger.error("It can not backoff the connection! Exception:", e);
                }
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
        switch(frame.getType()) {
            case RESPONSE_FRAME: {
                if (frame.isHeartBeat() && conn.isSubSent()) {
                    simpleClient.incoming(frame, conn);
                } else {
                    conn.addResponseFrame((ResponseFrame) frame);
                }
                break;
            }
            case ERROR_FRAME: {
                if (conn.isSubSent()) {
                    simpleClient.incoming(frame, conn);
                } else {
                    final ErrorFrame err = (ErrorFrame) frame;
                    conn.addErrorFrame(err);
                    logger.warn("Error-Frame from {} , frame: {}", conn.getAddress(), frame);
                }
                break;
            }
            case MESSAGE_FRAME: {
                received.incrementAndGet();
                final MessageFrame msg = (MessageFrame) frame;
                final NSQMessage message = createNSQMessage(msg, conn);

                //check desired tag
                DesiredTag tag = this.config.getConsumerDesiredTag();
                if (conn.isExtend() && null != tag && StringUtils.isNotEmpty(tag.getTagName()) && !tag.match(message.getTag())) {
                    logger.warn("Skip message {} has tag {} not desired. Consumer desired tag: {}, Address: {}", message.getInternalID(), message.getTag(), this.config.getConsumerDesiredTag(), conn.getAddress());
                    return;
                }

                if (TraceLogger.isTraceLoggerEnabled() && conn.getAddress().isHA())
                    TraceLogger.trace(this, conn, message);
                if (this.config.isOrdered()
                        && !conn.checkOrder(message.getInternalID(), message.getDiskQueueOffset(), message)) {
                    //order problem
                    throw new NSQInvalidMessageException("Invalid internalID or diskQueueOffset in order mode.");
                }
//                conn.setMessageTouched(System.currentTimeMillis());

                processMessage(message, conn);
            }
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
            logger.error("message handler task rejected as task queue is full.");
            connection.declineExpectedRdy();
        }
    }

    boolean needSkip(final NSQMessage msg) {
        //skip if:
        //1. message has extension header;
        //2. skip extension KV not empty;
        //3. 1 & 2 has subset;
        boolean skip = false;
        Map<String, Object> jsonExtHeader = msg.getJsonExtHeader();
        if(null != jsonExtHeader && jsonExtHeader.size() > 0) {
            Map<String, Object> skipKV = this.config.getMessageSkipExtensionKVMap();
            if(null != skipKV && skipKV.size() > 0) {
                //create a copy
                Set<String> subset = new HashSet(jsonExtHeader.keySet());
                subset.retainAll(skipKV.keySet());
                if(subset.size() > 0) {
                    LOG_CONSUME_POLICY.info("Message skipped as Key for skip found in json extension header. message: {}, subset: {}", msg, subset);
                    skip = true;
                }
            }
        }
        return skip;
    }

    /**
     * @param message    a NSQMessage
     * @param connection a NSQConnection
     */
    // through it is private method, signature of method will NOT change as it is used as cut point
    private void consume(final NSQMessage message, final NSQConnection connection) {
        boolean ok;
        boolean retry;
        boolean explicitRequeue = false;
        boolean skip = needSkip(message);

        long start = System.currentTimeMillis();
        try {
            if(!skip)
                handler.process(message);
            else
                skipped.incrementAndGet();
            ok = true;
            retry = false;
        } catch (ExplicitRequeueException e) {
            ok = false;
            retry = false;
            explicitRequeue = true;
            logger.info("Message {} explicit requeue by client business, in {} sec. {}", message, message.getNextConsumingInSecond(), e.getMessage());
            if(!e.isWarnLogDepressed()) {
                logger.warn("Client business has one error. Original message: {}. Exception:", message.getReadableContent(), e);
            }
        } catch (RetryBusinessException e) {
            ok = false;
            retry = true;
        } catch (Exception e) {
            ok = false;
            retry = false;
            logger.error("Client business has one error. Original message: {}. Exception:", message.getReadableContent(), e);
        }
        if (!ok && retry) {
            logger.info("Client has told SDK to do again. {}", message);
            try {
                handler.process(message);
                ok = true;
            } catch (Exception e) {
                ok = false;
                retry = false;
                logger.error("Client business retry fail. Original message: {}. Exception:", message.getReadableContent(), e);
            }
        }
        long end = System.currentTimeMillis() - start;
        if(PERF_LOG.isDebugEnabled())
            PERF_LOG.debug("Message handler took {} milliSec to finish consuming message for connection {}. Success:{}, Retry:{}", end, connection.getAddress(), ok, retry);
        if(end > this.config.getMsgTimeoutInMillisecond())
            PERF_LOG.warn("Message handler took {} milliSec to finish consuming message. Limitation is {}", end, this.config.getMsgTimeoutInMillisecond());

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
                    // ReQueue
                    cmd = new ReQueue(message.getMessageID(), nextConsumingWaiting.intValue());
                    final byte[] id = message.getMessageID();
                    logger.info("Do a re-queue by SDK that is a default behavior. MessageID: {} , Hex: {}", id, message.newHexString(id));
                } else {
                    if (!this.config.isOrdered() && end > this.config.getMsgTimeoutInMillisecond()) {
                        logger.warn("It took {} milliSec to for message to be consumed by message handler, and exceeds message timeout in nsq config. Fin not be invoked as requeue from NSQ server is on its way.", end);
                    } else {
                        // Finish: client explicitly sets NextConsumingInSecond is null
                        cmd = new Finish(message.getMessageID());
                        final byte[] id = message.getMessageID();
                        logger.info("Do a Finish by SDK, given that client process handler has failed and next consuming time elapse not specified. MessageID: {} , Hex: {}", id, message.newHexString(id));
                    }
                }
                if(!explicitRequeue)
                    connection.declineExpectedRdy();
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
            if (!closing.get()) {
                ChannelFuture future = connection.command(cmd);
                if (null != future) {
                    future.addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if (!future.isSuccess()) {
                                logger.error("Fail to send {}. Message {} will be delivered to consumer in another round.", cmdStr, message.getMessageID());
                            } else if (PERF_LOG.isDebugEnabled()) {
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
            }
        }
        // Post
        //log warn
        if (!ok) {
            //TODO: connection.setMessageConsumptionFailed(start);
//            logger.warn("Exception occurs in message handler. Please check it right now {} , Original message: {}.", message, message.getReadableContent());
        } else if (!this.config.isOrdered()){
            connection.increaseExpectedRdy();
        }
    }

    @Override
    public void backoff(NSQConnection conn) {
        conMgr.backoff(conn);
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
        if(started.get() && closing.compareAndSet(Boolean.FALSE, Boolean.TRUE)) {
            cLock.writeLock().lock();
            try {
                started.set(Boolean.FALSE);
                closing.set(Boolean.TRUE);
                //close lookup address update
                LookupAddressUpdate.getInstance().removeDefaultSeedLookupConfig(this.simpleClient.getLookupLocalID());
                LookupAddressUpdate.getInstance().closed();
                //stop & clear topic to partition mapping
                IOUtil.closeQuietly(simpleClient);
                //stop connect new NSQConnections
                scheduler.shutdownNow();
                this.conMgr.close();
                //backoff existing connections
                final Set<NSQConnection> connections = cleanClose();
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                        logger.warn("Message workers handles messages over 10 sec.");
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                close(connections);
                logger.info("The consumer has been closed.");
            } finally {
                cLock.writeLock().unlock();
            }
        }
    }

    @Override
    public void backoff(Topic topic) {
        this.conMgr.backoff(topic.getTopicText(), null);
    }

    @Override
    public void backoff(Topic topic, CountDownLatch latch) {
        this.conMgr.backoff(topic.getTopicText(), latch);
    }

    @Override
    public void resume(Topic topic) {
        this.conMgr.resume(topic.getTopicText(), null);
    }

    @Override
    public void resume(Topic topic, CountDownLatch latch) {
        this.conMgr.resume(topic.getTopicText(), latch);
    }


    private void disconnectServer(NSQConnection connection) {
        if (!connection.isConnected()) {
            // It has been closed
            return;
        }
        connection.disconnect(this.conMgr);
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
        try {
            disconnectServer(connection);
        } catch (Exception e) {
            logger.error("Fail backoff connection {} to the server, Exception", connection, e);
        }
    }

    private Set<NSQConnection> cleanClose() {
        final Set<NSQConnection> connections = new HashSet<>();
        final Set<Address> addresses = address_2_conn.keySet();
        if (!addresses.isEmpty()) {
            for (Address address : addresses) {
                Set<NSQConnection> tmp = clearDataNode(address);
                connections.addAll(tmp);
            }
        }
        address_2_conn.clear();
        topics2Partitions.clear();
        return connections;
    }

    private boolean handleResponse(NSQFrame frame, NSQConnection connection) {
        if (frame == null) {
            logger.warn("the nsq frame is null.");
            return false;
        }
        if (frame.getType() == FrameType.RESPONSE_FRAME) {
            return true;
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
                    Topic topic = connection.getTopic();
                    if(null != topic) {
                        this.simpleClient.invalidatePartitionsSelector(topic.getTopicText());
                        logger.info("Partitions info for {} invalidated and related lookupd address force updated.", topic.getTopicText());
                    } else {
                        logger.error("topic from connection {} is empty.", connection);
                    }
                    this.simpleClient.clearDataNode(address);
                    clearDataNode(address);
                    logger.info("NSQInvalidDataNode. {}", frame);
                    break;
                }
                //for error case which nsqd nodes does not invalidation
                case E_SUB_ORDER_IS_MUST: {
                    logger.error("SubOrder need for topic(s) consuming.");
                    break;
                }
                case E_SUB_EXTEND_NEED: {
                    logger.error("topic needs extend support identify.");
                    break;
                }
                default: {
                    logger.error("Unknown error type in ERROR_FRAME! {}", frame);
                }
            }
            return false;
        }
        logger.error("Receive frame client can not handle, frameType {}", frame.getType());
        return false;
    }

    /**
     * connection heart beat validation for consumer, invoked by netty idle event.
     * @param conn NSQConnection
     * @return  {@link Boolean#TRUE} if success, {@link Boolean#FALSE} otherwise.
     */
    @Override
    public boolean validateHeartbeat(NSQConnection conn) {
        if (!conn.isConnected()) {
            return false;
        }
        final ChannelFuture future = conn.command(Nop.getInstance());
        if (null != future && future.awaitUninterruptibly(config.getQueryTimeoutInMillisecond(), TimeUnit.MILLISECONDS)) {
            return future.isSuccess();
        }
        return false;
    }

    @Override
    public void finish(final NSQMessage message) throws NSQException {
        if (message == null) {
            return;
        }
        final NSQConnection conn = address_2_conn.get(message.getAddress());
        if (conn != null) {
            if (conn.getId() == message.getConnectionID().intValue()) {
                if (conn.isConnected()) {
                    ChannelFuture future = conn.command(new Finish(message.getMessageID()));
                    future.addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if(future.isSuccess()) {
                                finished.incrementAndGet();
                            } else {
                                logger.warn("Fail to FIN {}.", message, future.cause());
                            }
                        }
                    });
                } else {
                    logger.info("Connection for message {} is closed. Finish exits.", message);
                }
            } else {
                logger.error("message {} does not belong to current consumer's connection", message);
            }
        } else {
            throw new NSQNoConnectionException(
                    "The connection is closed so that can not retry. Please wait next consuming.");
        }
    }

    @Override
    public void touch(final NSQMessage message) throws NSQException {
        if (message == null) {
            return;
        }
        final NSQConnection conn = address_2_conn.get(message.getAddress());
        if (conn != null) {
            if (conn.getId() == message.getConnectionID().intValue()) {
                if (conn.isConnected()) {
                    ChannelFuture future = conn.command(new Touch(message.getMessageID()));
                    future.addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if(!future.isSuccess()) {
                                logger.warn("Fail to Touch {}.", message, future.cause());
                            }
                        }
                    });
                }
            } else {
                logger.error("message {} does not belong to current consumer's connection", message);
            }
        } else {
            throw new NSQNoConnectionException(
                    "The connection is broken so that Touch can not sent.");
        }
    }

    @Override
    public void setAutoFinish(boolean autoFinish) {
        this.autoFinish = autoFinish;
    }

    private NSQMessage createNSQMessage(final MessageFrame msgFrame, final NSQConnection conn) throws NSQInvalidMessageException {
        NSQMessage msg;
        if (config.isOrdered() && msgFrame instanceof OrderedMessageFrame) {
            OrderedMessageFrame orderedMsgFrame = (OrderedMessageFrame) msgFrame;
            msg = new NSQMessage(orderedMsgFrame.getTimestamp(), orderedMsgFrame.getAttempts(), orderedMsgFrame.getMessageID(),
                    orderedMsgFrame.getInternalID(), orderedMsgFrame.getTractID(),
                    orderedMsgFrame.getDiskQueueOffset(), orderedMsgFrame.getDiskQueueDataSize(),
                    msgFrame.getMessageBody(), conn.getAddress(), conn.getId(), this.config.getNextConsumingInSecond(), conn.getTopic(), conn.isExtend());
        } else {
            msg = new NSQMessage(msgFrame.getTimestamp(), msgFrame.getAttempts(), msgFrame.getMessageID(),
                    msgFrame.getInternalID(), msgFrame.getTractID(), msgFrame.getMessageBody(), conn.getAddress(), conn.getId(), this.config.getNextConsumingInSecond(), conn.getTopic(), conn.isExtend());
        }
        if(conn.isExtend()) {
            ExtVer extVer = ExtVer.getExtVersion(msgFrame.getExtVerBytes());
            try {
                msg.parseExtContent(extVer, msgFrame.getExtBytes());
            } catch (IOException e) {
                throw new NSQInvalidMessageException("Fail to parse ext content for incoming message.", e);
            }
        }
        return msg;
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

    @Override
    public float getLoadFactor() {
        ThreadPoolExecutor pool = (ThreadPoolExecutor) executor;
        int active = pool.getActiveCount();
        int queueSize = queue4Consume.get();
        if(active > 0)
            return (float)queueSize/active;
        else {
            return 0f;
        }
    }

    @Override
    public int getRdyPerConnection() {
        return this.config.getRdy();
    }

    public ConnectionManager getConnectionManager() {
        return this.conMgr;
    }
}
