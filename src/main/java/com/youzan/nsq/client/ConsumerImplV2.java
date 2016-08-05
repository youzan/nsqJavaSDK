package com.youzan.nsq.client;

import com.youzan.nsq.client.core.KeyedPooledConnectionFactory;
import com.youzan.nsq.client.core.NSQConnection;
import com.youzan.nsq.client.core.NSQSimpleClient;
import com.youzan.nsq.client.core.command.*;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.entity.Response;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.exception.NSQInvalidDataNodeException;
import com.youzan.nsq.client.exception.NSQNoConnectionException;
import com.youzan.nsq.client.network.frame.ErrorFrame;
import com.youzan.nsq.client.network.frame.MessageFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.nsq.client.network.frame.NSQFrame.FrameType;
import com.youzan.util.ConcurrentSortedSet;
import com.youzan.util.IOUtil;
import com.youzan.util.NamedThreadFactory;
import io.netty.channel.ChannelFuture;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <pre>
 * Expose to Client Code. Connect to one cluster(includes many brokers).
 * </pre>
 * <p>
 * Use JDK7
 *
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public class ConsumerImplV2 implements Consumer {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerImplV2.class);
    private final Object lock = new Object();
    private volatile boolean started = false;
    private final AtomicBoolean closing = new AtomicBoolean(false);

    private final GenericKeyedObjectPoolConfig poolConfig;
    private final KeyedPooledConnectionFactory factory;
    private GenericKeyedObjectPool<Address, NSQConnection> bigPool = null;

    private final AtomicInteger received = new AtomicInteger(0);
    private final AtomicInteger success = new AtomicInteger(0);

    /*-
     * =========================================================================
     * 
     * =========================================================================
     */
    private final Set<String> topics = new HashSet<>();
    private final ConcurrentHashMap<Address, Set<NSQConnection>> holdingConnections = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory(this.getClass().getName(), Thread.NORM_PRIORITY));

    /*-
     * =========================================================================
     *                          Client delegate to me
     */
    private final MessageHandler handler;
    private final int WORKER_SIZE = Runtime.getRuntime().availableProcessors() * 4;
    private final ExecutorService executor = Executors.newFixedThreadPool(WORKER_SIZE,
            new NamedThreadFactory(this.getClass().getName() + "-ClientBusiness", Thread.MAX_PRIORITY));
    /*-
     *                          Client delegate to me
     * =========================================================================
     */

    private final Rdy DEFAULT_RDY;
    private final Rdy MEDIUM_RDY;
    private final Rdy LOW_RDY;
    private volatile Rdy currentRdy = null;
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

        this.poolConfig = new GenericKeyedObjectPoolConfig();
        this.simpleClient = new NSQSimpleClient(config.getLookupAddresses());
        this.factory = new KeyedPooledConnectionFactory(this.config, this);

        int messagesPerBatch = config.getRdy();
        DEFAULT_RDY = new Rdy(Math.max(messagesPerBatch, 1));
        MEDIUM_RDY = new Rdy(Math.max((int) (messagesPerBatch * 0.3D), 1));
        LOW_RDY = new Rdy(1);
        currentRdy = DEFAULT_RDY;
    }

    @Override
    public void subscribe(String... topics) {
        if (topics == null) {
            return;
        }
        Collections.addAll(this.topics, topics);
    }

    @Override
    public void start() throws NSQException {
        if (this.config.getConsumerName() == null || this.config.getConsumerName().isEmpty()) {
            throw new IllegalArgumentException("Consumer Name is blank! Please check it!");
        }
        synchronized (lock) {
            if (!this.started) {
                this.started = true;
                // setting all of the configs
                this.poolConfig.setLifo(false);
                this.poolConfig.setFairness(true);
                this.poolConfig.setTestOnBorrow(false);
                this.poolConfig.setTestOnReturn(true);
                this.poolConfig.setTestWhileIdle(false);
                this.poolConfig.setJmxEnabled(false);
                //
                this.poolConfig.setMinEvictableIdleTimeMillis(-1);
                this.poolConfig.setSoftMinEvictableIdleTimeMillis(-1);
                this.poolConfig.setTimeBetweenEvictionRunsMillis(-1);
                //
                this.poolConfig.setMinIdlePerKey(this.config.getThreadPoolSize4IO());
                this.poolConfig.setMaxIdlePerKey(this.config.getThreadPoolSize4IO());
                this.poolConfig.setMaxTotalPerKey(this.config.getThreadPoolSize4IO());
                // acquire connection waiting time underlying the inner network
                this.poolConfig.setMaxWaitMillis(this.config.getConnectTimeoutInMillisecond() + this.config.getQueryTimeoutInMillisecond() / 3);
                this.poolConfig.setBlockWhenExhausted(true);
                //  new instance without performing to connect
                this.bigPool = new GenericKeyedObjectPool<>(this.factory, this.poolConfig);
                this.simpleClient.start();
                // -----------------------------------------------------------------
                //                       First, async keep
                keepConnecting();
                connect();
                // -----------------------------------------------------------------
                logger.info("The consumer is started.");
            }
        }
    }

    /**
     * schedule action
     */
    private void keepConnecting() {
        final int delay = _r.nextInt(60); // seconds
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    connect();
                } catch (Exception e) {
                    logger.error("Exception", e);
                }
            }
        }, delay, _INTERVAL_IN_SECOND, TimeUnit.SECONDS);
    }

    /**
     * Make it be a {@link NSQConnection}
     *
     * @param address the data-node
     * @throws Exception if an error occurs
     */
    private void connect(final Address address) throws Exception {
        if (address == null) {
            logger.error("Your input address is blank!");
            return;
        }
        synchronized (address) {
            bigPool.clear(address);
            bigPool.preparePool(address);
            logger.info("A consumer creates the {} pool.");
        }
    }

    /**
     * Make it be a consumer-connection
     *
     * @param connection a NSQConnection
     * @param topic      a topic
     * @throws TimeoutException if the CPU is too busy or network is too worst
     * @throws NSQException     if an error occurs
     */
    private void subscribe(NSQConnection connection, String topic) throws TimeoutException, NSQException {
        synchronized (connection) {
            final NSQFrame frame = connection.commandAndGetResponse(new Sub(topic, config.getConsumerName()));
            handleResponse(frame, connection);
            currentRdy = DEFAULT_RDY;
            connection.command(currentRdy);
        }
    }

    /**
     * Connect to all the brokers with the config, making sure the new is OK
     * and the old is clear.
     */
    private void connect() throws NSQException {
        final Set<Address> broken = new HashSet<>();
        synchronized (lock) {
            addresses:
            for (final Set<NSQConnection> connections : holdingConnections.values()) {
                for (final NSQConnection c : connections) {
                    try {
                        if (!c.isConnected()) {
                            c.close();
                            broken.add(c.getAddress());
                            continue addresses;
                        }
                    } catch (Exception e) {
                        logger.error("Exception occurs while detecting broken connections!", e);
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
                try {
                    clearDataNode(address);
                } catch (Exception e) {
                    logger.error("Exception", e);
                }
            }

            final ConcurrentHashMap<Address, Set<String>> address_2_topics = new ConcurrentHashMap<>();
            final Set<Address> targetAddresses = new TreeSet<>();
            for (String topic : topics) {
                final ConcurrentSortedSet<Address> dataNodes = simpleClient.getDataNodes(topic);
                final Set<Address> addresses = new TreeSet<>();
                addresses.addAll(dataNodes.newSortedSet());

                for (Address a : addresses) {
                    final Set<String> tmpTopics;
                    if (address_2_topics.containsKey(a)) {
                        tmpTopics = address_2_topics.get(a);
                    } else {
                        tmpTopics = new TreeSet<>();
                        address_2_topics.put(a, tmpTopics);
                    }
                    tmpTopics.add(topic);
                }
                targetAddresses.addAll(addresses);
            }
            logger.debug("address_2_topics: {}", address_2_topics);

            final Set<Address> oldAddresses = new TreeSet<>(this.holdingConnections.keySet());
            if (targetAddresses.isEmpty() && oldAddresses.isEmpty()) {
                return;
            }
            logger.debug("Prepare to connect new data-nodes(NSQd): {} , old data-nodes(NSQd): {}", targetAddresses,
                    oldAddresses);
            if (targetAddresses.isEmpty()) {
                logger.error("Get the current new DataNodes (NSQd). It will create a new pool next time! Now begin to clear up old data-nodes(NSQd) {}", oldAddresses);
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
                for (Address address : except2) {
                    if (address == null) {
                        continue;
                    }
                    if (holdingConnections.containsKey(address)) {
                        final Set<NSQConnection> holding = holdingConnections.get(address);
                        if (holding != null) {
                            for (NSQConnection c : holding) {
                                try {
                                    backoff(c);
                                } catch (Exception e) {
                                    logger.error("It can not backoff the connection!", e);
                                } finally {
                                    IOUtil.closeQuietly(c);
                                }
                            }
                        }
                    }
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
                for (Address a : except1) {
                    try {
                        connect(a);
                    } catch (Exception e) {
                        logger.error("Exception", e);
                        clearDataNode(a);
                    }
                }
                // subscribe and get all the connections
                for (Map.Entry<Address, Set<String>> pair : address_2_topics.entrySet()) {
                    Address a = pair.getKey();
                    Set<String> topics = pair.getValue();
                    try {
                        for (int i = 0; i < config.getThreadPoolSize4IO(); i++) {
                            final NSQConnection connection = bigPool.borrowObject(a);
                            for (String t : topics) {
                                subscribe(connection, t);
                            }
                            final Set<NSQConnection> holding;
                            if (holdingConnections.containsKey(a)) {
                                holding = holdingConnections.get(a);
                            } else {
                                holding = new TreeSet<>();
                                holdingConnections.put(a, holding);
                            }
                            holding.add(connection);
                            bigPool.returnObject(a, connection);
                        }
                    } catch (Exception e) {
                        clearDataNode(a);
                        logger.error("Address: {} , Exception", a, e);
                    }
                }
            }

            /*-
             * =====================================================================
             *                                Last Step:
             *                          Clean up local resources
             * =====================================================================
             */
            broken.clear();
            except1.clear();
            except2.clear();
            address_2_topics.clear();
            targetAddresses.clear();
            oldAddresses.clear();
        }
    }

    /**
     * @param address the data-node(NSQd)'s address
     */
    public void clearDataNode(Address address) {
        if (address == null) {
            return;
        }

        final Set<NSQConnection> holding = holdingConnections.get(address);
        cleanClose(holding);
        holding.clear();

        holdingConnections.remove(address);
        bigPool.clear(address);
    }

    @Override
    public void incoming(final NSQFrame frame, final NSQConnection conn) throws NSQException {
        if (frame != null && frame.getType() == FrameType.MESSAGE_FRAME) {
            received.incrementAndGet();
            final MessageFrame msg = (MessageFrame) frame;
            final NSQMessage message = new NSQMessage(msg.getTimestamp(), msg.getAttempts(), msg.getMessageID(),
                    msg.getMessageBody(), conn.getAddress(), Integer.valueOf(conn.getId()));
            processMessage(message, conn);
            return;
        }
        simpleClient.incoming(frame, conn);
    }

    private void processMessage(final NSQMessage message, final NSQConnection conn) {
        if (handler == null) {
            logger.error("No MessageHandler then drop the message {}", message);
            return;
        }
        try {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        consume(message, conn);
                        success.incrementAndGet();
                    } catch (Exception e) {
                        IOUtil.closeQuietly(conn);
                        logger.error("Exception", e);
                    }
                }
            });
        } catch (RejectedExecutionException re) {
            try {
                backoff(conn);
                conn.command(new ReQueue(message.getMessageID(), 3));
                logger.info("Do a re-queue. MessageID:{}", message.getMessageID());
                resumeRateLimiting(conn, 0);
            } catch (Exception e) {
                logger.error("I can not handle it MessageID:{}, {}", message.getMessageID(), e);
            }
        }
        resumeRateLimiting(conn, 1000);
    }

    private void resumeRateLimiting(final NSQConnection conn, final int delayInMillisecond) {
        if (executor instanceof ThreadPoolExecutor) {
            if (delayInMillisecond <= 0) {
                final ThreadPoolExecutor pools = (ThreadPoolExecutor) executor;
                final double threshold = pools.getActiveCount() / (1.0D * pools.getPoolSize());
                logger.info("Current status is not good. threshold: {}", threshold);
                if (threshold >= 0.9D) {
                    currentRdy = LOW_RDY;
                } else if (threshold >= 0.8D) {
                    currentRdy = MEDIUM_RDY;
                } else {
                    currentRdy = DEFAULT_RDY;
                }
                // Ignore the data-race
                conn.command(currentRdy);
            } else {
                if (currentRdy != DEFAULT_RDY) {
                    final ThreadPoolExecutor pools = (ThreadPoolExecutor) executor;
                    final double threshold = pools.getActiveCount() / (1.0D * pools.getPoolSize());
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

    private void consume(final NSQMessage message, final NSQConnection conn) {
        boolean ok = false;
        int c = 0;
        while (c++ < 2) {
            try {
                handler.process(message);
                ok = true;
                break;
            } catch (Exception e) {
                ok = false;
                logger.error("CurrentRetries: {} , Exception:", c, e);
            }
        }
        // The client commands ReQueue into NSQd.
        final Integer timeout = message.getNextConsumingInSecond();
        // It is too complex.
        NSQCommand cmd = null;
        if (autoFinish) {
            // Either Finish or ReQueue
            if (ok) {
                // Finish
                cmd = new Finish(message.getMessageID());
            } else {
                if (timeout != null) {
                    // ReQueue
                    cmd = new ReQueue(message.getMessageID(), timeout.intValue());
                    logger.info("Do a re-queue. MessageID: {}", message.getMessageID());
                } else {
                    // Finish
                    cmd = new Finish(message.getMessageID());
                }
            }
        } else {
            // Client code does finish explicitly.
            // Maybe ReQueue, but absolutely no Finish
            if (!ok) {
                if (timeout != null) {
                    // ReQueue
                    cmd = new ReQueue(message.getMessageID(), timeout.intValue());
                    logger.info("Do a re-queue. MessageID: {}", message.getMessageID());
                }
            } else {
                // ignore actions
                cmd = null;
            }
        }
        if (cmd != null) {
            conn.command(cmd);
        }
        // Post
        if (message.getReadableAttempts() > 10) {
            logger.error("{} , Processing 10 times is still a failure!", message);
        }
        if (!ok) {
            logger.error("{} , exception occurs but you don't catch it! Please check it right now!!!", message);
        }
    }

    @Override
    public void backoff(NSQConnection conn) {
        simpleClient.backoff(conn);
    }

    @Override
    public void close() {
        closing.set(true);
        started = false;

        cleanClose();
        if (factory != null) {
            factory.close();
        }
        if (bigPool != null) {
            bigPool.close();
        }
        IOUtil.closeQuietly(simpleClient);
        topics.clear();
    }

    private void cleanClose() {
        for (Set<NSQConnection> connections : holdingConnections.values()) {
            cleanClose(connections);
        }
        holdingConnections.clear();
    }

    private void cleanClose(Set<NSQConnection> connections) {
        if (connections == null) {
            return;
        }
        for (final NSQConnection c : connections) {
            try {
                backoff(c);
                final NSQFrame frame = c.commandAndGetResponse(Close.getInstance());
                handleResponse(frame, c);
            } catch (Exception e) {
                logger.error("Exception", e);
            } finally {
                IOUtil.closeQuietly(c);
            }
        }
    }

    private void handleResponse(NSQFrame frame, NSQConnection connection) throws NSQException {
        if (frame == null) {
            logger.warn("SDK bug: the frame is null.");
            return;
        }
        // TODO
        if (frame != null && frame.getType() == FrameType.ERROR_FRAME) {
            final Response err = ((ErrorFrame) frame).getError();
            if (err != null) {
                logger.error(err.getContent());
            }
        }
        if (frame != null && frame.getType() == FrameType.ERROR_FRAME) {
            final ErrorFrame err = (ErrorFrame) frame;
            logger.error("Address: {} got one error {} , that is {}", connection.getAddress(), err, err.getError());
            switch (err.getError()) {
                case E_FAILED_ON_NOT_LEADER: {
                }
                case E_FAILED_ON_NOT_WRITABLE: {
                }
                case E_TOPIC_NOT_EXIST: {
                    clearDataNode(connection.getAddress());
                    logger.error("Address: {} , Frame: {}", connection.getAddress(), frame);
                    throw new NSQInvalidDataNodeException();
                }
                default: {
                    throw new NSQException("Unknown response error!");
                }
            }
        }
    }

    @Override
    public boolean validateHeartbeat(NSQConnection conn) {
        currentRdy = DEFAULT_RDY;
        final ChannelFuture future = conn.command(currentRdy);
        if (future.awaitUninterruptibly(50, TimeUnit.MILLISECONDS)) {
            return future.isSuccess();
        }
        return false;
    }

    @Override
    public void finish(NSQMessage message) throws NSQException {
        final Set<NSQConnection> connections = holdingConnections.get(message.getAddress());
        if (connections != null) {
            for (NSQConnection c : connections) {
                if (c.getId() == message.getConnectionID().intValue()) {
                    if (c.isConnected()) {
                        c.command(new Finish(message.getMessageID()));
                        // It is OK.
                        return;
                    }
                    break;
                }
            }
        }
        throw new NSQNoConnectionException(
                "The connection is broken so that cann't retry. Please wait next consuming.");
    }

    @Override
    public void setAutoFinish(boolean autoFinish) {
        this.autoFinish = autoFinish;
    }

}
