package com.youzan.nsq.client;

import com.youzan.nsq.client.core.NSQConnection;
import com.youzan.nsq.client.core.NSQSimpleClient;
import com.youzan.nsq.client.core.command.*;
import com.youzan.nsq.client.core.pool.consumer.FixedPool;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.exception.NSQNoConnectionException;
import com.youzan.nsq.client.network.frame.ErrorFrame;
import com.youzan.nsq.client.network.frame.MessageFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.nsq.client.network.frame.NSQFrame.FrameType;
import com.youzan.util.ConcurrentSortedSet;
import com.youzan.util.IOUtil;
import com.youzan.util.NamedThreadFactory;
import com.youzan.util.ThreadSafe;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final Object lock = new Object();
    private boolean started = false;
    private boolean closing = false;
    private volatile long lastConnecting = 0L;


    private final AtomicInteger received = new AtomicInteger(0);
    private final AtomicInteger success = new AtomicInteger(0);

    /*-
     * =========================================================================
     * 
     * =========================================================================
     */
    private final Set<String> topics = new HashSet<>(); // client subscribes
    private final ConcurrentHashMap<Address, FixedPool> address_2_pool = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory(this.getClass().getName(), Thread.NORM_PRIORITY));

    /*-
      * =========================================================================
      *                          Client delegates to me
      */
    private final MessageHandler handler;
    private final int WORKER_SIZE = Runtime.getRuntime().availableProcessors() * 4;
    private final ExecutorService executor = Executors.newFixedThreadPool(WORKER_SIZE,
            new NamedThreadFactory(this.getClass().getName() + "-ClientBusiness", Thread.MAX_PRIORITY));
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
        this.simpleClient = new NSQSimpleClient(config.getLookupAddresses());


        final int messagesPerBatch = config.getRdy();
        DEFAULT_RDY = new Rdy(Math.max(messagesPerBatch, 1));
        MEDIUM_RDY = new Rdy(Math.max((int) (messagesPerBatch * 0.5D), 1));
        currentRdy = DEFAULT_RDY;
        logger.info("Initialize the Rdy, that is {}", currentRdy);
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
        if (this.topics.isEmpty()) {
            logger.warn("Are you kidding me? You do not subscribe any topic.");
        }
        synchronized (lock) {
            if (!this.started) {
                // create the pools
                this.simpleClient.start();
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
                try {
                    connect();
                } catch (Exception e) {
                    logger.error("Exception", e);
                }
                logger.info("Client received {} messages , success {} . Two values do not use a lock action.", received, success);
            }
        }, delay, _INTERVAL_IN_SECOND, TimeUnit.SECONDS);
    }

    /**
     * Connect to all the brokers with the config, making sure the new is OK
     * and the old is clear.
     */
    private void connect() throws NSQException {
        if (System.currentTimeMillis() < lastConnecting + TimeUnit.SECONDS.toMillis(_INTERVAL_IN_SECOND)) {
            return;
        }
        lastConnecting = System.currentTimeMillis();
        if (!this.started) {
            if (closing) {
                logger.error("You have closed the consumer sometimes ago!");
            }
            return;
        }
        if (this.topics.isEmpty()) {
            logger.error("Are you kidding me? You did not subscribe any topic. Please check it right now!");
            // Still check the resources , do not return right now
        }

        final Set<Address> broken = new HashSet<>();
        final ConcurrentHashMap<Address, Set<String>> address_2_topics = new ConcurrentHashMap<>();
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
        for (String topic : topics) {
            // maybe a exception occurs
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
            for (Address address : except1) {
                try {
                    final Set<String> topics = address_2_topics.get(address);
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
    private void connect(Address address, Set<String> topics) throws Exception {
        if (topics == null) {
            return;
        }
        final int topicSize = topics.size();
        final int manualPoolSize = config.getThreadPoolSize4IO();
        final int connectionSize = manualPoolSize * topicSize;
        if (connectionSize <= 0) {
            return;
        }
        final String[] topicArray = new String[topicSize];
        topics.toArray(topicArray);
        if (connectionSize != manualPoolSize * topicArray.length) {
            // concurrent problem
            return;
        }

        final FixedPool pool = new FixedPool(address, connectionSize, this, config);
        address_2_pool.put(address, pool);
        pool.prepare();
        List<NSQConnection> connections = pool.getConnections();
        for (int i = 0; i < connectionSize; i++) {
            int k = i % topicArray.length;
            assert k < topicArray.length;
            // init( connection, topic ) , let it be a consumer connection
            final NSQConnection connection = connections.get(i);
            final String topic = topicArray[k];
            try {
                connection.init();
            } catch (Exception e) {
                connection.close();
                throw new NSQNoConnectionException("Creating a connection and having a negotiation fails!", e);
            }
            if (!connection.isConnected()) {
                connection.close();
                throw new NSQNoConnectionException("Pool failed in connecting to NSQd!");
            } else {
                final Sub command = new Sub(topic, config.getConsumerName());
                final NSQFrame frame = connection.commandAndGetResponse(command);
                handleResponse(frame, connection);
                connection.command(currentRdy);
            }
        }
        logger.info("TopicSize: {} , ThreadPoolSize4IO: {} , Connection-Size: {}", topicSize, manualPoolSize, connectionSize);
    }

    /**
     * No any exception
     *
     * @param address the data-node(NSQd)'s address
     */
    public void clearDataNode(Address address) {
        if (address == null) {
            return;
        }
        if (!address_2_pool.containsKey(address)) {
            return;
        }
        final FixedPool pool = address_2_pool.get(address);
        address_2_pool.remove(address);
        if (pool != null) {
            final List<NSQConnection> connections = pool.getConnections();
            for (NSQConnection connection : connections) {
                try {
                    cleanClose(connection);
                } catch (Exception e) {
                    logger.error("It can not backoff the connection! Exception:", e);
                } finally {
                    connection.close();
                }
            }
            pool.close();
        }
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
            final NSQMessage message = new NSQMessage(msg.getTimestamp(), msg.getAttempts(), msg.getMessageID(),
                    msg.getMessageBody(), conn.getAddress(), Integer.valueOf(conn.getId()));
            processMessage(message, conn);
        } else {
            simpleClient.incoming(frame, conn);
        }
    }

    private void processMessage(final NSQMessage message, final NSQConnection connection) {
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
                    }
                }
            });
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
                conn.command(currentRdy);
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
        boolean ok = false;
        int c = 0;
        while ((c = c + 1) < 2) {
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
            connection.command(cmd);
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
    @ThreadSafe
    public void backoff(NSQConnection conn) {
        synchronized (conn) {
            simpleClient.backoff(conn);
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            started = false;
            closing = true;
            cleanClose();
            IOUtil.closeQuietly(simpleClient);
        }
        logger.info("The consumer has been closed.");
    }

    @ThreadSafe
    public void close(NSQConnection connection) {
        synchronized (connection) {
            try {
                cleanClose(connection);
            } catch (TimeoutException e) {
                logger.error("Exception", e);
            } finally {
                connection.close();
            }
        }
    }

    private void cleanClose() {
        final Set<Address> addresses = address_2_pool.keySet();
        if (!addresses.isEmpty()) {
            for (Address address : addresses) {
                clearDataNode(address);
            }
        }
        address_2_pool.clear();
        topics.clear();
    }

    @ThreadSafe
    private void cleanClose(NSQConnection connection) throws TimeoutException {
        synchronized (connection) {
            if (connection.isConnected()) {
                backoff(connection);
                sleep(2);
                final NSQFrame frame = connection.commandAndGetResponse(Close.getInstance());
                if (frame != null) {
                    handleResponse(frame, connection);
                }
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
                }
                case E_FAILED_ON_NOT_WRITABLE: {
                }
                case E_TOPIC_NOT_EXIST: {
                    Address address = connection.getAddress();
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


}
