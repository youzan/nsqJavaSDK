package com.youzan.nsq.client;

import com.youzan.nsq.client.core.NSQConnection;
import com.youzan.nsq.client.core.NSQSimpleClient;
import com.youzan.nsq.client.core.command.*;
import com.youzan.nsq.client.core.pool.consumer.FixedPool;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.entity.Role;
import com.youzan.nsq.client.exception.NSQClientInitializationException;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.exception.NSQNoConnectionException;
import com.youzan.nsq.client.exception.RetryBusinessException;
import com.youzan.nsq.client.network.frame.ErrorFrame;
import com.youzan.nsq.client.network.frame.MessageFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.nsq.client.network.frame.NSQFrame.FrameType;
import com.youzan.util.ConcurrentSortedSet;
import com.youzan.util.IOUtil;
import com.youzan.util.NamedThreadFactory;
import com.youzan.util.ThreadSafe;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
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
    private static final Logger BUSINESS_PERFORMANCE_LOG = LoggerFactory.getLogger("com.youzan.nsq.client.ConsumerImplV2.messageHandlerPerf");
    private final Object lock = new Object();
    private boolean started = false;
    private boolean closing = false;
    private volatile long lastConnecting = 0L;


    private final AtomicInteger queue4Consume = new AtomicInteger(0);
    private final AtomicInteger received = new AtomicInteger(0);
    private final AtomicInteger success = new AtomicInteger(0);
    private final AtomicInteger finished = new AtomicInteger(0);
    private final AtomicInteger re = new AtomicInteger(0); // have done reQueue
    private final AtomicInteger deferred = new AtomicInteger(0); // have done reQueue


    /*-
     * =========================================================================
     * 
     * =========================================================================
     */
    private final Set<String> topics = new HashSet<>(); // client subscribes
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
        this.simpleClient = new NSQSimpleClient(config.getLookupAddresses(), Role.Consumer);


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
        for (String topic : topics) {
            simpleClient.putTopic(topic);
        }
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
                try {
                    this.simpleClient.start();
                } catch (NSQClientInitializationException iniExp) {
                    throw new NSQClientInitializationException("Fail to start Consumer.", iniExp);
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
                logger.info("Client received {} messages , success {} , finished {}, queue4Consume {} , reQueue explicitly {}. The values do not use a lock action.", received, success, finished, queue4Consume, re);
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
        if (topics == null || topics.isEmpty()) {
            return;
        }
        synchronized (lock) {
            if (closing) {
                return;
            }
            final int topicSize = topics.size();
//          final int manualPoolSize = config.getThreadPoolSize4IO();
            final int connectionSize = topicSize;
            if (connectionSize <= 0) {
                return;
            }
            final String[] topicArray = topics.toArray(new String[0]);
            if (connectionSize != topicArray.length) {
                // concurrent problem
                return;
            }

            final FixedPool pool = new FixedPool(address, connectionSize, this, config);
            address_2_pool.put(address, pool);
            pool.prepare();
            List<NSQConnection> connections = pool.getConnections();
            if (connections == null || connections.isEmpty()) {
                logger.info("TopicSize: {} , Address: {}, Connection-Size: {} . The pool is empty.", topicSize, address , connectionSize);
                return;
            }
            logger.info("TopicSize: {} , Address: {} , Connection-Size: {} , Topics: {}", topicSize, address, connectionSize, topics);
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
                    final Sub command = new Sub(topic, config.getConsumerName());
                    final NSQFrame frame = connection.commandAndGetResponse(command);
                    handleResponse(frame, connection);
                    //as there is no success response from nsq, command is enough here
                    connection.command(currentRdy);
                }
                logger.info("Done. Current connection index: {}", i);
            } // end loop creating a connection
        }
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
            queue4Consume.incrementAndGet();
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
        boolean retry = false;
        try {
            long start = System.currentTimeMillis();
            handler.process(message);
            long eclapse = System.currentTimeMillis() - start;
            if(BUSINESS_PERFORMANCE_LOG.isDebugEnabled())
                BUSINESS_PERFORMANCE_LOG.debug("message handler logic ends in {} milliSec, messageID: {}", eclapse, message.getMessageID());
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
                    // Finish: client explicitly sets NextConsumingInSecond is null
                    cmd = new Finish(message.getMessageID());
                    final byte[] id = message.getMessageID();
                    logger.info("Do a finish. MessageID: {} , Hex: {}", id, message.newHexString(id));
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
            ChannelFuture finFuture = connection.command(cmd);
            finFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if(!future.isSuccess()) {
                        logger.warn("Fail to {} message {}. Message will be received from NSQ server again. Cause: {}", cmdStr, message.getAddress(), future.cause().getLocalizedMessage());
                        deferred.incrementAndGet();
                    }
                }
            });
            if (cmd instanceof Finish) {
                finished.incrementAndGet();
            } else {
                re.incrementAndGet();
            }
        }
        queue4Consume.decrementAndGet();
        // Post
        if (message.getReadableAttempts() > 10) {
            logger.warn("Fire,Fire,Fire! Processing 10 times is still a failure!!! {}", message);
        }
        if (!ok) {
            logger.warn("Exception occurs but you don't catch it! Please check it right now!!! {} , Original message: {}.", message, message.getReadableContent());
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


}
