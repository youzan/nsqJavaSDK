package com.youzan.nsq.client;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.core.Client;
import com.youzan.nsq.client.core.KeyedConnectionPoolFactory;
import com.youzan.nsq.client.core.NSQConnection;
import com.youzan.nsq.client.core.NSQSimpleClient;
import com.youzan.nsq.client.core.command.Mpub;
import com.youzan.nsq.client.core.command.Pub;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQDataNodesDownException;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.exception.NSQInvalidDataNodeException;
import com.youzan.nsq.client.exception.NSQInvalidMessageException;
import com.youzan.nsq.client.exception.NSQInvalidTopicException;
import com.youzan.nsq.client.exception.NoConnectionException;
import com.youzan.nsq.client.network.frame.ErrorFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.nsq.client.network.frame.NSQFrame.FrameType;
import com.youzan.util.ConcurrentSortedSet;
import com.youzan.util.IOUtil;
import com.youzan.util.Lists;
import com.youzan.util.NamedThreadFactory;

/**
 * <pre>
 * Use {@code NSQConfig} to set the lookup cluster.
 * It uses one connection pool(client->one broker) underlying TCP and uses
 * {@code GenericKeyedObjectPool} which is composed of many sub-pools.
 * </pre>
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class ProducerImplV2 implements Producer {

    private static final Logger logger = LoggerFactory.getLogger(ProducerImplV2.class);
    private volatile boolean started = false;
    private final Client simpleClient;
    private final NSQConfig config;
    private volatile int offset = 0;
    private final GenericKeyedObjectPoolConfig poolConfig;
    private final KeyedConnectionPoolFactory factory;
    private GenericKeyedObjectPool<Address, NSQConnection> bigPool = null;
    private final AtomicInteger success = new AtomicInteger(0);
    private final AtomicInteger total = new AtomicInteger(0);
    /**
     * Record the client's request time
     */
    private long lastTimeInMillisOfClientRequest = System.currentTimeMillis();

    private final ConcurrentSortedSet<Address> dataNodes = new ConcurrentSortedSet<>();
    private final ScheduledExecutorService scheduler = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory(this.getClass().getName(), Thread.NORM_PRIORITY));

    /**
     * @param config
     */
    public ProducerImplV2(NSQConfig config) {
        this.config = config;
        this.poolConfig = new GenericKeyedObjectPoolConfig();

        this.simpleClient = new NSQSimpleClient(config.getLookupAddresses(), config.getTopic());
        this.factory = new KeyedConnectionPoolFactory(this.config, this);
    }

    @Override
    public void start() throws NSQException {
        final String topic = this.config.getTopic();
        if (topic == null || topic.isEmpty()) {
            throw new NSQException("Please set topic name using {@code NSQConfig}");
        }
        if (!this.started) {
            this.started = true;
            // setting all of the configs
            this.poolConfig.setLifo(true);
            this.poolConfig.setFairness(false);
            this.poolConfig.setTestOnBorrow(true);
            this.poolConfig.setJmxEnabled(false);
            this.poolConfig.setMinIdlePerKey(1);
            this.poolConfig.setMinEvictableIdleTimeMillis(90 * 1000);
            this.poolConfig.setMaxIdlePerKey(this.config.getThreadPoolSize4IO());
            this.poolConfig.setMaxTotalPerKey(this.config.getThreadPoolSize4IO());
            // aquire connection waiting time
            this.poolConfig.setMaxWaitMillis(500);
            this.poolConfig.setBlockWhenExhausted(true);
            this.poolConfig.setTestWhileIdle(true);
            this.offset = _r.nextInt(100);
            try {
                this.simpleClient.start();
                newDataNodes();
            } catch (Exception e) {
                logger.error("Exception", e);
            }
            createBigPool();
            checkDataNodes();
        }
    }

    /**
     * 
     */
    private void checkDataNodes() {
        final int delay = _r.nextInt(60) + 60; // seconds
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                newDataNodes();
            } catch (Exception e) {
                logger.error("Exception", e);
            }
        }, delay, 1 * 30, TimeUnit.SECONDS);
    }

    /**
     * 
     */
    private void newDataNodes() {
        ConcurrentSortedSet<Address> nodes = this.simpleClient.getDataNodes();
        if (nodes != null) {
            this.dataNodes.swap(nodes.newSortedSet());
        }
    }

    /**
     * Create some pools. <br />
     * One pool to one broker.
     */
    private void createBigPool() {
        this.bigPool = new GenericKeyedObjectPool<>(this.factory, this.poolConfig);
    }

    /**
     * Get a connection foreach every broker in one loop because I don't believe
     * that every broker is down or every pool is busy.
     * 
     * TODO Get a connection for the ordered message handler.
     * 
     * 
     * @return NSQConnection that is having done a negotiation
     * @throws NoConnectionException
     */
    protected NSQConnection getNSQConnection() throws NoConnectionException {
        final ConcurrentSortedSet<Address> dataNodes = getDataNodes();
        if (dataNodes.isEmpty()) {
            throw new NoConnectionException("You still didn't start NSQd / lookup-topic / producer.start() ! ");
        }
        final int size = dataNodes.size();
        final Address[] addrs = dataNodes.newArray(new Address[size]);
        int c = 0, index = (this.offset++);
        while (c++ < size) {
            // current broker | next broker when have a try again
            final int effectedIndex = (index++ & Integer.MAX_VALUE) % size;
            logger.debug("Load-Balancing algorithm is Round-Robin! Size: {}, Index: {}", size, effectedIndex);
            final Address addr = addrs[effectedIndex];
            NSQConnection conn = null;
            try {
                conn = bigPool.borrowObject(addr);
                return conn;
            } catch (NoSuchElementException e) {
                // Either the pool is too busy or broker is down.
                // So ugly handler
                final boolean exhausted;
                final String poolTip = e.getMessage();
                if (poolTip != null) {
                    final String tmp = poolTip.trim().toLowerCase();
                    if (tmp.startsWith("pool exhausted")) {
                        exhausted = true;
                    } else if (tmp.contains("exhausted")) {
                        exhausted = true;
                    } else {
                        exhausted = false;
                    }
                } else {
                    exhausted = false;
                }
                if (!exhausted) {
                    // broker is down
                    factory.clear(addr);
                    bigPool.clear(addr);
                }
                logger.error("CurrentRetries: {}, Address: {}, Exception occurs...", c, addr, e);
            } catch (Exception e) {
                logger.error("CurrentRetries: {}, Address: {}, Exception occurs...", c, addr, e);
                IOUtil.closeQuietly(conn);
            }
        }
        return null;
    }

    /**
     * @param millisecond
     */
    private void sleep(final int millisecond) {
        try {
            Thread.sleep(millisecond);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("System is too busy! Please check it!", e);
        }
    }

    @Override
    public void publish(byte[] message) throws NSQException {
        if (!started) {
            throw new IllegalStateException("Producer must be started before producing messages!");
        }
        if (message == null || message.length <= 0) {
            throw new IllegalArgumentException("Your input is blank! Please check it!");
        }
        logger.debug("Begin to publish.");
        total.incrementAndGet();
        lastTimeInMillisOfClientRequest = System.currentTimeMillis();
        final Pub pub = new Pub(config.getTopic(), message);
        int c = 0, retries = 6; // be continuous
        while (c++ < retries) {
            if (c > 1) {
                logger.debug("Sleep. CurrentRetries: {}", c);
                sleep(c * 1000);
            }
            final NSQConnection conn = getNSQConnection();
            if (conn == null) {
                continue;
            }
            logger.debug("Get NSQConnection OK! CurrentRetries: {}", c);
            try {
                final NSQFrame frame = conn.commandAndGetResponse(pub);
                incoming(frame, conn);
                logger.debug("Get frame {} after published. CurrentRetries: {} ", frame, c);
            } catch (Exception e) {
                // Continue to retry
                logger.error("CurrentRetries: {}, Exception occurs...", c, e);
                continue;
            } finally {
                bigPool.returnObject(conn.getAddress(), conn);
            }
        }
        throw new NSQDataNodesDownException();
    }

    @Override
    public void publishMulti(List<byte[]> messages) throws NSQException {
        if (!started) {
            throw new IllegalStateException("Producer must be started before producing messages!");
        }
        if (messages == null || messages.isEmpty()) {
            throw new IllegalArgumentException("Your input is blank!");
        }
        total.addAndGet(messages.size());
        lastTimeInMillisOfClientRequest = System.currentTimeMillis();
        final List<List<byte[]>> batches = Lists.partition(messages, 30);
        for (List<byte[]> batch : batches) {
            publishBatch(batch);
        }
    }

    /**
     * @param batch
     * @throws NoConnectionException
     * @throws NSQDataNodesDownException
     * @throws NSQInvalidTopicException
     * @throws NSQInvalidMessageException
     * @throws NSQException
     */
    private void publishBatch(List<byte[]> batch) throws NoConnectionException, NSQDataNodesDownException,
            NSQInvalidTopicException, NSQInvalidMessageException, NSQException {
        final Mpub pub = new Mpub(config.getTopic(), batch);
        int c = 0; // be continuous
        while (c++ < 6) {
            if (c > 1) {
                logger.debug("Sleep. CurrentRetries: {}", c);
                sleep(c * 1000);
            }
            final NSQConnection conn = getNSQConnection();
            if (conn == null) {
                continue;
            }
            try {
                final NSQFrame frame = conn.commandAndGetResponse(pub);
                incoming(frame, conn);
            } catch (Exception e) {
                // Continue to retry
                logger.error("CurrentRetries: {}, Exception occurs...", c, e);
            } finally {
                bigPool.returnObject(conn.getAddress(), conn);
            }
        }
        throw new NSQDataNodesDownException();
    }

    @Override
    public void incoming(NSQFrame frame, NSQConnection conn) throws NSQException {
        if (frame.getType() == FrameType.ERROR_FRAME) {
            final ErrorFrame err = (ErrorFrame) frame;
            switch (err.getError()) {
                case E_BAD_TOPIC: {
                    conn.addErrorFrame(err);
                    throw new NSQInvalidTopicException();
                }
                case E_BAD_MESSAGE: {
                    conn.addErrorFrame(err);
                    throw new NSQInvalidMessageException();
                }
                case E_FAILED_ON_NOT_LEADER: {
                }
                case E_FAILED_ON_NOT_WRITABLE: {
                }
                case E_TOPIC_NOT_EXIST: {
                    conn.addErrorFrame(err);
                    final Address address = conn.getAddress();
                    if (address != null) {
                        factory.clear(address);
                        bigPool.clear(address);
                        dataNodes.remove(address);
                    }
                    throw new NSQInvalidDataNodeException();
                }
                default: {
                    break;
                }
            }
        }
        simpleClient.incoming(frame, conn);
    }

    @Override
    public void backoff(NSQConnection conn) {
        simpleClient.backoff(conn);
    }

    @Override
    public void close() {
        if (factory != null) {
            factory.close();
        }
        if (bigPool != null) {
            bigPool.close();
        }
    }

    @Override
    public ConcurrentSortedSet<Address> getDataNodes() {
        return dataNodes;
    }
}
