package com.youzan.nsq.client;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.core.Client;
import com.youzan.nsq.client.core.KeyedPooledConnectionFactory;
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

/**
 * <pre>
 * Use {@code NSQConfig} to set the lookup cluster.
 * It uses one connection pool(client connects to one broker) underlying TCP and uses
 * {@code GenericKeyedObjectPool} which is composed of many sub-pools.
 * </pre>
 * 
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public class ProducerImplV2 implements Producer {

    private static final Logger logger = LoggerFactory.getLogger(ProducerImplV2.class);
    private volatile boolean started = false;
    private final Client simpleClient;
    private final NSQConfig config;
    private volatile int offset = 0;
    private final GenericKeyedObjectPoolConfig poolConfig;
    private final KeyedPooledConnectionFactory factory;
    private GenericKeyedObjectPool<Address, NSQConnection> bigPool = null;
    private final AtomicInteger success = new AtomicInteger(0);
    private final AtomicInteger total = new AtomicInteger(0);
    /**
     * Record the client's request time
     */
    private long lastTimeInMillisOfClientRequest = System.currentTimeMillis();

    /**
     * @param config
     *            NSQConfig
     */
    public ProducerImplV2(NSQConfig config) {
        this.config = config;
        this.poolConfig = new GenericKeyedObjectPoolConfig();

        this.simpleClient = new NSQSimpleClient(config.getLookupAddresses());
        this.factory = new KeyedPooledConnectionFactory(this.config, this);
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
            this.poolConfig.setTestOnReturn(false);
            this.poolConfig.setTestWhileIdle(true);
            this.poolConfig.setJmxEnabled(false);
            // because of the latency's insensitivity, the Idle tiemout * 1.5
            // should be less-equals than CheckPeriod.
            this.poolConfig.setMinEvictableIdleTimeMillis(60 * 1000);
            this.poolConfig.setTimeBetweenEvictionRunsMillis(2 * 60 * 1000);
            this.poolConfig.setMinIdlePerKey(1);
            this.poolConfig.setMaxIdlePerKey(this.config.getThreadPoolSize4IO());
            this.poolConfig.setMaxTotalPerKey(this.config.getThreadPoolSize4IO());
            // aquire connection waiting time
            this.poolConfig.setMaxWaitMillis(50);
            this.poolConfig.setBlockWhenExhausted(true);
            this.offset = _r.nextInt(100);
            try {
                this.simpleClient.start();
            } catch (Exception e) {
                logger.error("Exception", e);
            }
            createBigPool();
        }
        logger.debug("Producer is started.");
    }

    /**
     * new instance without performing to connect
     */
    private void createBigPool() {
        this.bigPool = new GenericKeyedObjectPool<>(this.factory, this.poolConfig);
    }

    /**
     * Get a connection foreach every broker in one loop because I don't believe
     * that every broker is down or every pool is busy.
     * 
     * @param topic
     *             a topic name 
     * @return a validated {@code NSQConnection}
     * @throws NoConnectionException
     *             that is having done a negotiation
     */
    protected NSQConnection getNSQConnection(String topic) throws NoConnectionException {
        final ConcurrentSortedSet<Address> dataNodes = getDataNodes(topic);
        if (dataNodes.isEmpty()) {
            throw new NoConnectionException("You still didn't start NSQd / lookup-topic / producer.start() !");
        }
        final int size = dataNodes.size();
        final Address[] addrs = dataNodes.newArray(new Address[size]);
        int c = 0, index = (this.offset++);
        while (c++ < size) {
            // current broker | next broker when have a try again
            final int effectedIndex = (index++ & Integer.MAX_VALUE) % size;
            final Address addr = addrs[effectedIndex];
            logger.debug("Load-Balancing algorithm is Round-Robin! Size: {} , Index: {} , Got {}", size, effectedIndex,
                    addr);
            NSQConnection conn = null;
            try {
                logger.debug("Begin to borrowObject from the address: {}", addr);
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
                    clearDataNode(addr);
                }
                logger.error("CurrentRetries: {} , Address: {} , Exception: {}", c, addr, e);
            } catch (Exception e) {
                IOUtil.closeQuietly(conn);
                logger.error("CurrentRetries: {} , Address: {} , Exception: {}", c, addr, e);
            }
        }
        /**
         * no available {@code NSQConnection}
         */
        return null;
    }

    @Override
    public void publish(byte[] message, String topic) throws NSQException {
        if (!started) {
            throw new IllegalStateException("Producer must be started before producing messages!");
        }
        if (message == null || message.length <= 0) {
            throw new IllegalArgumentException("Your input is blank! Please check it!");
        }
        if (null == topic || topic.isEmpty()) {
            throw new IllegalArgumentException("Topic name is blank!");
        }
        total.incrementAndGet();
        lastTimeInMillisOfClientRequest = System.currentTimeMillis();
        final Pub pub = new Pub(topic, message);
        int c = 0; // be continuous
        while (c++ < 6) {
            if (c > 1) {
                logger.debug("Sleep. CurrentRetries: {}", c);
                sleep((1 << (c - 1)) * 1000);
            }
            final NSQConnection conn = getNSQConnection(topic);
            if (conn == null) {
                // Continue to retry
                continue;
            }
            logger.debug("Having acquired a {} NSQConnection! CurrentRetries: {}", conn.getAddress(), c);
            try {
                final NSQFrame frame = conn.commandAndGetResponse(pub);
                // delegate to method: incomming(...)
                return;
            } catch (Exception e) {
                IOUtil.closeQuietly(conn);
                logger.error("CurrentRetries: {} , Address: {} , Topic: {}, RawMessage: {} , Exception: ", c,
                        conn.getAddress(), topic, message, e);
                // Continue to retry
                continue;
            } finally {
                bigPool.returnObject(conn.getAddress(), conn);
            }
        }
        throw new NSQDataNodesDownException();
    }

    @Override
    public void publish(String message) throws NSQException {
        if (message == null || message.isEmpty()) {
            throw new NSQInvalidMessageException("Your input is blank!");
        }
        publish(message.getBytes(IOUtil.DEFAULT_CHARSET));
    }

    @Override
    public void publish(byte[] message) throws NSQException {
        publish(message, config.getTopic());
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
            publishSmallBatch(batch);
        }
    }

    /**
     * @param batch
     * @throws NSQException
     */
    private void publishSmallBatch(List<byte[]> batch) throws NSQException {
        final Mpub pub = new Mpub(config.getTopic(), batch);
        int c = 0; // be continuous
        while (c++ < 6) {
            if (c > 1) {
                logger.debug("Sleep. CurrentRetries: {}", c);
                sleep((1 << (c - 1)) * 1000);
            }
            final NSQConnection conn = getNSQConnection(config.getTopic());
            if (conn == null) {
                // Continue to retry
                continue;
            }
            try {
                final NSQFrame frame = conn.commandAndGetResponse(pub);
                // delegate to method: incomming(...)
                return;
            } catch (Exception e) {
                IOUtil.closeQuietly(conn);
                // Continue to retry
                logger.error("CurrentRetries: {} , Address: {} , Exception: {}", c, conn.getAddress(), e);
                continue;
            } finally {
                bigPool.returnObject(conn.getAddress(), conn);
            }
        }
        throw new NSQDataNodesDownException();
    }

    @Override
    public void incoming(NSQFrame frame, NSQConnection conn) throws NSQException {
        if (frame != null && frame.getType() == FrameType.ERROR_FRAME) {
            final ErrorFrame err = (ErrorFrame) frame;
            switch (err.getError()) {
                case E_BAD_TOPIC: {
                    throw new NSQInvalidTopicException();
                }
                case E_BAD_MESSAGE: {
                    throw new NSQInvalidMessageException();
                }
                case E_FAILED_ON_NOT_LEADER: {
                }
                case E_FAILED_ON_NOT_WRITABLE: {
                }
                case E_TOPIC_NOT_EXIST: {
                    clearDataNode(conn.getAddress());
                    logger.error("Adress: {} , Frame: {}", conn.getAddress(), frame);
                    throw new NSQInvalidDataNodeException();
                }
                default: {
                    throw new NSQException("Unkown response error!");
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
    public void clearDataNode(final Address address) {
        if (address == null) {
            return;
        }
        factory.clear(address);
        bigPool.clear(address);
        simpleClient.clearDataNode(address);
    }

    @Override
    public void close() {
        if (factory != null) {
            factory.close();
        }
        if (bigPool != null) {
            bigPool.close();
        }
        IOUtil.closeQuietly(simpleClient);
    }

    @Override
    public ConcurrentSortedSet<Address> getDataNodes(String topic) {
        return simpleClient.getDataNodes(topic);
    }

    @Override
    public boolean validateHeartbeat(NSQConnection conn) {
        return simpleClient.validateHeartbeat(conn);
    }

    void sleep(final long millisecond) {
        logger.debug("Sleep {} millisecond.", millisecond);
        try {
            Thread.sleep(millisecond);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Your machine is too busy! Please check it!");
        }
    }

}
