package com.youzan.nsq.client;

import java.util.List;
import java.util.NoSuchElementException;
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
import com.youzan.nsq.client.entity.Response;
import com.youzan.nsq.client.exception.NSQDataNodeDownException;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.exception.NSQInvalidMessageException;
import com.youzan.nsq.client.exception.NSQInvalidTopicException;
import com.youzan.nsq.client.exception.NoConnectionException;
import com.youzan.nsq.client.network.frame.ErrorFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.util.ConcurrentSortedSet;
import com.youzan.util.IOUtil;
import com.youzan.util.Lists;

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
            this.poolConfig.setBlockWhenExhausted(false);
            this.poolConfig.setTestWhileIdle(true);
            this.simpleClient.start();
            this.offset = _r.nextInt(100);
            createBigPool();
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
     * Get a connection for the ordered message handler
     * 
     * @return NSQConnection that is having done a negotiation
     * @throws NoConnectionException
     */
    protected NSQConnection getNSQConnection() throws NoConnectionException {
        final ConcurrentSortedSet<Address> dataNodes = getDataNodes();
        final int size = dataNodes.size();
        if (size < 1) {
            throw new NoConnectionException("You still didn't start NSQd / lookup-topic / producer.start() ! ");
        }
        final int retries = size + 1;
        final Address[] addrs = dataNodes.newArray(new Address[size]);
        int c = 0;
        while (c++ < retries) {
            final int index = (offset++ & Integer.MAX_VALUE) % size;
            final Address addr = addrs[index];
            logger.debug("Load-Balancing algorithm is Round-Robin! Size: {}, Index: {}", size, index);
            NSQConnection conn = null;
            try {
                conn = bigPool.borrowObject(addr);
                return conn;
            } catch (NoSuchElementException e) {
                // Either the pool is too busy or NSQd is down.
                logger.error("Exception", e);
            } catch (Exception e) {
                logger.error("Exception", e);
                IOUtil.closeQuietly(conn);
            }
            factory.clear(addr);
            bigPool.clear(addr);
            // End one round so that let system wait 1 second
            if (c == size) {
                sleep(1000);
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
        total.incrementAndGet();
        lastTimeInMillisOfClientRequest = System.currentTimeMillis();
        final Pub pub = new Pub(config.getTopic(), message);
        int c = 0, retries = 2; // be continuous
        conn: while (c++ < retries) { // 0,1,2
            final NSQConnection conn = getNSQConnection();
            if (conn == null) {
                // Fatal error. SDK cann't handle it.
                throw new NSQDataNodeDownException();
            }
            NSQFrame resp = null;
            try {
                resp = conn.commandAndGetResponse(pub);
            } catch (Exception e) {
                // Continue to retry
                logger.error("Exception", e);
            } finally {
                bigPool.returnObject(conn.getAddress(), conn);
            }
            if (resp == null) {
                continue;
            }
            s: switch (resp.getType()) {
                case RESPONSE_FRAME: {
                    final String content = resp.getMessage();
                    if (Response.OK.getContent().equals(content)) {
                        success.incrementAndGet();
                        return;
                    }
                    break s;
                }
                case ERROR_FRAME: {
                    final ErrorFrame err = (ErrorFrame) resp;
                    switch (err.getError()) {
                        case E_BAD_TOPIC: {
                            throw new NSQInvalidTopicException();
                        }
                        case E_BAD_MESSAGE: {
                            throw new NSQInvalidMessageException();
                        }
                        case E_FAILED_ON_NOT_LEADER: {
                            retries++;
                            continue conn;
                        }
                        case E_FAILED_ON_NOT_WRITABLE: {
                            retries++;
                            continue conn;
                        }
                        case E_TOPIC_NOT_EXIST: {
                            retries++;
                            continue conn;
                        }
                        default: {
                            throw new NSQException(err.getMessage());
                        }
                    }
                }
                default: {
                    break s;
                }
            } // end handling {@code Response}
            sleep(c * 1000);
        }
        throw new NSQDataNodeDownException();
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
     * @throws NSQDataNodeDownException
     * @throws NSQInvalidTopicException
     * @throws NSQInvalidMessageException
     * @throws NSQException
     */
    private void publishBatch(List<byte[]> batch) throws NoConnectionException, NSQDataNodeDownException,
            NSQInvalidTopicException, NSQInvalidMessageException, NSQException {
        final Mpub pub = new Mpub(config.getTopic(), batch);
        int c = 0; // be continuous
        while (c++ < 3) { // 0,1,2
            final NSQConnection conn = getNSQConnection();
            if (conn == null) {
                // Fatal error. SDK cann't handle it.
                throw new NSQDataNodeDownException();
            }
            NSQFrame resp = null;
            try {
                resp = conn.commandAndGetResponse(pub);
            } catch (Exception e) {
                // Continue to retry
                logger.error("Exception", e);
            } finally {
                bigPool.returnObject(conn.getAddress(), conn);
            }
            if (resp == null) {
                continue;
            }
            s: switch (resp.getType()) {
                case RESPONSE_FRAME: {
                    final String content = resp.getMessage();
                    if (Response.OK.getContent().equals(content)) {
                        success.addAndGet(batch.size());
                        return;
                    }
                    break s;
                }
                case ERROR_FRAME: {
                    final ErrorFrame err = (ErrorFrame) resp;
                    switch (err.getError()) {
                        case E_BAD_TOPIC: {
                            throw new NSQInvalidTopicException();
                        }
                        case E_BAD_MESSAGE: {
                            throw new NSQInvalidMessageException();
                        }
                        default: {
                            throw new NSQException(err.getMessage());
                        }
                    }
                }
                default: {
                    break s;
                }
            } // end handling {@code Response}
            sleep(c * 1000);
        }
        throw new NSQDataNodeDownException();
    }

    @Override
    public void incoming(NSQFrame frame, NSQConnection conn) {
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
        return simpleClient.getDataNodes();
    }
}
