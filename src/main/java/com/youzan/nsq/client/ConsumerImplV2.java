package com.youzan.nsq.client;

import java.util.Random;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.core.Client;
import com.youzan.nsq.client.core.KeyedConnectionPoolFactory;
import com.youzan.nsq.client.core.NSQConnection;
import com.youzan.nsq.client.core.NSQSimpleClient;
import com.youzan.nsq.client.core.lookup.NSQLookupService;
import com.youzan.nsq.client.core.lookup.NSQLookupServiceImpl;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.util.ConcurrentSortedSet;

/**
 * Use {@code NSQConfig} to set the lookup cluster. <br />
 * Expose to Client Code. Connect to one cluster(includes many brokers).
 * 
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class ConsumerImplV2 implements Consumer {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerImplV2.class);
    private volatile boolean started = false;

    private final Client simpleClient;

    private final NSQConfig config;
    private volatile NSQLookupService migratingLookup = null;
    /**
     * NSQd Servers
     */
    private final ConcurrentSortedSet<Address> dataNodes = new ConcurrentSortedSet<>();
    private volatile int offset = 0;
    private final NSQLookupService lookup;
    private final GenericKeyedObjectPoolConfig poolConfig;
    private final KeyedConnectionPoolFactory factory;
    private GenericKeyedObjectPool<Address, NSQConnection> bigPool = null;

    private final AtomicInteger success = new AtomicInteger(0);
    private final AtomicInteger total = new AtomicInteger(0);

    /**
     * Record the client's request time
     */
    private volatile long lastTimeInMillisOfClientRequest = System.currentTimeMillis();

    /**
     * @param config
     * @param handler
     */
    public ConsumerImplV2(NSQConfig config, MessageHandler handler) {
        this.config = config;
        this.poolConfig = new GenericKeyedObjectPoolConfig();

        this.lookup = new NSQLookupServiceImpl(config.getLookupAddresses());
        this.simpleClient = new NSQSimpleClient();
        this.factory = new KeyedConnectionPoolFactory(this.config, this);
    }

    @Override
    public Consumer start() throws NSQException {
        if (this.config.getConsumerName() == null || this.config.getConsumerName().isEmpty()) {
            throw new IllegalArgumentException("Consumer Name is blank! Please check it!");
        }
        if (!this.started) {
            this.started = true;

            // setting all of the configs
            this.poolConfig.setFairness(false);
            this.poolConfig.setTestOnBorrow(false);
            this.poolConfig.setJmxEnabled(false);
            this.poolConfig.setMinIdlePerKey(1);
            this.poolConfig.setMinEvictableIdleTimeMillis(90 * 1000);
            this.poolConfig.setMaxIdlePerKey(this.config.getThreadPoolSize4IO());
            this.poolConfig.setMaxTotalPerKey(this.config.getThreadPoolSize4IO());
            // aquire connection waiting time
            this.poolConfig.setMaxWaitMillis(500);
            this.poolConfig.setBlockWhenExhausted(true);
            this.poolConfig.setTestWhileIdle(true);

            final String topic = this.config.getTopic();
            if (topic == null || topic.isEmpty()) {
                throw new NSQException("Please set topic name using {@code NSQConfig}");
            }
            int c = 0;
            while (c++ < 3) { // 0,1,2
                try {
                    final SortedSet<Address> nodes = this.lookup.lookup(this.config.getTopic());
                    if (nodes != null && !nodes.isEmpty()) {
                        this.dataNodes.swap(nodes);
                        break;
                    }
                } catch (Exception e) {
                    logger.error("Exception", e);
                }
                sleep(1000 * c);
            }
            final Random r = new Random(10000);
            this.offset = r.nextInt(100);
            createBigPool();
        }
        return this;
    }

    /**
     * 
     */
    private void createBigPool() {
        this.bigPool = new GenericKeyedObjectPool<>(this.factory, this.poolConfig);
        assert this.bigPool != null;
        // TODO new connection directly.
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
        if (dataNodes != null && !dataNodes.isEmpty()) {
            dataNodes.clear();
        }
    }
}
