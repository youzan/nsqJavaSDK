package com.youzan.nsq.client;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.core.Client;
import com.youzan.nsq.client.core.Connection;
import com.youzan.nsq.client.core.KeyedConnectionPoolFactory;
import com.youzan.nsq.client.core.NSQSimpleClient;
import com.youzan.nsq.client.core.command.Pub;
import com.youzan.nsq.client.core.lookup.NSQLookupService;
import com.youzan.nsq.client.core.lookup.NSQLookupServiceImpl;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQDataNodeDownException;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.exception.NoConnectionException;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.util.IOUtil;

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
    private final Client simpleClient;

    private volatile boolean started = false;

    private final NSQConfig config;
    private volatile NSQLookupService migratingLookup = null;
    /**
     * NSQd Servers
     */
    private final SortedSet<Address> dataNodes = new TreeSet<>();
    private volatile int offset = 0;
    private final NSQLookupService lookup;
    private GenericKeyedObjectPoolConfig poolConfig = null;
    private KeyedConnectionPoolFactory factory;
    private GenericKeyedObjectPool<Address, Connection> bigPool = null;

    /**
     * @param config
     */
    public ProducerImplV2(NSQConfig config) {
        this.config = config;
        this.poolConfig = new GenericKeyedObjectPoolConfig();

        this.lookup = new NSQLookupServiceImpl(config.getLookupAddresses());
        this.simpleClient = new NSQSimpleClient(this.config);
        this.factory = new KeyedConnectionPoolFactory(this.config);
    }

    @Override
    public Producer start() throws NSQException {
        if (!started) {
            started = true;
            // setting all of the configs
            poolConfig.setFairness(false);
            poolConfig.setTestOnBorrow(true);
            poolConfig.setJmxEnabled(false);
            poolConfig.setMinIdlePerKey(1);
            poolConfig.setMinEvictableIdleTimeMillis(90 * 1000);
            poolConfig.setMaxIdlePerKey(this.config.getThreadPoolSize4IO());
            poolConfig.setMaxTotalPerKey(this.config.getThreadPoolSize4IO());
            poolConfig.setMaxWaitMillis(500); // aquire connection waiting time
            poolConfig.setBlockWhenExhausted(false);
            poolConfig.setTestWhileIdle(true);
            createBigPool();
            final String topic = this.getConfig().getTopic();
            if (topic == null || topic.isEmpty()) {
                throw new NSQException("Please set topic name using {@code NSQConfig}");
            }
            final int retries = 2;
            int c = 0;
            while (c++ < retries) {
                try {
                    SortedSet<Address> nodes = this.lookup.lookup(this.getConfig().getTopic());
                    if (nodes != null && !nodes.isEmpty()) {
                        this.dataNodes.addAll(nodes);
                        break;
                    }
                } catch (Exception e) {
                    logger.error("Exception", e);
                }
                sleep(1000 * c);
            }
            final Random r = new Random(10000);
            this.offset = r.nextInt(100);
        }
        return this;
    }

    /**
     * Create some pools. <br />
     * One pool to one broker.
     */
    private void createBigPool() {
        this.bigPool = new GenericKeyedObjectPool<>(this.factory, this.poolConfig);
        assert this.bigPool != null;
    }

    /**
     * TODO Get a connection for the ordered message handler
     * 
     * @return NSQConnection that is having done a negotiation
     * @throws NoConnectionException
     */
    protected Connection getNSQConnection() throws NoConnectionException {
        assert this.dataNodes != null && !this.dataNodes.isEmpty();
        final int size = this.dataNodes.size();
        if (size < 1) {
            throw new NoConnectionException("You still didn't start NSQd / lookup-topic / producer.start() ! ");
        }
        final int retries = size + 1;
        final Address[] addrs = this.dataNodes.toArray(new Address[size]);
        int c = 0;
        while (c++ < retries) {
            final int index = (this.offset++ & Integer.MAX_VALUE) % size;
            Address addr = addrs[index];
            Connection conn = null;
            try {
                conn = this.bigPool.borrowObject(addr);
                if (null != conn) {
                    if (!conn.isHavingNegotiation()) {
                        this.negotiate(conn);
                    }
                    if (conn.isHavingNegotiation()) {
                        return conn;
                    } else {
                        IOUtil.closeQuietly(conn);
                    }
                    this.bigPool.returnObject(addr, conn);
                }
            } catch (NoSuchElementException e) {
                // Either the pool is too busy or NSQd is down.
                logger.error("Exception", e);
            } catch (Exception e) {
                logger.error("Exception", e);
                IOUtil.closeQuietly(conn);
            }
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
    public void close() {
        factory.close();
        bigPool.close();
        dataNodes.clear();
    }

    @Override
    public NSQConfig getConfig() {
        return this.config;
    }

    private void doOncePublish(String topic, byte[] message, Connection conn) throws Exception {
        final Pub command = new Pub(topic, message);
        final NSQFrame resp = conn.commandAndGetResponse(command);
        if (null != resp) {
            switch (resp.getType()) {
                case RESPONSE_FRAME: {
                    return;
                }
                case ERROR_FRAME: {
                    break;
                }
                default: {
                    throw new NSQException("Client can not parse the frame type!");
                }
            }
        }
    }

    @Override
    public void publish(byte[] message) throws NSQException {
        if (!started) {
            throw new IllegalStateException("Producer must be started before producing messages!");
        }
        // TODO loop all NSQd when the connection attempt fails
        // TODO try 2 times getNSQConnection. sleep(1 second)
        final Connection conn = getNSQConnection();
        if (conn == null) {
            throw new NSQDataNodeDownException();
        } else {

        }
    }

    @Override
    public void publishMulti(List<byte[]> messages) throws NSQException {
        if (!started) {
            throw new IllegalStateException("Producer must be started before producing messages!");
        }
    }

    @Override
    public void incoming(NSQFrame frame, Connection conn) {
    }

    @Override
    public void negotiate(Connection conn) throws NSQException {
        this.simpleClient.negotiate(conn);
    }

    @Override
    public void backoff(Connection conn) throws NSQException {
        this.simpleClient.backoff(conn);
    }

}
