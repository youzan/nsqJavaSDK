package com.youzan.nsq.client;

import java.util.List;
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
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.exception.NoConnectionException;
import com.youzan.nsq.client.network.frame.NSQFrame;

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
    public Producer start() {
        if (!started) {
            started = true;
            // TODO setting all of the configs
            // TODO lookup NSQd
            createBigPool();
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
     * @return
     * @throws NoConnectionException
     */
    protected Connection getNSQConnection() throws NoConnectionException {
        // TODO getConnection from big pool. try the best. do a negotiation
        return null;
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
    public void publish(String topic, byte[] message) throws NSQException {
        if (!started) {
            throw new IllegalStateException("Producer must be started before producing messages!");
        }
        // TODO loop all NSQd when the connection attempt fails
        // TODO try 2 times getNSQConnection. sleep(1 second)
        final Connection conn = getNSQConnection();
    }

    @Override
    public void publishMulti(String topic, List<byte[]> messages) throws NSQException {
        if (!started) {
            throw new IllegalStateException("Producer must be started before producing messages!");
        }
    }

    @Override
    public void incoming(NSQFrame frame, Connection conn) {
    }

    @Override
    public void identify(Connection conn) throws NSQException {
        this.simpleClient.identify(conn);
    }

    @Override
    public void backoff(Connection conn) throws NSQException {
        this.simpleClient.backoff(conn);
    }

}
