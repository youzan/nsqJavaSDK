package com.youzan.nsq.client;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.core.Client;
import com.youzan.nsq.client.core.Connection;
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
 * Use {@code NSQConfig} to set the lookup cluster.<br />
 * It uses one connection pool(client->one broker) underlying TCP and uses
 * {@code GenericKeyedObjectPool} which is composed of many sub-pools.
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class ProducerImplV2 implements Producer {

    private static final Logger logger = LoggerFactory.getLogger(ProducerImplV2.class);
    private final Client simpleClient;

    private volatile boolean started = false;
    private ExecutorService executor = Executors.newCachedThreadPool();

    private volatile NSQLookupService migratingLookup = null;
    private final NSQLookupService lookup;

    private final NSQConfig config;
    private GenericKeyedObjectPoolConfig poolConfig = null;
    private GenericKeyedObjectPool<Address, Connection> bigPool = null;

    /**
     * @param config
     */
    public ProducerImplV2(NSQConfig config) {
        this.config = config;
        this.lookup = new NSQLookupServiceImpl(config.getLookupAddresses());
        this.simpleClient = new NSQSimpleClient(this.config);
    }

    @Override
    public Producer start() {
        if (!started) {
            started = true;
            createBigPool();
        }
        return this;
    }

    /**
     * Create some pools. <br />
     * One pool to one broker.
     */
    private void createBigPool() {
    }

    /**
     * TODO Get a connection for the ordered message handler
     * 
     * @return
     * @throws NoConnectionException
     */
    protected Connection getConnection() throws NoConnectionException {
        return null;
    }

    @Override
    public void close() {
        // How can we do, even if IOException occurs.
        bigPool.close();
    }

    @Override
    public NSQConfig getConfig() {
        return this.config;
    }

    @Override
    public void publish(String topic, byte[] message) throws NSQException {
        if (!started) {
            throw new IllegalStateException("Producer must be started before producing messages!");
        }
        final Connection conn = getConnection();
        assert null != conn;

        try {
            final Pub command = new Pub(topic, message);
            final NSQFrame resp = conn.commandAndGetResponse(command);
            if (null != resp) {
                switch (resp.getType()) {
                    case RESPONSE_FRAME: {
                        // TODO it is OK!
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
        } catch (TimeoutException e) {
            logger.error("Exception", e);
            throw new NSQException("Client Machine Performance Issue! Please check it!");
        } finally {
            // TODO: handle finally clause
            // TODO return back to the pool
        }
    }

    @Override
    public void publishMulti(String topic, List<byte[]> messages) {
        if (!started) {
            throw new IllegalStateException("Producer must be started before producing messages!");
        }
    }

    @Override
    public void incoming(NSQFrame frame, Connection conn) {
    }

    @Override
    public void identify(Connection conn) throws NSQException {
    }

}
