package com.youzan.nsq.client;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

import com.youzan.nsq.client.core.Connection;
import com.youzan.nsq.client.core.command.Pub;
import com.youzan.nsq.client.core.lookup.NSQLookupService;
import com.youzan.nsq.client.core.lookup.NSQLookupServiceImpl;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.exception.NoConnectionException;
import com.youzan.nsq.client.network.frame.NSQFrame;

/**
 * Use {@code NSQConfig} to set the lookup cluster.
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class ProducerImplV2 implements Producer {

    private volatile boolean started = false;
    private final NSQConfig config;

    private volatile NSQLookupService migratingLookup = null;
    private final NSQLookupService lookup;

    private ExecutorService executor = Executors.newCachedThreadPool();
    private GenericKeyedObjectPoolConfig poolConfig = null;
    private GenericKeyedObjectPool<Address, Connection> pool = null;

    /**
     * @param config
     */
    public ProducerImplV2(NSQConfig config) {
        this.config = config;
        this.lookup = new NSQLookupServiceImpl(config.getLookupAddresses());
    }

    @Override
    public Producer start() {
        if (!started) {
            started = true;
            createPools();
        }
        return this;
    }

    /**
     * Create some pools. <br />
     * One pool to one broker.
     */
    private void createPools() {
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
    }

    @Override
    public NSQConfig getConfig() {
        return this.config;
    }

    @Override
    public void publish(String topic, byte[] message) throws NSQException, TimeoutException {
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

}
