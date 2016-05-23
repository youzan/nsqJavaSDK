package com.youzan.nsq.client;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.core.Client;
import com.youzan.nsq.client.core.Connection;
import com.youzan.nsq.client.core.MessageHandler;
import com.youzan.nsq.client.core.NSQSimpleClient;
import com.youzan.nsq.client.core.lookup.NSQLookupService;
import com.youzan.nsq.client.core.lookup.NSQLookupServiceImpl;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.network.frame.NSQFrame;

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
    private final Client simpleClient;

    private volatile NSQLookupService migratingLookup = null;
    private final NSQLookupService lookup;

    private final NSQConfig config;
    private GenericKeyedObjectPoolConfig poolConfig = null;
    private GenericKeyedObjectPool<Address, Connection> pool = null;

    /**
     * @param config
     * @param handler
     */
    public ConsumerImplV2(NSQConfig config, MessageHandler handler) {
        this.config = config;
        this.lookup = new NSQLookupServiceImpl(config.getLookupAddresses());
        this.simpleClient = new NSQSimpleClient(this.config);
    }

    @Override
    public Consumer start() {
        // TODO
        return this;
    }

    private void connect() {
    }

    @Override
    public NSQConfig getConfig() {
        return this.config;
    }

    @Override
    public void close() {
    }

    @Override
    public void incoming(NSQFrame frame, Connection conn) {
    }

    @Override
    public void identify(Connection conn) throws NSQException {
        simpleClient.identify(conn);
    }

}
