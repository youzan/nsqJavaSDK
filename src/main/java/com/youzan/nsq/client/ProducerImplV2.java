package com.youzan.nsq.client;

import java.util.List;

import com.youzan.nsq.client.core.lookup.NSQLookupService;
import com.youzan.nsq.client.core.lookup.NSQLookupServiceImpl;
import com.youzan.nsq.client.entity.NSQConfig;

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

    /**
     * 
     * @param config
     */
    public ProducerImplV2(NSQConfig config) {
        this.config = config;
        this.lookup = new NSQLookupServiceImpl(config.getLookupAddresses());
    }

    @Override
    public Producer start() {
        started = true;
        return this;
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
    public void publish(String topic, byte[] message) {
        if (!started) {
            throw new IllegalStateException("Producer must be started before producing messages!");
        }
    }

    @Override
    public void publishMulti(String topic, List<byte[]> messages) {
        if (!started) {
            throw new IllegalStateException("Producer must be started before producing messages!");
        }
    }

}
