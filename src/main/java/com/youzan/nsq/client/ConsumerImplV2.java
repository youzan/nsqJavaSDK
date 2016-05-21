package com.youzan.nsq.client;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.core.ConsumerWorker;
import com.youzan.nsq.client.core.MessageHandler;
import com.youzan.nsq.client.core.lookup.NSQLookupService;
import com.youzan.nsq.client.core.lookup.NSQLookupServiceImpl;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.util.IOUtil;

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

    private final NSQConfig config;

    private volatile NSQLookupService migratingLookup = null;
    private final NSQLookupService lookup;

    private volatile List<ConsumerWorker> workers;

    /**
     * @param config
     * @param handler
     */
    public ConsumerImplV2(NSQConfig config, MessageHandler handler) {
        this.config = config;
        this.lookup = new NSQLookupServiceImpl(config.getLookupAddresses());
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
        for (ConsumerWorker w : workers) {
            IOUtil.closeQuietly(w);
        }
    }

}
