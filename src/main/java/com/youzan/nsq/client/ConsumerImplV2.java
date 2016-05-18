package com.youzan.nsq.client;

import java.io.IOException;
import java.util.ArrayList;
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
 * Expose to Client Code
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class ConsumerImplV2 implements Consumer {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerImplV2.class);

    private volatile NSQLookupService migratingLookup = null;
    private final NSQLookupService lookup;

    private final List<ConsumerWorker> workers;

    private final NSQConfig config;

    /**
     * 
     * @param config
     * @param handler
     */
    public ConsumerImplV2(NSQConfig config, MessageHandler handler) {
        this.config = config;

        lookup = new NSQLookupServiceImpl(config.getLookupAddresses());
        // TODO - implement ConsumerImplV2.Consumer
        final int size = 0;
        if (size >= Runtime.getRuntime().availableProcessors() * 5) {
            logger.error("You set too large workers. In recommanded, your tuning should be reasonable.");
        }
        this.workers = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
        }
    }

    @Override
    public Consumer start() {
        // TODO - implement ConsumerImplV2.start
        throw new UnsupportedOperationException();
    }

    /**
     * notify the NSQ-Server that turrning off pushing some messages 
     */
    @Override
    public void backoff() {
        // TODO - implement ConsumerImplV2.backoff
        throw new UnsupportedOperationException();
    }

    /**
     * 
     * @param addresses
     */
    @Override
    public void addLookupCluster(List<String> addresses) {
        // TODO - implement ConsumerImplV2.addLookupCluster
        throw new UnsupportedOperationException();
    }

    @Override
    public NSQConfig getConfig() {
        return this.config;
    }

    @Override
    public void close() throws IOException {
        for (ConsumerWorker w : workers) {
            IOUtil.closeQuietly(w);
        }
    }

}
