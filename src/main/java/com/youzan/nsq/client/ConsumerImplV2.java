package com.youzan.nsq.client;

import java.io.IOException;
import java.util.List;

import com.youzan.nsq.client.core.MessageHandler;
import com.youzan.nsq.client.entity.NSQConfig;

public class ConsumerImplV2 implements Consumer {

    /**
     * 
     * @param config
     * @param handler
     */
    public ConsumerImplV2(NSQConfig config, MessageHandler handler) {
        // TODO - implement ConsumerImplV2.Consumer
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
    public void close() throws IOException {
    }

}
