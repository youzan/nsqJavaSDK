package com.youzan.nsq.client;

import java.util.List;

import com.youzan.nsq.client.entity.NSQConfig;

public class ProducerImplV2 implements Producer {

    private final NSQConfig config;

    /**
     * 
     * @param config
     */
    public ProducerImplV2(NSQConfig config) {
        this.config = config;
    }

    @Override
    public Producer start() {
        // TODO - implement ProducerImplV2.start
        throw new UnsupportedOperationException();
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
    public void pub(String topic, byte[] message) {
    }

    @Override
    public void pubMulti(String topic, List<byte[]> messages) {
    }

}
