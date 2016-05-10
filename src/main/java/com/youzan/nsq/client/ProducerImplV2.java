package com.youzan.nsq.client;

import java.io.IOException;

import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;

public class ProducerImplV2 implements Producer {

    /**
     * 
     * @param config
     */
    public ProducerImplV2(NSQConfig config) {
        // TODO - implement ProducerImplV2.Producer
        throw new UnsupportedOperationException();
    }

    @Override
    public Producer start() {
        // TODO - implement ProducerImplV2.start
        throw new UnsupportedOperationException();
    }

    /**
     * 
     * @param msg
     */
    @Override
    public void pub(NSQMessage msg) {
        // TODO - implement ProducerImplV2.pub
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
    }

}
