package com.youzan.nsq.client.core;

import org.testng.annotations.Test;

import com.youzan.nsq.client.ConsumerImplV2;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;

public class ConsumerTest {

    @Test
    public void newConsumer() throws NSQException {
        NSQConfig config = new NSQConfig();
        config.setLookupAddresses("127.0.0.1:4161");
        ConsumerImplV2 consumer = new ConsumerImplV2(config, (message) -> {
            return true;
        });
        consumer.start();
        consumer.close();
    }

    @Test
    public void consume() {
    }
}
