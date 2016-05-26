package com.youzan.nsq.client.core;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.youzan.nsq.client.ConsumerImplV2;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;

public class ConsumerTest {

    @Test
    public void consumeAndReQueue() throws NSQException {
        final NSQConfig config = new NSQConfig();
        config.setLookupAddresses("127.0.0.1:4161");
        config.setThreadPoolSize4IO(1);
        config.setMsgTimeoutInMillisecond(60 * 1000);
        config.setConsumerName("consumer_is_zhaoxi");
        final ConsumerImplV2 consumer = new ConsumerImplV2(config, (message) -> {
            Assert.assertNotNull(message);
            return false;
        });
        consumer.start();
        consumer.close();
    }

}
