package com.youzan.nsq.client.core;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.youzan.nsq.client.ConsumerImplV2;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;

public class ConsumerTest {

    @Test
    public void consume() throws NSQException {
        final NSQConfig config = new NSQConfig();
        config.setLookupAddresses("127.0.0.1:4161");
        config.setThreadPoolSize4IO(1);
        config.setTimeoutInSecond(120);
        config.setMsgTimeoutInMillisecond(120 * 1000);
        config.setTopic("test");
        config.setConsumerName("consumer_is_zhaoxi");
        final ConsumerImplV2 consumer = new ConsumerImplV2(config, (message) -> {
            Assert.assertNotNull(message);
            return true;
        });
        consumer.start();
        try {
            Thread.sleep(3600 * 1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        consumer.close();
    }

}
