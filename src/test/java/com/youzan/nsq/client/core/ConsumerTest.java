package com.youzan.nsq.client.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.youzan.nsq.client.ConsumerImplV2;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;

public class ConsumerTest {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerTest.class);
    // private static final String lookup = "10.9.80.209:4161";
    private static final String lookup = "127.0.0.1:4161";

    @Test
    public void consumeOK() throws NSQException {
        final NSQConfig config = new NSQConfig();
        config.setLookupAddresses(lookup);
        config.setThreadPoolSize4IO(1);
        config.setTimeoutInSecond(60);
        config.setMsgTimeoutInMillisecond(120 * 1000);
        config.setTopic("test");
        config.setConsumerName("consumer_is_zhaoxi");
        final ConsumerImplV2 consumer = new ConsumerImplV2(config, (message) -> {
            Assert.assertNotNull(message);
        });
        consumer.start();
        try {
            Thread.sleep(3600 * 1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        consumer.close();
    }

    // @Test
    public void consumeAndRequeue() throws NSQException {
        final NSQConfig config = new NSQConfig();
        config.setLookupAddresses(lookup);
        config.setThreadPoolSize4IO(1);
        config.setTimeoutInSecond(120);
        config.setMsgTimeoutInMillisecond(120 * 1000);
        config.setTopic("test");
        config.setConsumerName("consumer_is_zhaoxi");
        final ConsumerImplV2 consumer = new ConsumerImplV2(config, (message) -> {
            Assert.assertNotNull(message);
            try {
                message.setNextConsumingInSecond(null);
            } catch (NSQException e) {
                logger.error("Exception", e);
            }
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
