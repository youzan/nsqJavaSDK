package com.youzan.nsq.client.core;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.youzan.nsq.client.ConsumerImplV2;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;

public class ConsumerTest {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerTest.class);
    private static final String lookup = "10.9.80.209:4161";
    // private static final String lookup = "127.0.0.1:4161";

    @Test
    public void consumeOK() throws NSQException {
        final NSQConfig config = new NSQConfig();
        config.setLookupAddresses(lookup);
        config.setTimeoutInSecond(2);
        config.setThreadPoolSize4IO(2);
        config.setMsgTimeoutInMillisecond(120 * 1000);
        config.setTopic("test");
        config.setConsumerName("consumer_is_zhaoxi");

        final Random r = new Random(100);
        final ConsumerImplV2 consumer = new ConsumerImplV2(config, (message) -> {
            Assert.assertNotNull(message);
            sleep(10);
            // if (r.nextInt(100) % 10 == 0) {
            // try {
            // message.setNextConsumingInSecond(30);
            // } catch (Exception e) {
            // logger.error("Exception", e);
            // }
            // }
        });
        sleep(3600 * 2 * 1000);
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
        sleep(3600 * 2 * 1000);
        consumer.close();
    }

    /**
     * @param millisecond
     */
    private void sleep(final int millisecond) {
        try {
            Thread.sleep(millisecond);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("System is too busy! Please check it!", e);
        }
    }

}
