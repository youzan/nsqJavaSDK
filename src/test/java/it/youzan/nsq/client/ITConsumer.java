package it.youzan.nsq.client;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.youzan.nsq.client.Consumer;
import com.youzan.nsq.client.ConsumerImplV2;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;

public class ITConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ITConsumer.class);

    // Integration Testing
    private static final String lookup = "10.9.80.209:4161";
    // private static final String lookup = "127.0.0.1:4161";

    @Test
    public void consumeOK() throws NSQException {
        final NSQConfig config = new NSQConfig();
        config.setLookupAddresses(lookup);
        config.setConnectTimeoutInMillisecond(100);
        config.setTimeoutInSecond(3);
        config.setThreadPoolSize4IO(2);
        config.setMsgTimeoutInMillisecond(120 * 1000);
        config.setTopic("test");
        config.setConsumerName("consumer_is_zhaoxi");

        final Random r = new Random(100);
        final AtomicLong sucess = new AtomicLong(0L), total = new AtomicLong(0L);
        final long end = (int) System.currentTimeMillis() + 1 * 3600 * 1000L;
        final Consumer consumer = new ConsumerImplV2(config, (message) -> {
            Assert.assertNotNull(message);
            total.incrementAndGet();
            sucess.incrementAndGet();
        });
        consumer.start();
        sleep((3600 + 1200) * 1000L);
        consumer.close();
        logger.info("Total: {}", total);
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
        final Consumer consumer = new ConsumerImplV2(config, (message) -> {
            Assert.assertNotNull(message);
            try {
                // 设置了不合法(经过多久后)下次消费
                message.setNextConsumingInSecond(null);
            } catch (NSQException e) {
                logger.error("Exception", e);
            }
        });
        consumer.start();
        sleep(3600 * 2 * 1000);
        // 一定要在finally里做下优雅的关闭
        consumer.close();
    }

    /**
     * @param millisecond
     */
    private void sleep(final long millisecond) {
        try {
            Thread.sleep(millisecond);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("System is too busy! Please check it!", e);
        }
    }

}
