package it.youzan.nsq.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.youzan.nsq.client.Consumer;
import com.youzan.nsq.client.ConsumerImplV2;
import com.youzan.nsq.client.MessageHandler;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.exception.NSQException;

public class ITConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ITConsumer.class);

    private NSQConfig config;

    @BeforeClass
    public void init() throws NSQException, IOException {
        logger.info("Now init {} at {} .", this.getClass().getName(), System.currentTimeMillis());
        config = new NSQConfig();
        final Properties props = new Properties();
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }
        final String env = props.getProperty("env");
        final String consumeName = env + "-" + this.getClass().getName();

        config.setLookupAddresses(props.getProperty("lookup-addresses"));
        config.setTopic("test");
        config.setConsumerName(consumeName);
        config.setConnectTimeoutInMillisecond(Integer.valueOf(props.getProperty("connectTimeoutInMillisecond")));
        config.setTimeoutInSecond(Integer.valueOf(props.getProperty("timeoutInSecond")));
        config.setMsgTimeoutInMillisecond(Integer.valueOf(props.getProperty("msgTimeoutInMillisecond")));
        config.setThreadPoolSize4IO(2);
        props.clear();
        sleep(10);
    }

    @Test(dependsOnGroups = { "ITProducer" })
    public void consume() throws NSQException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger total = new AtomicInteger(0);
        final MessageHandler handler = new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                latch.countDown();
                total.incrementAndGet();
            }
        };
        try (final Consumer consumer = new ConsumerImplV2(config, handler);) {
            consumer.start();
            latch.await(1, TimeUnit.MINUTES);
        } finally {
            logger.info("It has {} messages received.", total.get());
        }
    }

    @Test(dependsOnGroups = { "ITProducer" })
    public void finish() throws NSQException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger total = new AtomicInteger(0);
        final List<NSQMessage> collector = new ArrayList<>();
        final MessageHandler handler = new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                latch.countDown();
                total.incrementAndGet();
                collector.add(message);
            }
        };
        NSQConfig c = (NSQConfig) config.clone();
        c.setTopic(config.getTopic() + "_finish");
        try (final Consumer consumer = new ConsumerImplV2(c, handler);) {
            consumer.start();
            latch.await(2, TimeUnit.MINUTES);
            Assert.assertFalse(collector.isEmpty());
            consumer.finish(collector.get(0));
            consumer.close();
        } finally {
            logger.info("It has {} messages received.", total.get());
        }
    }

    @Test(dependsOnGroups = { "ITProducer" })
    public void reQueue() throws NSQException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger total = new AtomicInteger(0);
        final List<NSQMessage> collector = new ArrayList<>();
        final MessageHandler handler = new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                collector.add(message);
                latch.countDown();
                total.incrementAndGet();
                try {
                    message.setNextConsumingInSecond(2);
                } catch (NSQException e) {
                    logger.error("Exception", e);
                }
            }
        };
        NSQConfig c = (NSQConfig) config.clone();
        c.setTopic(config.getTopic() + "_reQueue");
        try (final Consumer consumer = new ConsumerImplV2(c, handler);) {
            consumer.start();
            latch.await(2, TimeUnit.MINUTES);
            Assert.assertFalse(collector.isEmpty());
            consumer.close();
        } finally {
            logger.info("It has {} messages received.", total.get());
        }
    }

    void sleep(final long millisecond) {
        logger.debug("Sleep {} millisecond.", millisecond);
        try {
            Thread.sleep(millisecond);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Your machine is too busy! Please check it!");
        }
    }
}
