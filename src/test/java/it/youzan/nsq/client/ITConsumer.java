package it.youzan.nsq.client;

import com.youzan.nsq.client.*;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.util.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Test(groups = {"ITConsumer-Base"}, dependsOnGroups = {"ITProducer-Base"}, priority = 5)
public class ITConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ITConsumer.class);

    private final int rdy = 2;
    private final NSQConfig config = new NSQConfig();
    private Consumer consumer;

    @BeforeClass
    public void init() throws Exception {
        logger.info("At {} , initialize: {}", System.currentTimeMillis(), this.getClass().getName());
        final Properties props = new Properties();
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }

        final String lookups = props.getProperty("lookup-addresses");
        final String connTimeout = props.getProperty("connectTimeoutInMillisecond");
        final String msgTimeoutInMillisecond = props.getProperty("msgTimeoutInMillisecond");
        final String threadPoolSize4IO = props.getProperty("threadPoolSize4IO");

        config.setLookupAddresses(lookups);
        config.setConnectTimeoutInMillisecond(Integer.valueOf(connTimeout));
        config.setMsgTimeoutInMillisecond(Integer.valueOf(msgTimeoutInMillisecond));
        config.setThreadPoolSize4IO(Integer.valueOf(threadPoolSize4IO));
        config.setRdy(rdy);
        config.setConsumerName("BaseConsumer");
    }

    public void test() throws NSQException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(10);
        final AtomicInteger received = new AtomicInteger(0);
        consumer = new ConsumerImplV2(config, new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                received.incrementAndGet();
                latch.countDown();
            }
        });
        consumer.setAutoFinish(true);
        consumer.subscribe("JavaTesting-Producer-Base");
        consumer.start();
        latch.await(1, TimeUnit.MINUTES);
        logger.info("Consumer received {} messages.", received.get());
    }

    @AfterClass
    public void close() {
        logger.info("Consumer closed.");
        IOUtil.closeQuietly(consumer);
    }
}
