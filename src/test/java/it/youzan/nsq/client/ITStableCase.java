package it.youzan.nsq.client;

import com.youzan.nsq.client.*;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.util.IOUtil;
import com.youzan.util.SystemUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public class ITStableCase {
    private static final Logger logger = LoggerFactory.getLogger(ITStableCase.class);

    private final String consumerName = "BaseConsumer";

    private boolean stable;
    private long allowedRunDeadline = 0;
    private final Random _r = new Random();
    private final BlockingQueue<NSQMessage> store = new LinkedBlockingQueue<>(1000);

    private AtomicInteger successPub = new AtomicInteger(0);
    private AtomicInteger totalPub = new AtomicInteger(0);

    private AtomicInteger received = new AtomicInteger(0);
    private AtomicInteger successFinish = new AtomicInteger(0);


    private final NSQConfig config = new NSQConfig();
    private Producer producer;
    private Consumer consumer;

    @BeforeClass
    public void init() throws Exception {
        logger.info("At {} , initialize: {}", System.currentTimeMillis(), this.getClass().getName());

        final String stableProp = System.getProperty("stable", "false");
        stable = Boolean.valueOf(stableProp);
        if (!stable) {
            return;
        }
        final String hoursProp = System.getProperty("hours", "4");
        allowedRunDeadline = TimeUnit.HOURS.toMillis(Long.valueOf(hoursProp)) + System.currentTimeMillis();
        logger.info("Now {} , allowedRunDeadline {} . Got {} hours", System.currentTimeMillis(), allowedRunDeadline, hoursProp);


        final Properties props = new Properties();
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }

        final String lookups = props.getProperty("lookup-addresses");
        final String connTimeout = props.getProperty("connectTimeoutInMillisecond");
        config.setLookupAddresses(lookups);
        config.setConnectTimeoutInMillisecond(Integer.valueOf(connTimeout));
        config.setThreadPoolSize4IO(1);
    }

    @Test(priority = 12)
    public void produce() throws NSQException {
        if (!stable) {
            return;
        }
        final NSQConfig config = (NSQConfig) this.config.clone();
        config.setThreadPoolSize4IO(1);
        producer = new ProducerImplV2(config);
        producer.start();
        for (long now = 0; now < allowedRunDeadline; now = System.currentTimeMillis()) {
            final byte[] message = new byte[512];
            _r.nextBytes(message);
            try {
                totalPub.getAndIncrement();
                producer.publish(message, "JavaTesting-Finish");
                successPub.getAndIncrement();
            } catch (Exception e) {
                logger.error("Exception", e);
            }
        }
    }

    @Test(priority = 12)
    public void consume() throws InterruptedException, NSQException {
        if (!stable) {
            return;
        }
        final MessageHandler handler = new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                received.getAndIncrement();
                store.offer(message);
            }
        };
        final NSQConfig config = (NSQConfig) this.config.clone();
        config.setRdy(4);
        config.setConsumerName(consumerName);
        config.setThreadPoolSize4IO(Math.max(2, Runtime.getRuntime().availableProcessors()));
        consumer = new ConsumerImplV2(config, handler);
        consumer.setAutoFinish(true);
        consumer.subscribe("JavaTesting-Finish");
        consumer.start();

        for (long now = 0; now < allowedRunDeadline; now = System.currentTimeMillis()) {
            try {
                final NSQMessage message = store.poll(2, TimeUnit.SECONDS);
                if (message == null) {
                    continue;
                }
                consumer.finish(message);
                successFinish.getAndIncrement();
            } catch (Exception e) {
                logger.error("Exception", e);
            }
        }
    }


    @AfterClass
    public void close() {
        IOUtil.closeQuietly(consumer, producer);
        logger.info("Done. successPub: {} , totalPub: {} , received: {} , successFinish: {} , now the temporary store in memory has {} messages.", successPub.get(), totalPub.get(), received.get(), successFinish.get(), store.size());
    }

}
