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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
@Test(priority = 12)
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
        logger.debug("stable: {}", stableProp);
        stable = Boolean.valueOf(stableProp);
        if (!stable) {
            logger.info("Skipped");
            return;
        }
        final String hoursProp = System.getProperty("hours", "4");
        logger.debug("hours: {}", hoursProp);
        allowedRunDeadline = Long.valueOf(hoursProp) * 3600 * 1000L + System.currentTimeMillis();

        final Properties props = new Properties();
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }

        final String lookups = props.getProperty("lookup-addresses");
        final String connTimeout = props.getProperty("connectTimeoutInMillisecond");
        config.setLookupAddresses(lookups);
        config.setConnectTimeoutInMillisecond(Integer.valueOf(connTimeout));
        config.setThreadPoolSize4IO(Runtime.getRuntime().availableProcessors() * 2);

        // create new instances
        producer = new ProducerImplV2(config);
        producer.start();
        sleep(60 * 1000);

    }

    public void produceFinish() throws NSQException {
        if (producer == null) {
            return;
        }
        if (!stable) {
            logger.info("Skipped");
            return;
        }
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

    public void testFinish() throws InterruptedException, NSQException {
        if (producer == null) {
            return;
        }
        if (!stable) {
            logger.info("Skipped");
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
        config.setRdy(2);
        config.setConsumerName(consumerName);
        config.setThreadPoolSize4IO(Math.max(2, Runtime.getRuntime().availableProcessors()));
        consumer = new ConsumerImplV2(config, handler);
        consumer.setAutoFinish(false);
        consumer.subscribe("JavaTesting-Finish");
        consumer.start();

        for (long now = 0; now < allowedRunDeadline; now = System.currentTimeMillis()) {
            try {
                final NSQMessage message = store.poll();
                consumer.finish(message);
                successFinish.getAndIncrement();
            } catch (Exception e) {
                logger.error("Exception", e);
            }
        }
    }

    private void sleep(final long millisecond) {
        logger.debug("Sleep {} millisecond.", millisecond);
        try {
            Thread.sleep(millisecond);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Your machine is too busy! Please check it!");
        }
    }

    @AfterClass
    public void close() {
        IOUtil.closeQuietly(producer, consumer);
        logger.info("Done. successPub: {} , totalPub: {} , received: {} , successFinish: {} , now the temporary store in memory has {} messages.", successPub.get(), totalPub.get(), received.get(), successFinish.get(), store.size());
    }

}
