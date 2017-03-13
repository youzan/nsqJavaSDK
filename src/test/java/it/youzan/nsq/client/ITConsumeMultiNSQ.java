package it.youzan.nsq.client;

import com.youzan.nsq.client.*;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.exception.NSQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lin on 17/3/13.
 */
public class ITConsumeMultiNSQ {
    private static final Logger logger = LoggerFactory.getLogger(ITConsumeMultiNSQ.class);

    private final int rdy = 10;
    private final NSQConfig config = new NSQConfig();
    private final NSQConfig configProd1 = new NSQConfig();
    private final NSQConfig configProd2 = new NSQConfig();
    private Consumer consumer;

    @BeforeClass
    public void init() throws Exception {
        logger.info("At {} , initialize: {}", System.currentTimeMillis(), this.getClass().getName());
        final Properties props = new Properties();
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }

        final String lookups = props.getProperty("lookup-addresses");
        final String lookupsAnother = props.getProperty("lookup-addresses-dev");
        final String connTimeout = props.getProperty("connectTimeoutInMillisecond");
        final String msgTimeoutInMillisecond = props.getProperty("msgTimeoutInMillisecond");
        final String threadPoolSize4IO = props.getProperty("threadPoolSize4IO");

        config.setLookupAddresses(lookups + "," + lookupsAnother);
        config.setConnectTimeoutInMillisecond(Integer.valueOf(connTimeout));
        config.setMsgTimeoutInMillisecond(Integer.valueOf(msgTimeoutInMillisecond));
        config.setThreadPoolSize4IO(Integer.valueOf(threadPoolSize4IO));
        config.setRdy(rdy);
        config.setConsumerName("BaseConsumer");

        configProd1.setLookupAddresses(lookups);
        configProd2.setLookupAddresses(lookupsAnother);
    }

    @Test
    public void test() throws NSQException, InterruptedException {
        Producer prod1 = null,  prod2 = null;
        try {
            final CountDownLatch latch = new CountDownLatch(2);
            final AtomicInteger received = new AtomicInteger(0);
            consumer = new ConsumerImplV2(config, new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    received.incrementAndGet();
                    if (message.getReadableContent().startsWith("message from one lookup addresses"))
                        latch.countDown();
                }
            });
            consumer.setAutoFinish(true);
            consumer.subscribe("JavaTesting-Producer-Base");
            consumer.start();

            prod1 = new ProducerImplV2(configProd1);
            prod1.start();
            prod1.publish("message from one lookup addresses".getBytes(Charset.defaultCharset()), "JavaTesting-Producer-Base");
            prod1.close();

            prod2 = new ProducerImplV2(configProd2);
            prod2.start();
            prod2.publish("message from one lookup addresses another".getBytes(Charset.defaultCharset()), "JavaTesting-Producer-Base");
            prod2.close();

            Assert.assertTrue(latch.await(1, TimeUnit.MINUTES));
            logger.info("Consumer received {} messages.", received.get());
        }finally {
            consumer.close();
            if(null != prod1)
                prod1.close();
            if(null != prod2)
                prod2.close();
        }
    }
}
