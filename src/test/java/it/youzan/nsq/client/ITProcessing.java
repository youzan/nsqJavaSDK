package it.youzan.nsq.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.youzan.nsq.client.Consumer;
import com.youzan.nsq.client.ConsumerImplV2;
import com.youzan.nsq.client.MessageHandler;
import com.youzan.nsq.client.Producer;
import com.youzan.nsq.client.ProducerImplV2;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.exception.NSQException;

public class ITProcessing {

    private static final Logger logger = LoggerFactory.getLogger(ITProcessing.class);

    private final Random random = new Random();

    @Test
    public void processBinary() throws NSQException, IOException, InterruptedException {
        final NSQConfig config = new NSQConfig();
        final Properties props = new Properties();
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }
        final String env = props.getProperty("env");
        final String consumeName = env + "-" + this.getClass().getName();

        config.setLookupAddresses(props.getProperty("lookup-addresses"));
        config.setTopic(props.getProperty("topic-prefix") + "_BinaryType_BeEmpty");
        config.setConsumerName(consumeName);
        config.setConnectTimeoutInMillisecond(Integer.valueOf(props.getProperty("connectTimeoutInMillisecond")));
        config.setTimeoutInSecond(Integer.valueOf(props.getProperty("timeoutInSecond")));
        config.setMsgTimeoutInMillisecond(Integer.valueOf(props.getProperty("msgTimeoutInMillisecond")));
        config.setThreadPoolSize4IO(Integer.valueOf(props.getProperty("threadPoolSize4IO")));
        props.clear();

        final int size = 1;
        final CountDownLatch latch = new CountDownLatch(size);
        final List<NSQMessage> collector = new ArrayList<>();
        final AtomicInteger index = new AtomicInteger(0);
        final MessageHandler handler = new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                collector.add(message);
                latch.countDown();
            }
        };
        final Consumer consumer = new ConsumerImplV2(config, handler);
        consumer.start();
        /******************************
         * Produce a byte type
         ******************************/
        final byte[] message = new byte[1024];
        random.nextBytes(message);
        try (Producer p = new ProducerImplV2(config);) {
            p.start();
            p.publish(message);
        } catch (NSQException e) {
            logger.error("Exception", e);
        }
        // Because of the distributed environment and the network, after
        // publishing
        latch.await(2, TimeUnit.MINUTES);
        consumer.close();

        Assert.assertFalse(collector.isEmpty());
        NSQMessage actual = null;
        for (NSQMessage msg : collector) {
            if (msg.getMessageBody().equals(message)) {
                actual = msg;
                break;
            }
        }
        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getReadableAttempts(), 1,
                "The message is not mine. Please check the data in the environment.");
    }

    // Generate some messages
    // Publish to the data-node
    // Get the messsages
    // Validate the messages
    @SuppressWarnings("deprecation")
    @Test
    public void processString() throws NSQException, InterruptedException, IOException {
        final NSQConfig config = new NSQConfig();
        final Properties props = new Properties();
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }
        final String env = props.getProperty("env");
        final String consumeName = env + "-" + this.getClass().getName();

        config.setLookupAddresses(props.getProperty("lookup-addresses"));
        config.setTopic(props.getProperty("topic-prefix") + "_StringType_BeEmpty");
        config.setConsumerName(consumeName);
        config.setConnectTimeoutInMillisecond(Integer.valueOf(props.getProperty("connectTimeoutInMillisecond")));
        config.setTimeoutInSecond(Integer.valueOf(props.getProperty("timeoutInSecond")));
        config.setMsgTimeoutInMillisecond(Integer.valueOf(props.getProperty("msgTimeoutInMillisecond")));
        config.setThreadPoolSize4IO(Integer.valueOf(props.getProperty("threadPoolSize4IO")));
        props.clear();

        final int size = 1;
        final CountDownLatch latch = new CountDownLatch(size);
        final List<NSQMessage> collector = new ArrayList<>();
        final AtomicInteger index = new AtomicInteger(0);
        final MessageHandler handler = new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                collector.add(message);
                latch.countDown();
            }
        };
        final Consumer consumer = new ConsumerImplV2(config, handler);
        consumer.start();
        /******************************
         * Produce a string type
         ******************************/
        final String message = MessageUtil.randomString();
        try (Producer p = new ProducerImplV2(config)) {
            p.start();
            p.publish(message);
        } catch (NSQException e) {
            logger.error("Exception", e);
        }
        // Because of the distributed environment and the network, after
        // publishing
        latch.await(2, TimeUnit.MINUTES);
        consumer.close();

        Assert.assertFalse(collector.isEmpty());
        NSQMessage actual = null;
        for (NSQMessage msg : collector) {
            if (msg.getMessageBody().equals(message)) {
                actual = msg;
                break;
            }
        }
        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getReadableAttempts(), 1,
                "The message is not mine. Please check the data in the environment.");
    }
}
