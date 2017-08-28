package it.youzan.nsq.client;

import com.youzan.nsq.client.*;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.exception.ConfigAccessAgentException;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.utils.TopicUtil;
import com.youzan.util.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public class ITComplexConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ITComplexConsumer.class);

    private final String consumerName = "BaseConsumer";

    private final AtomicInteger counter = new AtomicInteger(0);
    private final Random _r = new Random();
    private final NSQConfig config = new NSQConfig();
    private Producer producer;
    private HashSet<String> messages4Finish = new HashSet<>();
    private String admin;
    private String lookups;


    @BeforeClass
    public void init() throws Exception {
        logger.info("At {} , initialize: {}", System.currentTimeMillis(), this.getClass().getName());
        System.setProperty("nsq.sdk.configFilePath", "src/test/resources/configClientTest.properties");
        final Properties props = new Properties();
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }

        lookups = props.getProperty("lookup-addresses");
        final String connTimeout = props.getProperty("connectTimeoutInMillisecond");
        admin = "http://" + props.getProperty("admin-address");
        config.setLookupAddresses(lookups);
        config.setConnectTimeoutInMillisecond(Integer.valueOf(connTimeout));
        config.setThreadPoolSize4IO(Runtime.getRuntime().availableProcessors() * 2);

        // empty channel
        // curl -X POST http://127.0.0.1:4151/channel/empty?topic=name&channel=name
        // curl -X /api/topics/:topic/:channel    body: {"action":"empty"}
        String[] topics = new String[]{"JavaTesting-Finish", "JavaTesting-ReQueue"};
        for (String t : topics) {
            TopicUtil.emptyQueue(admin, t, consumerName);
        }
        // create new instances
        producer = new ProducerImplV2(config);
        producer.start();
    }

    /**
     * touch message several times and consumer can still finish it
     * @throws Exception
     */
    @Test
    public void testTouch() throws Exception {
        logger.info("[testTouch] starts.");
        String topicName = "JavaTesting-ReQueue";
        TopicUtil.emptyQueue(admin, topicName, consumerName);
        final byte[] message = new byte[32];
        _r.nextBytes(message);
        producer.publish(message, topicName);

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean fail = new AtomicBoolean(false);
        final AtomicBoolean receiveGuard = new AtomicBoolean(false);

        final NSQConfig config = (NSQConfig) this.config.clone();
        config.setRdy(4);
        config.setConsumerName(consumerName);
        config.setThreadPoolSize4IO(Math.max(2, Runtime.getRuntime().availableProcessors()));
        final Consumer consumer4Touch = new ConsumerImplV2(config);
        consumer4Touch.setAutoFinish(false);
        final MessageHandler handler = new MessageHandler() {
            @Override
            public void process(final NSQMessage message) {
                try {
                        if(receiveGuard.compareAndSet(false, true)){
                            Thread.sleep(50000);
                            consumer4Touch.touch(message);
                            logger.info("message touched");
                            Thread.sleep(50000);
                            consumer4Touch.finish(message);
                            logger.info("message finished");
                        } else {
                            //fail it
                            fail.set(true);
                        }
                        latch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (NSQException e) {
                    e.printStackTrace();
                }
            }
        };

        consumer4Touch.subscribe(topicName);
        consumer4Touch.setMessageHandler(handler);
        consumer4Touch.start();

        Assert.assertTrue(latch.await(5, TimeUnit.MINUTES));
        Assert.assertFalse(fail.get());
        consumer4Touch.close();
        TopicUtil.emptyQueue(admin, topicName, consumerName);
        logger.info("[testTouch] ends.");
    }

    @Test
    public void testFinish() throws NSQException, InterruptedException {
        for (int i = 0; i < 10; i++) {
            String message = newMessage();
            producer.publish(message, "JavaTesting-Finish");
            messages4Finish.add(message);
        }
        logger.debug("======messages4Finish:{} ", messages4Finish);
        final int count = 10;
        final CountDownLatch latch = new CountDownLatch(count);
        final HashSet<String> actualMessages = new HashSet<>();
        final List<NSQMessage> actualNSQMessages = new ArrayList<>();
        final MessageHandler handler = new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                logger.debug("======================Be pushed.");
                logger.debug("======================From server {} , {}, Binary: {}", message.getReadableContent(), message.getMessageBody(), message);
                actualMessages.add(message.getReadableContent());
                actualNSQMessages.add(message);
                // finally
                latch.countDown();
            }
        };
        final NSQConfig config = (NSQConfig) this.config.clone();
        config.setMsgTimeoutInMillisecond(5 * 60 * 1000);
        config.setRdy(20);
        config.setConsumerName(consumerName);
        config.setThreadPoolSize4IO(1);
        Consumer consumer4Finish = new ConsumerImplV2(config, handler);
        consumer4Finish.setAutoFinish(false);
        consumer4Finish.subscribe("JavaTesting-Finish");
        consumer4Finish.start();
        logger.debug("=======================start consumer.... topic : JavaTesting-Finish");
        boolean full = latch.await(2 * 60L, TimeUnit.SECONDS);
        final List<NSQMessage> received = new ArrayList<>(actualNSQMessages);
        logger.debug("=======================received: {}", received.size());
        for (NSQMessage m : received) {
            consumer4Finish.finish(m);
            logger.debug("=========================Finish one: {}", m.newHexString(m.getMessageID()));
        }
        logger.debug("======messages4Finish:{} ", messages4Finish);
        if (full) {
            Assert.assertEquals(actualNSQMessages.size(), count);
            Assert.assertEquals(actualMessages, messages4Finish);
        } else {
            Assert.assertTrue(false, "Not have got enough messages.");
        }
        consumer4Finish.close();
    }

    @Test
    public void testReQueue() throws Exception {
        TopicUtil.emptyQueue(admin, "JavaTesting-ReQueue", consumerName);
        for (int i = 0; i < 10; i++) {
            final byte[] message = new byte[32];
            _r.nextBytes(message);
            producer.publish(message, "JavaTesting-ReQueue");
        }

        final CountDownLatch latch = new CountDownLatch(10);
        final MessageHandler handler = new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                latch.countDown();
                try {
                    message.setNextConsumingInSecond(120);
                } catch (NSQException e) {
                    logger.error("Exception", e);
                }
            }
        };
        final NSQConfig config = (NSQConfig) this.config.clone();
        config.setRdy(4);
        config.setConsumerName(consumerName);
        config.setThreadPoolSize4IO(Math.max(2, Runtime.getRuntime().availableProcessors()));
        Consumer consumer4ReQueue = new ConsumerImplV2(config, handler);
        consumer4ReQueue.subscribe("JavaTesting-ReQueue");
        consumer4ReQueue.start();
        latch.await(1, TimeUnit.MINUTES);
        TopicUtil.emptyQueue(admin, "JavaTesting-ReQueue", consumerName);
        consumer4ReQueue.close();
    }

    @Test
    public void testReadabeAttempt() throws Exception {
        logger.info("[testReadableAttempt] starts.");
        final String topic = "JavaTesting-ReQueue";
        TopicUtil.emptyQueue(admin, topic, "BaseConsumer");
        final NSQConfig config = new NSQConfig("BaseConsumer");
        config.setMaxRequeueTimes(1024);
        config.setLookupAddresses(lookups);
        Producer producer = new ProducerImplV2(config);
        producer.start();
        producer.publish("message".getBytes(Charset.defaultCharset()), topic);
        producer.close();

        //consume
        Consumer consumer = null;
        try {
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicInteger cnt = new AtomicInteger(0);
            consumer = new ConsumerImplV2(config, new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    int attempt = message.getReadableAttempts();
                    if (attempt - cnt.get() != 1) {
                        latch.countDown();
                    } else {
                        logger.info("msg attempt: {}", cnt.incrementAndGet());
                        try {
                            message.setNextConsumingInSecond(0);
                        } catch (NSQException e) {
                            logger.error("fail to set message requeue timeout.");
                        }
                        throw new RuntimeException("on purpose");
                    }
                }
            });
            consumer.subscribe(topic);
            consumer.start();
            Assert.assertFalse(latch.await(1, TimeUnit.MINUTES));
        }finally {
            consumer.close();
            TopicUtil.emptyQueue(admin, topic, "BaseConsumer");
            logger.info("[testReadableAttempt] ends.");
        }
    }

    private String newMessage() {
        return "String Message: " + counter.getAndIncrement() + " . At: " + System.currentTimeMillis();
    }

    @AfterClass
    public void close() throws NoSuchMethodException, ConfigAccessAgentException, InvocationTargetException, IllegalAccessException {
        logger.debug("================Begin to close");
        IOUtil.closeQuietly(producer);
        System.clearProperty("nsq.sdk.configFilePath");
    }
}
