package it.youzan.nsq.client;

import com.youzan.nsq.client.Consumer;
import com.youzan.nsq.client.ConsumerImplV2;
import com.youzan.nsq.client.MessageHandler;
import com.youzan.nsq.client.entity.ConsumeMessageFilterMode;
import com.youzan.nsq.client.entity.DesiredTag;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.utils.TopicUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lin on 17/6/12.
 */
@Test(groups = {"ITTagConsumer"}, dependsOnGroups = {"ITTagProducer"}, priority = 5)
public class ITTagConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ITTagConsumer.class);
    private Properties props = new Properties();

    @BeforeClass
    public void init() throws Exception {
        logger.info("At {} , initialize: {}", System.currentTimeMillis(), this.getClass().getName());
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }
    }

    public void testConsumeOneTagOneNormal() throws Exception {
        String topic = "testExt2Par2Rep";
        Consumer consumer = null;
        Consumer consumerTag = null;
        try {
            final CountDownLatch latch = new CountDownLatch(20);
            final AtomicInteger receivedTag1 = new AtomicInteger(0);
            NSQConfig config = new NSQConfig("BaseConsumer");
            config.setLookupAddresses(props.getProperty("lookup-addresses"));
            config.setConsumerDesiredTag(new DesiredTag("TAG1"));
            consumerTag = new ConsumerImplV2(config, new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    logger.info("Message received: " + message.getReadableContent());
                    logger.info("message tag: " + message.getTag().toString());
                    receivedTag1.incrementAndGet();
                    latch.countDown();
                }
            });
            consumerTag.setAutoFinish(true);
            consumerTag.subscribe(topic);
            consumerTag.start();

            NSQConfig configTag = new NSQConfig("BaseConsumer");
            configTag.setLookupAddresses(props.getProperty("lookup-addresses"));
            consumer = new ConsumerImplV2(configTag, new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    logger.error("Message should not received: " + message.getReadableContent());
                    logger.error("message tag: " + message.getTag().toString());

                }
            });
            consumer.setAutoFinish(true);
            consumer.subscribe(topic);
            consumer.start();

            Assert.assertTrue(latch.await(1, TimeUnit.MINUTES));
            Assert.assertEquals(receivedTag1.get(), 20);
        }finally {
            if(null != consumer)
                consumer.close();
            if(null != consumerTag)
                consumerTag.close();
            TopicUtil.emptyQueue("http://" + props.getProperty("admin-address"), topic, "BaseConsumer");
        }
    }

    public void test() throws Exception {
        String topic = "testExt2Par2Rep";
        try {
            final CountDownLatch latch = new CountDownLatch(20);
            final AtomicInteger receivedTag1 = new AtomicInteger(0);
            final AtomicInteger receivedTag2 = new AtomicInteger(0);
            NSQConfig config = new NSQConfig("BaseConsumer");
            config.setLookupAddresses(props.getProperty("lookup-addresses"));
            config.setConsumerDesiredTag(new DesiredTag("TAG2"));
            Consumer consumer = new ConsumerImplV2(config, new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    logger.info("Message received: " + message.getReadableContent());
                    logger.info("message tag: " + message.getTag().toString());
                    receivedTag1.incrementAndGet();
                    latch.countDown();
                }
            });
            consumer.setAutoFinish(true);
            consumer.subscribe(topic);
            consumer.start();

            NSQConfig configTag = new NSQConfig("BaseConsumer");
            configTag.setLookupAddresses(props.getProperty("lookup-addresses"));
            configTag.setConsumerDesiredTag(new DesiredTag("TAG1"));
            Consumer consumerTag = new ConsumerImplV2(configTag, new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    logger.info("Message received: " + message.getReadableContent());
                    logger.info("message tag: " + message.getTag().toString());
                    receivedTag2.incrementAndGet();
                    latch.countDown();
                }
            });
            consumerTag.setAutoFinish(true);
            consumerTag.subscribe(topic);
            consumerTag.start();

            Assert.assertTrue(latch.await(1, TimeUnit.MINUTES));
            Assert.assertEquals(receivedTag1.get(), 10);
            Assert.assertEquals(receivedTag2.get(), 10);

            consumer.close();
            consumerTag.close();
        }finally {
            TopicUtil.emptyQueue("http://" + props.getProperty("admin-address"), topic, "BaseConsumer");
        }
    }

    public void testConsumeTagMix() throws Exception {
        String topic = "testExt2Par2Rep";
        Consumer consumer = null;
        try {
            final CountDownLatch latch = new CountDownLatch(20);
            final AtomicInteger received = new AtomicInteger(0);
            final AtomicInteger receivedTag = new AtomicInteger(0);
            NSQConfig config = new NSQConfig("BaseConsumer");
            config.setLookupAddresses(props.getProperty("lookup-addresses"));
            consumer = new ConsumerImplV2(config, new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    logger.info("Message received: " + message.getReadableContent());
                    logger.info("message tag: " + message.getTag());
                    if (null == message.getTag())
                        received.incrementAndGet();
                    else
                        receivedTag.incrementAndGet();
                    latch.countDown();
                }
            });
            consumer.setAutoFinish(true);
            consumer.subscribe(topic);
            consumer.start();

            Assert.assertTrue(latch.await(1, TimeUnit.MINUTES));
            Assert.assertEquals(received.get(), 10);
            Assert.assertEquals(receivedTag.get(), 10);
        }finally {
            if(null != consumer)
                consumer.close();
            TopicUtil.emptyQueue("http://" + props.getProperty("admin-address"), topic, "BaseConsumer");
        }
    }

    public void testConsumeTagMixWHeader() throws Exception {
        String topic = "testExt2Par2Rep";
        Consumer consumer = null;
        try {
            final AtomicBoolean fail = new AtomicBoolean(false);
            final CountDownLatch latch = new CountDownLatch(20);
            final AtomicInteger received = new AtomicInteger(0);
            final AtomicInteger receivedTag = new AtomicInteger(0);
            NSQConfig config = new NSQConfig("BaseConsumer");
            config.setLookupAddresses(props.getProperty("lookup-addresses"));
            consumer = new ConsumerImplV2(config, new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    logger.info("Message received: " + message.getReadableContent());
                    logger.info("message tag: " + message.getTag());
                    if (!message.getExtByName("key").equals("javaSDK") ||
                            !message.getExtByName("key1").equals("this is value 1") ||
                            !message.getExtByName("key2").equals("this is value 2") ||
                            !message.getExtByName("key3").equals("this is value 3") ||
                            !message.getExtByName("key4").equals("this is value 4"))
                        fail.set(true);

                    if (null == message.getTag())
                        received.incrementAndGet();
                    else
                        receivedTag.incrementAndGet();
                    latch.countDown();
                }
            });
            consumer.setAutoFinish(true);
            consumer.subscribe(topic);
            consumer.start();

            Assert.assertTrue(latch.await(1, TimeUnit.MINUTES));
            Assert.assertFalse(fail.get());
            Assert.assertEquals(received.get(), 10);
            Assert.assertEquals(receivedTag.get(), 10);

        }finally {
            if(null != consumer)
                consumer.close();
            TopicUtil.emptyQueue("http://" + props.getProperty("admin-address"), topic, "BaseConsumer");
        }
    }

    public void testConsumerWithHeaderFilter() throws Exception {
        String topic = "JavaTesting-Ext";
        Consumer consumer = null;
        try{
            logger.info("[testConsumerWithHeaderFilter] starts");
            NSQConfig config = new NSQConfig("BaseConsumer");
            config.setLookupAddresses(props.getProperty("lookup-addresses"));
            config.setConsumeMessageFilter("filter_key1", "filter_val1");
            config.setConsumeMessageFilterMode(ConsumeMessageFilterMode.EXACT_MATCH);
            final AtomicBoolean fail = new AtomicBoolean(false);
            final CountDownLatch latch = new CountDownLatch(5);
            consumer = new ConsumerImplV2(config, new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    if (null == message.getExtByName("filter_key1") || !message.getExtByName("filter_key1").equals("filter_val1"))
                        fail.set(true);
                    latch.countDown();
                }
            });
            consumer.setAutoFinish(true);
            consumer.subscribe(topic);
            consumer.start();
            latch.await(1, TimeUnit.MINUTES);
            Assert.assertFalse(fail.get());
        }finally {
            consumer.close();
            TopicUtil.emptyQueue("http://" + props.getProperty("admin-address"), topic, "BaseConsumer");
            logger.info("[testConsumerWithHeaderFilter] ends");
        }
    }
}
