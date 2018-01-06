package it.youzan.nsq.client;

import com.youzan.nsq.client.*;
import com.youzan.nsq.client.entity.Message;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by lin on 17/7/29.
 */
public class ITProducerExt {
    private static final Logger logger = LoggerFactory.getLogger(ITProducerExt.class);

    protected final NSQConfig config = new NSQConfig();
    protected Producer producer;
    protected String adminHttp;

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
        adminHttp = "http://" + props.getProperty("admin-http");
        config.setLookupAddresses(lookups);
        config.setConnectTimeoutInMillisecond(Integer.valueOf(connTimeout));
        config.setMsgTimeoutInMillisecond(Integer.valueOf(msgTimeoutInMillisecond));
        config.setThreadPoolSize4IO(Integer.valueOf(threadPoolSize4IO));
        config.turnOnLocalTrace("JavaTesting-Ext");
        producer = new ProducerImplV2(config);
        producer.start();
    }

    @Test
    public void publishExt() throws Exception {
        logger.info("[publishExt] starts.");
        String topicName = "JavaTesting-Ext";
//        TopicUtil.emptyQueue(adminHttp, topicName, "default");
        //set trace id, which is a long(8-byte-length)
        Topic topic = new Topic(topicName);
        Map<String, String> properties = new HashMap<>();
        properties.put("key" ,"javaSDK");
        Consumer consumer = null;
        try {
            for (int i = 0; i < 10; i++) {
                Message msg = Message.create(topic, 45678L, ("Message #" + i));
                //add ext json
                msg.setJsonHeaderExt(properties);
                producer.publish(msg);
            }

            producer.close();
            config.setConsumerName("default");
            final CountDownLatch latch = new CountDownLatch(10);
            consumer = new ConsumerImplV2(config, new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    logger.info(message.getReadableContent());
                    if ("javaSDK".equals(message.getJsonExtHeader().get("key")))
                        latch.countDown();
                }
            });
            consumer.subscribe(topicName);
            consumer.start();
            Assert.assertTrue(latch.await(60, TimeUnit.SECONDS));
        } finally {
            Thread.sleep(1000);
            consumer.close();
            logger.info("[publishExt] ends.");
        }
    }

    @Test
    public void publishExtZanTest() throws NSQException, InterruptedException {
        logger.info("[publishExtZanTest] starts.");
        String topicName = "JavaTesting-Ext";
//        TopicUtil.emptyQueue(adminHttp, topicName, "default");
        //set trace id, which is a long(8-byte-length)
        Topic topic = new Topic(topicName);
        Map<String, Object> properties = new HashMap<>();
        properties.put("zan_test" , Boolean.TRUE);
        Consumer consumer = null;
        try {
            for (int i = 0; i < 10; i++) {
                Message msg = Message.create(topic, 45678L, ("Message #" + i));
                //add ext json
                msg.setJsonHeaderExt(properties);
                producer.publish(msg);
            }

            producer.close();
            config.setConsumerName("default");
            config.setMessageSkipExtensionKey("zan_test");
            final CountDownLatch failLatch = new CountDownLatch(1);
            consumer = new ConsumerImplV2(config, new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    failLatch.countDown();
                }
            });
            consumer.subscribe(topicName);
            consumer.start();
            Assert.assertFalse(failLatch.await(60, TimeUnit.SECONDS));
        } finally {
            Thread.sleep(1000);
            consumer.close();
            logger.info("[publishExtZanTest] ends.");
        }
    }

    @AfterClass
    public void clear() {
        if(null != producer)
            producer.close();
    }
}
