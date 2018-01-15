package com.youzan.nsq.client;

import com.youzan.nsq.client.core.ConnectionManager;
import com.youzan.nsq.client.core.NSQConnection;
import com.youzan.nsq.client.entity.*;
import com.youzan.nsq.client.exception.ExplicitRequeueException;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.utils.TopicUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by lin on 17/3/23.
 */
public class ConsumerTest extends AbstractNSQClientTestcase {

    private final static Logger logger = LoggerFactory.getLogger(ConsumerTest.class);

//    public void testHowBadCanItBeStable() throws Exception {
//        logger.info("[testHowBadCanItBe] starts.");
//        final String topic = "testHowBadCanItBe";//_" + System.currentTimeMillis();
//        int expectRdy = 3;
//        logger.info("ExpectedRdy: {}", expectRdy);
//        final NSQConfig config = new NSQConfig("BaseConsumer");
//        final Random ran1 = new Random();
//        final Random ran2 = new Random();
//        final Random ran3 = new Random();
//        config.setLookupAddresses(props.getProperty("dcc-lookup"));
//        config.setRdy(expectRdy);
//        ScheduledExecutorService exec = null;
//        Producer producer = null;
//        Consumer consumer1 = null;
//        Consumer consumer2 = null;
//        Consumer consumer3 = null;
//        String adminHttp = "http://" + props.getProperty("admin-address");
//        try {
////            TopicUtil.createTopic(adminHttp, topic, 5, 1, "default");
////            TopicUtil.createTopicChannel(adminHttp, topic, "default");
//
//            producer = new ProducerImplV2(config);
//            producer.start();
//            exec = keepMessagePublish(producer, topic,10);
//            final AtomicInteger cnt1 = new AtomicInteger(0);
//            MessageHandler handler1 = new MessageHandler() {
//                @Override
//                public void process(NSQMessage message) {
//                    try {
//                        if (ran1.nextBoolean()) {
//                            Thread.sleep(100);
//                        } else {
//                            if (cnt1.incrementAndGet() == 100) {
//                                cnt1.set(0);
//                                throw new RuntimeException("exp on purpose");
//                            }
//                        }
//                    } catch (InterruptedException e) {
//                        logger.error("Interrupted while sleep");
//                    }
//                }
//            };
//
//            final AtomicInteger cnt2 = new AtomicInteger(0);
//            MessageHandler handler2 = new MessageHandler() {
//                @Override
//                public void process(NSQMessage message) {
//                    try {
//                        if (ran2.nextBoolean()) {
//                            Thread.sleep(100);
//                        } else {
//                            if (cnt2.incrementAndGet() == 100) {
//                                cnt2.set(0);
//                                throw new RuntimeException("exp on purpose");
//                            }
//                        }
//                    } catch (InterruptedException e) {
//                        logger.error("Interrupted while sleep");
//                    }
//                }
//            };
//
//            final AtomicInteger cnt3 = new AtomicInteger(0);
//            MessageHandler handler3 = new MessageHandler() {
//                @Override
//                public void process(NSQMessage message) {
//                    try {
//                        if (ran3.nextBoolean()) {
//                            Thread.sleep(100);
//                        } else {
//                            if (cnt3.incrementAndGet() == 100) {
//                                cnt3.set(0);
//                                throw new RuntimeException("exp on purpose");
//                            }
//                        }
//                    } catch (InterruptedException e) {
//                        logger.error("Interrupted while sleep");
//                    }
//                }
//            };
//
//            consumer1 = new ConsumerImplV2(config, handler1);
//            consumer1.subscribe(topic);
//            consumer1.start();
//
//            consumer2 = new ConsumerImplV2(config, handler2);
//            consumer2.subscribe(topic);
//            consumer2.start();
//
//            consumer3 = new ConsumerImplV2(config, handler3);
//            consumer3.subscribe(topic);
//            consumer3.start();
//            CountDownLatch latch = new CountDownLatch(1);
//            logger.info("Wait for 24h for consumers to play...");
//            latch.await(24, TimeUnit.HOURS);
//        }finally {
//            exec.shutdownNow();
//            Thread.sleep(1000L);
//            producer.close();
//            consumer1.close();
//            consumer2.close();
//            consumer3.close();
////            TopicUtil.deleteTopic(adminHttp, topic);
//            logger.info("[testHowBadCanItBe] ends.");
//        }
//    }

    @Test
    public void testNextConsumingTimeout() throws Exception {
        logger.info("[testNextConsumingTimeout] starts.");
        final NSQConfig config = new NSQConfig("BaseConsumer");
        config.setLookupAddresses(props.getProperty("lookup-addresses"));
        String adminHttp = "http://" + props.getProperty("admin-address");
        String topicName = "testNextConsumingTimeout";

        final int requeueTimeout = 10;
        Consumer consumer = null;
        try {
            TopicUtil.createTopic(adminHttp, topicName, "BaseConsumer");
            TopicUtil.createTopicChannel(adminHttp, topicName, "BaseConsumer");

            Producer producer = new ProducerImplV2(config);
            producer.start();
            producer.publish(Message.create(new Topic(topicName), "msg1"));
            producer.close();
            final int nextTimeoutDefault = config.getNextConsumingInSecond();
            final AtomicInteger cnt = new AtomicInteger(0);
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicLong timestamp = new AtomicLong(0);
            //consumer
            consumer = new ConsumerImplV2(config, new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    int timeout = message.getNextConsumingInSecond();
                    if (0 == cnt.get())
                        Assert.assertEquals(timeout, nextTimeoutDefault);
                    else if (1 == cnt.get()) {
                        long ts = System.currentTimeMillis();
                        long elapse = ts - timestamp.get();
                        logger.info("time elapse between requeue {}", elapse);
                        if (elapse >= requeueTimeout * 1000) {
                            latch.countDown();
                        }
                    } else {
                        return;
                    }
                    try {
                        message.setNextConsumingInSecond(requeueTimeout);
                    } catch (NSQException e) {
                        logger.error("Fail to update next consuming timeout.");
                    }
                    if (0 == cnt.get()) {
                        cnt.incrementAndGet();
                        timestamp.set(System.currentTimeMillis());
                        throw new RuntimeException("on purpose exception");
                    }
                }
            });

            consumer.subscribe(new Topic(topicName));
            consumer.start();
            Assert.assertTrue(latch.await(2, TimeUnit.MINUTES));
        }finally {
            consumer.close();
            logger.info("[testNextConsumingTimeout] ends.");
            TopicUtil.deleteTopic(adminHttp, topicName);
        }
    }

    @Test
    public void testRdyIncrease() throws Exception {
        logger.info("[testRdyIncrease] starts.");
        final String topicName = "testRdyIncrease";
        Random ran = new Random();
        int expectRdy = ran.nextInt(6) + 5;
        logger.info("ExpectedRdy: {}", expectRdy);
        final NSQConfig config = new NSQConfig("BaseConsumer");
        config.setLookupAddresses(props.getProperty("lookup-addresses"));
        config.setRdy(expectRdy);
        ScheduledExecutorService exec = null;
        Producer producer = null;
        Consumer consumer = null;
        String adminHttp = "http://" + props.getProperty("admin-address");
        try {
            TopicUtil.createTopic(adminHttp, topicName, 5, 1, "BaseConsumer");
            TopicUtil.createTopicChannel(adminHttp, topicName, "BaseConsumer");

            producer = new ProducerImplV2(config);
            producer.start();
            exec = keepMessagePublish(producer, topicName, 1000);

            MessageHandler handler = new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        logger.error("Interrupted while sleep");
                    }
                }
            };

            consumer = new ConsumerImplV2(config, handler);
            consumer.subscribe(topicName);
            consumer.start();
            int timeout = expectRdy * 10;
            logger.info("Sleep {} sec to wait for rdy to increase...", timeout);
            Thread.sleep(timeout * 1000);
            logger.info("Wake up.");

            ConnectionManager conMgr = consumer.getConnectionManager();
            Set<ConnectionManager.NSQConnectionWrapper> connSet = conMgr.getSubscribeConnections(topicName);
            for (ConnectionManager.NSQConnectionWrapper wrapper : connSet) {
                int actualRdy = wrapper.getConn().getCurrentRdyCount();
                Assert.assertEquals(actualRdy, expectRdy, "rdy in connection does not equals to expected rdy.");
            }
        }finally {
            exec.shutdownNow();
            Thread.sleep(10000L);
            producer.close();
            consumer.close();
            TopicUtil.deleteTopic(adminHttp, topicName);
            logger.info("[testRdyIncrease] ends.");
        }
    }

    @Test
    public void testLoadFactor() throws Exception {
        logger.info("[testLoadFactor] starts.");
        final String topic = "testLoadFactor";
        int expectRdy = 10;
        logger.info("ExpectedRdy: {}", expectRdy);
        final NSQConfig config = new NSQConfig("BaseConsumer");
        config.setLookupAddresses(props.getProperty("lookup-addresses"));
        config.setRdy(expectRdy);
        ScheduledExecutorService exec = null;
        Producer producer = null;
        Consumer consumer = null;
        String adminHttp = "http://" + props.getProperty("admin-address");
        try {
            TopicUtil.createTopic(adminHttp, topic, "BaseConsumer");
            TopicUtil.createTopicChannel(adminHttp, topic, "BaseConsumer");

            producer = new ProducerImplV2(config);
            producer.start();
            exec = keepMessagePublish(producer, topic,10);
            final AtomicInteger cnt = new AtomicInteger(0);
            MessageHandler handler = new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    try {
                        Thread.sleep(100 * cnt.incrementAndGet());
                    } catch (InterruptedException e) {
                        logger.error("Interrupted while sleep");
                    }
                }
            };

            consumer = new ConsumerImplV2(config, handler);
            consumer.subscribe(topic);
            consumer.start();
            logger.info("Wait for 20s for consumer to start...");
            Thread.sleep(20000L);
            float loadFactor;
            for(int i = 0; i < 10; i++) {
                Thread.sleep(5000);
                loadFactor = ((ConsumerImplV2)consumer).getLoadFactor();
                logger.info("loadFactor {}", loadFactor);
            }
        }finally {
            exec.shutdownNow();
            Thread.sleep(10000L);
            producer.close();
            consumer.close();
            TopicUtil.deleteTopic(adminHttp, topic);
            logger.info("[testLoadFactor] ends.");
        }
    }

    @Test
    public void testCloseConsumerWhileConsumption() throws Exception {
        logger.info("[testCloseConsumerWhileConsumption] starts.");
        final String topic = "testClsConsumeWhileConsume";
        int expectRdy = 10;
        logger.info("ExpectedRdy: {}", expectRdy);
        final NSQConfig config = new NSQConfig("BaseConsumer");
        config.setLookupAddresses(props.getProperty("lookup-addresses"));
        config.setRdy(expectRdy);
        ScheduledExecutorService exec = null;
        Producer producer = null;
        Consumer consumer = null;
        String adminHttp = "http://" + props.getProperty("admin-address");
        try {
            TopicUtil.createTopic(adminHttp, topic, "BaseConsumer");
            TopicUtil.createTopicChannel(adminHttp, topic, "BaseConsumer");

            producer = new ProducerImplV2(config);
            producer.start();
            exec = keepMessagePublish(producer, topic,10);
            MessageHandler handler = new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        logger.error("Interrupted while sleep");
                    }
                }
            };

            consumer = new ConsumerImplV2(config, handler);
            consumer.subscribe(topic);
            consumer.start();
            logger.info("Wait for 60s for consumer to start...");
            Thread.sleep(60000L);
        }finally {
            exec.shutdownNow();
            Thread.sleep(1000L);
            producer.close();
            consumer.close();
            TopicUtil.deleteTopic(adminHttp, topic);
            logger.info("[testCloseConsumerWhileConsumption] ends.");
        }
    }


    /**
     * Consume with high rdy and exceptions always, rdy should down to 1, then ??
     */
    @Test
    public void testHowBadCanItBe() throws Exception {
        logger.info("[testHowBadCanItBe] starts.");
        final String topic = "testHowBadCanItBeBasic";
        int expectRdy = 100;
        logger.info("ExpectedRdy: {}", expectRdy);
        final NSQConfig config = new NSQConfig("BaseConsumer");
        config.setLookupAddresses(props.getProperty("lookup-addresses"));
        config.setRdy(expectRdy);
        ScheduledExecutorService exec = null;
        Producer producer = null;
        Consumer consumer = null;
        String adminHttp = "http://" + props.getProperty("admin-address");
        try {
            TopicUtil.createTopic(adminHttp, topic, "default");
            TopicUtil.createTopicChannel(adminHttp, topic, "default");

            producer = new ProducerImplV2(config);
            producer.start();
            exec = keepMessagePublish(producer, topic,10);
            MessageHandler handler = new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    try {
                        Thread.sleep(100);
                        throw new RuntimeException("exp on purpose");
                    } catch (InterruptedException e) {
                        logger.error("Interrupted while sleep");
                    }
                }
            };

            consumer = new ConsumerImplV2(config, handler);
            consumer.subscribe(topic);
            consumer.start();
            logger.info("Wait for 30s for consumer to start...");
            Thread.sleep(30000L);
        }finally {
            exec.shutdownNow();
            Thread.sleep(1000L);
            producer.close();
            consumer.close();
            TopicUtil.deleteTopic(adminHttp, topic);
            logger.info("[testHowBadCanItBe] ends.");
        }
    }

    @Test
    public void testConsumerExpireTopic() throws Exception {
        logger.info("[testConsumerExpireTopic] starts");
        final String topic1 = "testConsumerExpireTopic1";
        final String topic2 = "testConsumerExpireTopic2";
        final NSQConfig config = new NSQConfig("default");
        config.setLookupAddresses(props.getProperty("lookup-addresses"));
        ScheduledExecutorService exec = null;
        Producer producer = null;
        MockedConsumer consumer = null;
        String adminHttp = "http://" + props.getProperty("admin-address");
        try {
            TopicUtil.createTopic(adminHttp, topic1, 2, 1,"default");
            TopicUtil.createTopicChannel(adminHttp, topic1, "default");

            TopicUtil.createTopic(adminHttp, topic2, 2, 1, "default");
            TopicUtil.createTopicChannel(adminHttp, topic2, "default");

            MessageHandler handler = new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    try {
                        Thread.sleep(100);
                        throw new RuntimeException("exp on purpose");
                    } catch (InterruptedException e) {
                        logger.error("Interrupted while sleep");
                    }
                }
            };

            consumer = new MockedConsumer(config, handler);
            consumer.subscribe(topic1);
            consumer.startParent();
            logger.info("Wait for 30s for consumer to start...");
            Thread.sleep(30000L);
            consumer.unsubscribe(topic1);
            consumer.subscribe(topic2);
            logger.info("Wait for 30s for consumer to resubscribe...");
            Thread.sleep(30000);
            Map<Address, NSQConnection> addr2Conn = consumer.getAddress2Conn();
            Assert.assertEquals(addr2Conn.size(), 2, "existing partition num does not match");
            for(NSQConnection conn : addr2Conn.values()) {
                Assert.assertEquals(conn.getTopic().getTopicText(), topic2);
            }
        }finally {
            Thread.sleep(1000L);
            consumer.close();
            TopicUtil.deleteTopic(adminHttp, topic1);
            TopicUtil.deleteTopic(adminHttp, topic2);
            logger.info("[testConsumerExpireTopic] ends.");
        }
    }

    @Test
    public void testExplicitReQueueMessage() throws Exception {
        logger.info("[testExplicitReQueueMessage] starts");
        final NSQConfig config = new NSQConfig("default");
        config.setLookupAddresses(props.getProperty("lookup-addresses"));
        final String topic = "testExplicitReQueueMessage";
        String adminHttp = "http://" + props.getProperty("admin-address");
        Producer producer = new ProducerImplV2(config);
        producer.start();
        Consumer consumer = new ConsumerImplV2(config);
        try{
            TopicUtil.createTopic(adminHttp, topic, 2, 1,"default");
            TopicUtil.createTopicChannel(adminHttp, topic, "default");
            final CountDownLatch latch = new CountDownLatch(1);
            producer.publish("message requeue explicit", topic);

            MessageHandler handler = new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    try {
                        if(message.getReadableAttempts() == 1) {
                            message.setNextConsumingInSecond(1);
                            throw new ExplicitRequeueException("exp on purpose requeue");
                        }
                        if(message.getReadableAttempts() == 2) {
                            logger.info("requeue message received. {}", message);
                            latch.countDown();
                        }
                    } catch (NSQException e) {
                        logger.error("Interrupted while sleep");
                    }
                }
            };
            consumer.setMessageHandler(handler);
            consumer.subscribe(topic);
            consumer.start();
            Assert.assertTrue(latch.await(90, TimeUnit.SECONDS));
        } finally {
            producer.close();
            consumer.close();
            Thread.sleep(5000);
            TopicUtil.deleteTopic(adminHttp, topic);
            logger.info("[testExplicitReQueueMessage] ends.");
        }
    }

    @Test
    public void testSkipMessage() {
        NSQConfig config = new NSQConfig();
//        Map<String, String> skipKV1 = new HashMap<>();
//        skipKV1.put("zan_test", "true");
//        config.setMessageSkipExtensionKVMap(skipKV1);
        config.setMessageSkipExtensionKey("zan_test");

        Map<String, Object> jsonHeader = new HashMap<>();
        jsonHeader.put("zan_test", "true");
        jsonHeader.put("desiredTag", "another");
        NSQMessage msg = new NSQMessage();
        msg.setJsonExtHeader(jsonHeader);

        MockedConsumer consumer = new MockedConsumer(config, null);
        boolean skipped = consumer.needSkip4MsgKV(msg);
        Assert.assertTrue(skipped);

        config.setMessageSkipExtensionKey("zan_test");

        Map<String, Object> jsonHeader3 = new HashMap<>();
        jsonHeader3.put("zan_test", "false");
        msg = new NSQMessage();
        msg.setJsonExtHeader(jsonHeader3);
        skipped = consumer.needSkip4MsgKV(msg);
        Assert.assertTrue(skipped);


        NSQConfig config2 = new NSQConfig();
        config2.setMessageSkipExtensionKey("zan_test_not");

        Map<String, Object> jsonHeader2 = new HashMap<>();
        jsonHeader2.put("zan_test", Boolean.TRUE);
        jsonHeader2.put("desiredTag", "another");
        NSQMessage msg3 = new NSQMessage();
        msg3.setJsonExtHeader(jsonHeader2);

        MockedConsumer consumer2 = new MockedConsumer(config2, null);
        skipped = consumer2.needSkip4MsgKV(msg3);
        Assert.assertFalse(skipped);
    }

    @Test
    public void testMessageHeaderFilter() {
        NSQConfig config = new NSQConfig("BaseConsumer");
        config.setConsumeMessageFilter("filter_key1", "filter_val1");
        ConsumerImplV2 consumer = new ConsumerImplV2(config, new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                //nothing happen
            }
        });
        //mock a message frame, and NSQConnection
        Map<String, Object> jsonExt = new HashMap();
        jsonExt.put("filter_key1", "filter_val1");
        jsonExt.put("filter_key2", "filter_val2");

        NSQMessage message = new NSQMessage();
        message.setJsonExtHeader(jsonExt);

        MockedNSQConnectionImpl conn = new MockedNSQConnectionImpl(0, new Address("127.0.0.1", 4150, "ha", "fakeTopic", 1, true), null, config);

        Assert.assertTrue(consumer.checkExtFilter(message, conn));

        Map<String, Object> jsonExtMissing = new HashMap();
        jsonExtMissing.put("filter_key3", "filter_val3");
        jsonExtMissing.put("filter_key2", "filter_val2");

        message.setJsonExtHeader(jsonExtMissing);
        Assert.assertFalse(consumer.checkExtFilter(message, conn));

        jsonExtMissing = new HashMap();
        jsonExtMissing.put("filter_key1", "filter_val1_missing");
        jsonExtMissing.put("filter_key2", "filter_val2");

        message.setJsonExtHeader(jsonExtMissing);
        Assert.assertFalse(consumer.checkExtFilter(message, conn));

        //test default
        config = new NSQConfig("BaseConsumer");
        consumer = new ConsumerImplV2(config, new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                //nothing happen
            }
        });

        message.setJsonExtHeader(jsonExtMissing);
        Assert.assertTrue(consumer.checkExtFilter(message, conn));
    }

    @Test
    public void testConsumerToString() {
        NSQConfig config = new NSQConfig("BaseConsumer");
        Consumer consumer = new ConsumerImplV2(config);
        String consumerString = consumer.toString();
        Assert.assertTrue(consumerString.contains("BaseConsumer"));
    }

    private ScheduledExecutorService keepMessagePublish(final Producer producer, final String topic, long interval) throws NSQException {
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    producer.publish("message keep sending.", topic);
                } catch (NSQException e) {
                    logger.error("fail to send message.", e);
                }
            }
        }, 0, interval, TimeUnit.MILLISECONDS);
        return exec;
    }
}
