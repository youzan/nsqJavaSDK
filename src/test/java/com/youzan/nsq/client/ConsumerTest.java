package com.youzan.nsq.client;

import com.youzan.nsq.client.core.ConnectionManager;
import com.youzan.nsq.client.entity.Message;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.utils.TopicUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

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

    @Test
    public void testNextConsumingTimeout() throws Exception {
        logger.info("[testNextConsumingTimeout] starts.");
        final NSQConfig config = new NSQConfig("BaseConsumer");
        config.setLookupAddresses(props.getProperty("lookup-addresses"));
        String adminHttp = "http://" + props.getProperty("admin-address");
        TopicUtil.emptyQueue(adminHttp, "JavaTesting-ReQueue", "BaseConsumer");
        final int requeueTimeout = 10;
        try {
            Producer producer = new ProducerImplV2(config);
            producer.start();
            producer.publish(Message.create(new Topic("JavaTesting-ReQueue"), "msg1"));
            producer.close();
            final int nextTimeoutDefault = config.getNextConsumingInSecond();
            final AtomicInteger cnt = new AtomicInteger(0);
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicLong timestamp = new AtomicLong(0);
            //consumer
            Consumer consumer = new ConsumerImplV2(config, new MessageHandler() {
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

            consumer.subscribe(new Topic("JavaTesting-ReQueue"));
            consumer.start();
            Assert.assertTrue(latch.await(2, TimeUnit.MINUTES));
        }finally {
            logger.info("[testNextConsumingTimeout] ends.");
            TopicUtil.emptyQueue(adminHttp, "JavaTesting-ReQueue", "BaseConsumer");
        }
    }

    @Test
    public void testRdyIncrease() throws Exception {
        logger.info("[testRdyIncrease] starts.");
        final String topic = "test5Par1Rep";
        Random ran = new Random();
        int expectRdy = ran.nextInt(6) + 5;
        logger.info("ExpectedRdy: {}", expectRdy);
        final NSQConfig config = new NSQConfig("BaseConsumer");
        config.setLookupAddresses(props.getProperty("lookup-addresses"));
        config.setRdy(expectRdy);
        ScheduledExecutorService exec = null;
        Producer producer = null;
        Consumer consumer = null;
        try {
            producer = new ProducerImplV2(config);
            producer.start();
            exec = keepMessagePublish(producer, topic, 1000);

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
            int timeout = expectRdy * 10;
            logger.info("Sleep {} sec to wait for rdy to increase...", timeout);
            Thread.sleep(timeout * 1000);
            logger.info("Wake up.");

            ConnectionManager conMgr = ((ConsumerImplV2) consumer).getConnectionManager();
            Set<ConnectionManager.NSQConnectionWrapper> connSet = conMgr.getSubscribeConnections(topic);
            for (ConnectionManager.NSQConnectionWrapper wrapper : connSet) {
                int actualRdy = wrapper.getConn().getCurrentRdyCount();
                Assert.assertEquals(actualRdy, expectRdy, "rdy in connection does not equals to expected rdy.");
            }
        }finally {
            exec.shutdownNow();
            Thread.sleep(10000L);
            producer.close();
            consumer.close();
            String adminHttp = "http://" + props.getProperty("admin-address");
            TopicUtil.emptyQueue(adminHttp, topic, "BaseConsumer");
            logger.info("[testRdyIncrease] ends.");
        }
    }

    @Test
    public void testLoadFactor() throws Exception {
        logger.info("[testLoadFactor] starts.");
        final String topic = "test5Par1Rep";
        int expectRdy = 10;
        logger.info("ExpectedRdy: {}", expectRdy);
        final NSQConfig config = new NSQConfig("BaseConsumer");
        config.setLookupAddresses(props.getProperty("lookup-addresses"));
        config.setRdy(expectRdy);
        ScheduledExecutorService exec = null;
        Producer producer = null;
        Consumer consumer = null;
        try {
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
            float lastLoadFactor = 0f, loadFactor;
            for(int i = 0; i < 10; i++) {
                Thread.sleep(5000);
                loadFactor = ((ConsumerImplV2)consumer).getLoadFactor();
                logger.info("loadFactor {}", loadFactor);
                Assert.assertTrue(loadFactor >= lastLoadFactor);
                lastLoadFactor = loadFactor;
            }
        }finally {
            exec.shutdownNow();
            Thread.sleep(10000L);
            producer.close();
            consumer.close();
            String adminHttp = "http://" + props.getProperty("admin-address");
            TopicUtil.emptyQueue(adminHttp, topic, "BaseConsumer");
            logger.info("[testLoadFactor] ends.");
        }
    }

    @Test
    public void testCloseConsumerWhileConsumption() throws Exception {
        logger.info("[testCloseConsumerWhileConsumption] starts.");
        final String topic = "test5Par1Rep";
        int expectRdy = 10;
        logger.info("ExpectedRdy: {}", expectRdy);
        final NSQConfig config = new NSQConfig("BaseConsumer");
        config.setLookupAddresses(props.getProperty("lookup-addresses"));
        config.setRdy(expectRdy);
        ScheduledExecutorService exec = null;
        Producer producer = null;
        Consumer consumer = null;
        try {
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
            String adminHttp = "http://" + props.getProperty("admin-address");
            TopicUtil.emptyQueue(adminHttp, topic, "BaseConsumer");
            logger.info("[testCloseConsumerWhileConsumption] ends.");
        }
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
