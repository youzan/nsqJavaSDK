package com.youzan.nsq.client;

import com.youzan.nsq.client.core.ConnectionManager;
import com.youzan.nsq.client.core.NSQConnection;
import com.youzan.nsq.client.entity.Message;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by lin on 17/3/23.
 */
public class ConsumerTest extends AbstractNSQClientTestcase {

    private final static Logger logger = LoggerFactory.getLogger(ConsumerTest.class);

    @Test
    public void testNextConsumingTimeout() throws NSQException, InterruptedException {
        final NSQConfig config = new NSQConfig("BaseConsumer");
        config.setLookupAddresses(props.getProperty("lookup-addresses"));

        Producer producer = new ProducerImplV2(config);
        producer.start();
        producer.publish(Message.create(new Topic("JavaTesting-Producer-Base"), "msg1"));
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
                if(0 == cnt.get())
                    Assert.assertEquals(timeout, nextTimeoutDefault);
                else {
                    Assert.assertTrue(System.currentTimeMillis() - timestamp.get() < 13000);
                }
                try {
                    message.setNextConsumingInSecond(10);
                } catch (NSQException e) {
                    logger.error("Fail to update next consuming timeout.");
                }
                if(0 == cnt.get()) {
                    cnt.incrementAndGet();
                    timestamp.set(System.currentTimeMillis());
                    throw new RuntimeException("on purpose exception");
                }else{
                    latch.countDown();
                }
            }
        });

        consumer.subscribe(new Topic("JavaTesting-Producer-Base"));
        consumer.start();
        latch.await(1, TimeUnit.MINUTES);
    }

    @Test
    public void testRdyIncrease() throws NSQException, InterruptedException {
        logger.info("[testRdyIncrease] starts.");
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
            exec = keepMessagePublish(producer, 1000);

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
            consumer.subscribe("test5Par1Rep");
            consumer.start();
            logger.info("Sleep 30 sec to wait for rdy to increase...");
            Thread.sleep(30000);
            logger.info("Wake up.");

            ConnectionManager conMgr = ((ConsumerImplV2) consumer).getConnectionManager();
            Set<ConnectionManager.NSQConnectionWrapper> connSet = conMgr.getSubscribeConnections("test5Par1Rep");
            for (ConnectionManager.NSQConnectionWrapper wrapper : connSet) {
                int actualRdy = wrapper.getConn().getCurrentRdyCount();
                Assert.assertEquals(actualRdy, expectRdy, "rdy in connection does not equals to expected rdy.");
            }
        }finally {
            exec.shutdown();
            Thread.sleep(10000L);
            producer.close();
            logger.info("Wait for 10sec to clean mq channel.");
            Thread.sleep(10000L);
            consumer.close();
            logger.info("[testRdyIncrease] ends.");
        }
    }

    @Test
    public void testLoadFactor() throws NSQException, InterruptedException {
        logger.info("[testRdyIncrease] starts.");
        Random ran = new Random();
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
            exec = keepMessagePublish(producer, 10);
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
            consumer.subscribe("test5Par1Rep");
            consumer.start();
            logger.info("Wait for 20s for consumer to start...");
            Thread.sleep(20000L);
            float lastLoadFactor = 0f, loadFactor;
            for(int i = 0; i < 10; i++) {
                Thread.sleep(5000);
                loadFactor = ((ConsumerImplV2)consumer).getLoadFactor();
                logger.info("loadFactor {}", loadFactor);
                lastLoadFactor = loadFactor;
            }
        }finally {
            exec.shutdown();
            Thread.sleep(10000L);
            producer.close();
            logger.info("Wait for 10sec to clean mq channel.");
            Thread.sleep(10000L);
            consumer.close();
            logger.info("[testRdyIncrease] ends.");
        }
    }

    private ScheduledExecutorService keepMessagePublish(final Producer producer, long interval) throws NSQException {
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    producer.publish("message keep sending.", "test5Par1Rep");
                } catch (NSQException e) {
                    logger.error("fail to send message.", e);
                }
            }
        }, 0, interval, TimeUnit.MILLISECONDS);
        return exec;
    }
}
