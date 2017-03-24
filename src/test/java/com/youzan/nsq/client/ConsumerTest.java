package com.youzan.nsq.client;

import com.youzan.nsq.client.entity.Message;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by lin on 17/3/23.
 */
public class ConsumerTest extends AbstractNSQClientTestcase {

    private final static Logger logger = LoggerFactory.getLogger(ConsumerTest.class);
//    @Test
//    public void testConsumeWDummyLookupSource() throws NSQException, InterruptedException {
//        NSQConfig config = new NSQConfig("BaseConsumer");
//        config.setLookupAddresses("dcc://123.123.123.123:8089?env=qa");
//        final CountDownLatch latch = new CountDownLatch(1);
//        Consumer consumer = new ConsumerImplV2(config, new MessageHandler() {
//            @Override
//            public void process(NSQMessage message) {
//                //do nothing
//            }
//        });
//        consumer.subscribe("JavaTesting-Producer-Base");
//        consumer.start();
//        latch.await();
//    }
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
}
