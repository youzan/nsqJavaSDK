package it.youzan.nsq.client;

import com.youzan.nsq.client.Consumer;
import com.youzan.nsq.client.ConsumerImplV2;
import com.youzan.nsq.client.MessageHandler;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.exception.NSQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lin on 16/8/19.
 */
@Test(groups = {"ITConsumerWPartition-Base"}, dependsOnGroups = {"ITProducerWPartition-Base"}, priority = 5)
public class ITConsumerWPartition extends AbstractITConsumer{

    private static final Logger logger = LoggerFactory.getLogger(ITConsumerWPartition.class);

    public void test() throws NSQException, InterruptedException {

        String[] lookupds = config.getLookupAddresses();
        if(config.getUserSpecifiedLookupAddress() && null != lookupds && lookupds[0].contains("nsq-"))
            return;

        final CountDownLatch latch = new CountDownLatch(10);
        final AtomicInteger received = new AtomicInteger(0);
        Consumer consumer = new ConsumerImplV2(config, new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                received.incrementAndGet();
                latch.countDown();
            }
        });
        consumer.setAutoFinish(true);
        consumer.subscribe("JavaTesting-Producer-Base");
        consumer.start();
        Assert.assertTrue(latch.await(1, TimeUnit.MINUTES));
        logger.info("Consumer received {} messages.", received.get());
        consumer.close();
    }


    //start up two consumer subscribe on different partition, one should receive and another should NOT
    public void testTwoConsumerOn2Partition() throws NSQException, InterruptedException {

        String[] lookupds = config.getLookupAddresses();
        if(config.getUserSpecifiedLookupAddress() && null != lookupds && lookupds[0].contains("nsq-"))
            return;

        final CountDownLatch latch = new CountDownLatch(2);
        final AtomicInteger received = new AtomicInteger(0);
        config.setOrdered(true);
        Consumer recievedConsumer = new ConsumerImplV2(config, new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                latch.countDown();
                received.incrementAndGet();
                logger.info("Received {}", message.getReadableContent());
            }
        });

        recievedConsumer.setAutoFinish(true);
        recievedConsumer.subscribe("JavaTesting-Partition");
        recievedConsumer.start();
        Assert.assertTrue(latch.await(1, TimeUnit.MINUTES));
        Assert.assertEquals(received.get(), 2);
        recievedConsumer.close();
    }
}
