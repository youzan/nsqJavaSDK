package it.youzan.nsq.client;

import com.youzan.nsq.client.ConsumerImplV2;
import com.youzan.nsq.client.MessageHandler;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.exception.NSQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Test(groups = {"ITConsumer-Base"}, dependsOnGroups = {"ITProducer-Base"}, priority = 5)
public class ITConsumer extends AbstractITConsumer{
    private static final Logger logger = LoggerFactory.getLogger(ITConsumer.class);

    public void test() throws NSQException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(10);
        final AtomicInteger received = new AtomicInteger(0);
        consumer = new ConsumerImplV2(config, new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                received.incrementAndGet();
                latch.countDown();
            }
        });
        consumer.setAutoFinish(true);
        consumer.subscribe("JavaTesting-Producer-Base");
        consumer.start();
        latch.await(1, TimeUnit.MINUTES);
        logger.info("Consumer received {} messages.", received.get());
    }

}
