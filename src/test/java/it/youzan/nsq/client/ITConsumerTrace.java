package it.youzan.nsq.client;

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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lin on 16/10/19.
 */
@Test(groups = {"ITConsumerTrace"}, dependsOnGroups = {"ITProducerTrace"}, priority = 5)
public class ITConsumerTrace extends AbstractITConsumer{
    private final static Logger logger = LoggerFactory.getLogger(ITConsumerTrace.class);

    public void test() throws InterruptedException, NSQException {
        final CountDownLatch latch = new CountDownLatch(10);
        final AtomicInteger received = new AtomicInteger(0);
        consumer = new ConsumerImplV2(config, new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                String msgStr = message.getReadableContent();
                logger.info("Message received: " + msgStr);
                received.incrementAndGet();
                latch.countDown();
            }
        });
        consumer.setAutoFinish(true);
        consumer.subscribe("JavaTesting-Trace");
        consumer.start();
        Assert.assertTrue(latch.await(1, TimeUnit.MINUTES));
        logger.info("Consumer received {} messages in SUB Trace mode.", received.get());
    }
}
