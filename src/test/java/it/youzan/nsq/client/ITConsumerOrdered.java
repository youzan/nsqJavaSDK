package it.youzan.nsq.client;

import com.youzan.nsq.client.ConsumerImplV2;
import com.youzan.nsq.client.MessageHandler;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.entity.Topic;
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
@Test(groups = {"ITConsumerOrdered"}, dependsOnGroups = {"ITProducerOrdered"}, priority = 5)
public class ITConsumerOrdered extends AbstractITConsumer {
    private final static Logger logger = LoggerFactory.getLogger(ITConsumerOrdered.class);

    public void test() throws InterruptedException, NSQException {
        final CountDownLatch latch = new CountDownLatch(100);
        final AtomicInteger received = new AtomicInteger(0);
        final AtomicInteger current = new AtomicInteger(-1);
        final AtomicBoolean fail = new AtomicBoolean(false);

        String[] lookupds = config.getLookupAddresses();
        if(config.getUserSpecifiedLookupAddress() && null != lookupds && lookupds[0].contains("nsq-"))
            return;

        //turn on sub ordered
        config.setOrdered(true);
        consumer = new ConsumerImplV2(config, new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                String msgStr = message.getReadableContent();
                int num = Integer.valueOf(msgStr.split("#")[1]);
                logger.info("Message received: " + msgStr);
                received.incrementAndGet();

                if(current.get() >= num) {
                    Assert.fail("Message is not received in ordered. Current#" + current.get() + " but got#" + num);
                    fail.set(true);
                }
                else
                    current.set(num);

                latch.countDown();
            }
        });
        consumer.setAutoFinish(true);
        Topic aTopic = new Topic("JavaTesting-Order");
        aTopic.setPartitionID(1);
        consumer.subscribe(aTopic);
        consumer.start();
        Assert.assertTrue(latch.await(3, TimeUnit.MINUTES));
        Assert.assertFalse(fail.get());
        logger.info("Consumer received {} messages in SUB Ordered mode.", received.get());
    }

}
