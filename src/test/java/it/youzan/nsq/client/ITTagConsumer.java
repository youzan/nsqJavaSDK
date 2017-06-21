package it.youzan.nsq.client;

import com.youzan.nsq.client.Consumer;
import com.youzan.nsq.client.ConsumerImplV2;
import com.youzan.nsq.client.MessageHandler;
import com.youzan.nsq.client.entity.DesiredTag;
import com.youzan.nsq.client.entity.NSQConfig;
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
 * Created by lin on 17/6/12.
 */
public class ITTagConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ITTagConsumer.class);

    @Test
    public void test() throws NSQException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(200);
        final AtomicInteger receivedTag1 = new AtomicInteger(0);
        final AtomicInteger receivedTag2 = new AtomicInteger(0);
        NSQConfig config = new NSQConfig("default");
        config.setLookupAddresses("qabb-qa-nsqtest0:4161");
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
        consumer.subscribe("tag_java_2par_2rep");
        consumer.start();

        NSQConfig configTag = new NSQConfig("default");
        configTag.setLookupAddresses("qabb-qa-nsqtest0:4161");
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
        consumerTag.subscribe("tag_java_2par_2rep");
        consumerTag.start();

        Assert.assertTrue(latch.await(5, TimeUnit.MINUTES));
        Assert.assertEquals(receivedTag1.get(), 100);
        Assert.assertEquals(receivedTag2.get(), 100);
    }
}
