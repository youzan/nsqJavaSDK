package it.youzan.nsq.client;

import com.youzan.nsq.client.ConsumerImplV2;
import com.youzan.nsq.client.MessageHandler;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.entity.NSQConfig;
import org.junit.Assert;
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
        final String msgStr = "The quick brown fox jumps over the lazy dog, 那只迅捷的灰狐狸跳过了那条懒狗";
        consumer = new ConsumerImplV2(config, new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                logger.info("Message received: " + message.getReadableContent());
                org.testng.Assert.assertTrue(message.getReadableContent().startsWith(msgStr));
                logger.info("From topic {}", message.getTopicInfo());
                Assert.assertNotNull(message.getTopicInfo());
                received.incrementAndGet();
                latch.countDown();
            }
        });
        consumer.setAutoFinish(true);
        consumer.subscribe("JavaTesting-Producer-Base");
        consumer.start();
        try {
            Assert.assertTrue(latch.await(1, TimeUnit.MINUTES));
            Thread.sleep(100);
        }finally {
            consumer.close();
            logger.info("Consumer received {} messages.", received.get());
        }
    }

    public void testSnappy() throws InterruptedException, NSQException {
        final CountDownLatch latch = new CountDownLatch(10);
        final AtomicInteger received = new AtomicInteger(0);
        config.setCompression(NSQConfig.Compression.SNAPPY);
        final String msgStr = "The quick brown fox jumps over the lazy dog, 那只迅捷的灰狐狸跳过了那条懒狗";
        consumer = new ConsumerImplV2(config, new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                logger.info("Message received: " + message.getReadableContent());
                org.testng.Assert.assertTrue(message.getReadableContent().startsWith(msgStr));
                logger.info("From topic {}", message.getTopicInfo());
                Assert.assertNotNull(message.getTopicInfo());
                received.incrementAndGet();
                latch.countDown();
            }
        });
        consumer.setAutoFinish(true);
        consumer.subscribe("JavaTesting-Producer-Base");
        consumer.start();
        try {
            Assert.assertTrue(latch.await(90, TimeUnit.MINUTES));
            Thread.sleep(100);
        }finally {
            consumer.close();
            logger.info("Consumer received {} messages.", received.get());
        }
    }

    public void testDeflate() throws InterruptedException, NSQException {
        final CountDownLatch latch = new CountDownLatch(10);
        final AtomicInteger received = new AtomicInteger(0);
        config.setCompression(NSQConfig.Compression.DEFLATE);
        config.setDeflateLevel(3);
        final String msgStr = "The quick brown fox jumps over the lazy dog, 那只迅捷的灰狐狸跳过了那条懒狗";
        consumer = new ConsumerImplV2(config, new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                logger.info("Message received: " + message.getReadableContent());
                org.testng.Assert.assertTrue(message.getReadableContent().startsWith(msgStr));
                logger.info("From topic {}", message.getTopicInfo());
                Assert.assertNotNull(message.getTopicInfo());
                received.incrementAndGet();
                latch.countDown();
            }
        });
        consumer.setAutoFinish(true);
        consumer.subscribe("JavaTesting-Producer-Base");
        consumer.start();
        try {
            Assert.assertTrue(latch.await(1, TimeUnit.MINUTES));
            Thread.sleep(100);
        }finally {
            consumer.close();
            logger.info("Consumer received {} messages.", received.get());
        }
    }

}
