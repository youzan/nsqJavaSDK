package it.youzan.nsq.client;

import com.google.common.collect.Sets;
import com.youzan.nsq.client.Consumer;
import com.youzan.nsq.client.ConsumerImplV2;
import com.youzan.nsq.client.MessageHandler;
import com.youzan.nsq.client.core.ConnectionManager;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.entity.lookup.SeedLookupdAddress;
import com.youzan.nsq.client.entity.lookup.SeedLookupdAddressTestcase;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.utils.TopicUtil;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Test(groups = {"ITConsumer-Base"}, dependsOnGroups = {"ITProducer-Base"}, priority = 5)
public class ITConsumer extends AbstractITConsumer{
    private static final Logger logger = LoggerFactory.getLogger(ITConsumer.class);

    public void test() throws Exception {
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
            TopicUtil.emptyQueue("http://" + adminHttp, "JavaTesting-Producer-Base", "BaseConsumer");
        }
    }

    public void testLookupAddressUpdate() throws NSQException, InterruptedException {
        logger.info("[testLookupAddressUpdate] starts.");
        String seedLookupAddress = SeedLookupdAddressTestcase.seedlookupds[0];
        String dailyLookupAddress = SeedLookupdAddressTestcase.dailySeedlookupds[0];
        final String topic = "JavaTesting-Producer-Base";
        String[] lookupAddresses = this.config.getLookupAddresses();
        final NSQConfig config = new NSQConfig(seedLookupAddress);
        config.setRdy(4);
        config.setConsumerName("BaseConsumer");

        Consumer consumer = new ConsumerImplV2(config, new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                //nothing happen
            }
        });
        consumer.subscribe(topic);

        try {
            consumer.start();
            Thread.sleep(10000);
            //dump connection
            Set<ConnectionManager.NSQConnectionWrapper> connWrappersSet = ((ConsumerImplV2) consumer).getConnectionManager().getSubscribeConnections(topic);
            Set<Address> addresses = new HashSet<>();
            for (ConnectionManager.NSQConnectionWrapper wrapper : connWrappersSet) {
                addresses.add(wrapper.getConn().getAddress());
            }
            logger.info("nsqd addr set {}", addresses);
            //hack SeedLookupAddress
            SeedLookupdAddress seedLookup = SeedLookupdAddress.create(seedLookupAddress);
            seedLookup.setAddress(dailyLookupAddress);
            seedLookup.setClusterId(dailyLookupAddress);

            Thread.sleep(90000);
            //dump connection, again
            connWrappersSet = ((ConsumerImplV2) consumer).getConnectionManager().getSubscribeConnections(topic);
            Set<Address> addressesAfter = new HashSet<>();
            for (ConnectionManager.NSQConnectionWrapper wrapper : connWrappersSet) {
                addressesAfter.add(wrapper.getConn().getAddress());
            }

            org.testng.Assert.assertEquals(Sets.difference(addresses, addressesAfter).size(), addresses.size());
            logger.info("nsqd addr set after {}", addressesAfter);
        }finally {
            this.config.setLookupAddresses(lookupAddresses[0]);
            consumer.close();
            logger.info("[testLookupAddressUpdate] ends.");
        }
    }

    public void testMpub() throws Exception {
        final CountDownLatch latch = new CountDownLatch(30);
        final AtomicInteger received = new AtomicInteger(0);
        final String msgStr = "The quick brow";
        consumer = new ConsumerImplV2(config, new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                logger.info("Message received: " + message.getReadableContent());
                org.testng.Assert.assertTrue(message.getReadableContent().startsWith(msgStr));
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
            TopicUtil.emptyQueue("http://" + adminHttp, "JavaTesting-Producer-Base", "BaseConsumer");
        }
    }

    public void testSnappy() throws Exception {
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
            Assert.assertTrue(latch.await(2, TimeUnit.MINUTES));
            Thread.sleep(100);
        }finally {
            consumer.close();
            logger.info("Consumer received {} messages.", received.get());
            TopicUtil.emptyQueue("http://" + adminHttp, "JavaTesting-Producer-Base", "BaseConsumer");
        }
    }

    public void testDeflate() throws Exception {
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
            Assert.assertTrue(latch.await(2, TimeUnit.MINUTES));
            Thread.sleep(100);
        }finally {
            consumer.close();
            logger.info("Consumer received {} messages.", received.get());
            TopicUtil.emptyQueue("http://" + adminHttp, "JavaTesting-Producer-Base", "BaseConsumer");
        }
    }
}
