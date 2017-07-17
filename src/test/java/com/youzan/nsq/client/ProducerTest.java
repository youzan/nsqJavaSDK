package com.youzan.nsq.client;

import com.youzan.nsq.client.entity.Message;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.exception.NSQInvalidMessageException;
import com.youzan.nsq.client.exception.NSQTopicNotFoundException;
import com.youzan.nsq.client.utils.CompressUtil;
import com.youzan.nsq.client.utils.TopicUtil;
import com.youzan.util.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lin on 17/1/11.
 */
public class ProducerTest extends AbstractNSQClientTestcase {

    private static final Logger logger = LoggerFactory.getLogger(ProducerTest.class);

    @BeforeClass
    public void init() throws IOException {
        super.init();
    }

    @Test(expectedExceptions = {NSQTopicNotFoundException.class})
    public void testPubException2InvalidTopic() throws NSQException {
        NSQConfig config = this.getNSQConfig();
        config.setLookupAddresses(props.getProperty("old-lookup-addresses"));
        Producer producer = this.createProducer(config);
        try{
            //a topic is invalid enough
            Topic topicInvalid = new Topic("hulululu_tOpIC");
            producer.start();
            Message msg = Message.create(topicInvalid, "should not be sent");
            producer.publish(msg);
        }finally {
            producer.close();
            logger.info("Producer closed");
        }
    }

    @Test(expectedExceptions = {NSQTopicNotFoundException.class})
    public void testPubException2InvalidChannel() throws Exception {
        String adminUrlStr = "http://" + props.getProperty("admin-address");
        String topicName = "topicHasNoChannel_" + System.currentTimeMillis();
        String channel = "chanDel";
        //create topic

        NSQConfig config = this.getNSQConfig();
        Producer producer = this.createProducer(config);
        try{
            TopicUtil.createTopic(adminUrlStr, topicName, channel);
            TopicUtil.deleteTopicChannel(adminUrlStr, topicName, channel);
            //a topic is invalid enough
            Topic topicInvalid = new Topic(topicName);
            producer.start();
            Message msg = Message.create(topicInvalid, "should not be sent");
            producer.publish(msg);
        }finally {
            producer.close();
            logger.info("Producer closed");
            TopicUtil.deleteTopic(adminUrlStr, topicName);
        }
    }

    @Test(expectedExceptions = {NSQInvalidMessageException.class})
    public void testPubMessageExceed() throws NSQException {
        ByteBuffer bf =  ByteBuffer.allocate(2000000);
        int i = 0;
        while(i++ < 2000000) {
            bf.put("M".getBytes());
        }

        NSQConfig config = this.getNSQConfig();
        config.setLookupAddresses(props.getProperty("lookup-addresses"));
        Producer producer = this.createProducer(config);
        try{
            //a topic is invalid enough
            Topic topic = new Topic("JavaTesting-Producer-Base");
            producer.start();
            producer.publish(bf.array(), topic);
        }finally {
            producer.close();
            logger.info("Producer closed");
        }
    }


    @Test
    public void testCompensationPublish() throws Exception {
        String adminUrlStr = "http://" + props.getProperty("admin-address");
        String topicName = "topicCompensation_" + System.currentTimeMillis();
        Consumer consumer = null;
        Producer producer = null;
        //create topic
        try {
            TopicUtil.createTopic(adminUrlStr, topicName, "default");

            NSQConfig config = this.getNSQConfig();
            config.setLookupAddresses(props.getProperty("lookup-addresses"));
            config.setMaxRequeueTimes(0);
            config.setConsumerName("BaseConsumer");

            Topic topic = new Topic(topicName);
            final AtomicInteger cnt = new AtomicInteger(0);
            final CountDownLatch latch = new CountDownLatch(1);
            consumer = new ConsumerImplV2(this.config, new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    if (cnt.getAndIncrement() == 0)
                        throw new RuntimeException("exp");
                    else {
                        logger.info("compensation message get.");
                        latch.countDown();
                    }
                }
            });
            consumer.subscribe(topic);
            consumer.start();
            final CountDownLatch consumerWaitlatch = new CountDownLatch(1);
            logger.info("Wait for 60s for consumer to subscribe.");
            consumerWaitlatch.await(60, TimeUnit.SECONDS);
            producer = new ProducerImplV2(config);
            producer.start();
            //publish one message
            producer.publish(Message.create(topic, "message"));

            Assert.assertTrue(latch.await(3, TimeUnit.MINUTES));
        }finally {
            producer.close();
            consumer.close();
            TopicUtil.deleteTopic(adminUrlStr, topicName);
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testStartFailWNoLookupSource() throws NSQException {
        try {
            logger.info("[testStartFailWNoLookupSource] starts");
            NSQConfig invalidCnf = new NSQConfig();
            Producer prod = new ProducerImplV2(invalidCnf);
            prod.start();
        } finally {
            logger.info("[testStartFailWNoLookupSource] ends");
        }
    }

    @Test
    public void testStartWDCCLookupSource() throws NSQException {
        try {
            System.clearProperty("nsq.sdk.configFilePath");
            NSQConfig cnf = new NSQConfig();
            cnf.setLookupAddresses(props.getProperty("dcc-lookup"));
            Producer prod = new ProducerImplV2(cnf);
            try {
                prod.start();
            } finally {
                prod.close();
            }
        }finally{
            System.setProperty("nsq.sdk.configFilePath", "src/test/resources/configClientTest.properties");
        }
    }

    /**
     * what if there is a dummy lookup source
     */
    @Test(expectedExceptions = Exception.class)
    public void testLookupSourceIsDeceived() throws NSQException {
        NSQConfig cnf = new NSQConfig();
        String dummyDCCURL = "dcc://123.123.123.123:8089?env=dummy";
        cnf.setLookupAddresses(dummyDCCURL);
        Producer prod = new ProducerImplV2(cnf);
        try {
            prod.start();
            prod.publish(Message.create(new Topic("aTopic"), "should not be sent."));
        }finally {
            prod.close();
            NSQConfig.resetConfigAccessConfigs();
        }
    }

    @Test
    public void testMessageContent() {
        String raw  = "This is raw message 1234567890.";
        final Topic topic = new Topic("JavaTesting-Producer-Base");
        Message msg = Message.create(topic, raw);
        Assert.assertEquals(msg.getMessageBody(), raw);
    }

    @Test
    public void testSendConsumeCompressedBytes() throws Exception {
        logger.info("[testSendConsumeCompressedBytes] starts.");
        String adminHttp = "http://" + props.getProperty("admin-address");
        String topicName = "testSendConsumeCompressedBytes_" + System.currentTimeMillis();
        String channel = "default";
        try {
            TopicUtil.createTopic(adminHttp, topicName, channel);
            final byte[] compressed = new byte[]{-57, 4, 0, 0, -16, 51, 123, 34, 97, 112, 112, 78, 97, 109, 101, 34, 58, 34, 105, 99, 34, 44, 34, 97, 114, 103, 115, 34, 58, 91, 123, 34, 119, 105, 116, 104, 68, 101, 108, 101, 116, 101, 34, 58, 102, 97, 108, 115, 101, 44, 34, 107, 100, 116, 73, 100, 34, 58, 51, 53, 49, 49, 57, 50, 44, 34, 105, 116, 101, 109, 73, 100, 46, 0, -16, 69, 49, 52, 55, 54, 51, 49, 52, 44, 49, 50, 57, 57, 55, 57, 52, 93, 44, 34, 99, 111, 110, 116, 97, 105, 110, 115, 67, 111, 110, 116, 101, 110, 116, 34, 58, 116, 114, 117, 101, 44, 34, 99, 108, 97, 115, 115, 34, 58, 34, 99, 111, 109, 46, 121, 111, 117, 122, 97, 110, 46, 105, 99, 46, 112, 97, 114, 97, 109, 46, 105, 116, 101, 109, 108, 105, 115, 116, 46, 81, 117, 101, 114, 121, 80, 20, 0, 112, 82, 101, 113, 117, 105, 114, 101, 101, 0, -14, 45, 125, 93, 44, 34, 98, 97, 115, 105, 99, 67, 111, 110, 102, 105, 103, 68, 79, 34, 58, 123, 34, 99, 104, 101, 99, 107, 84, 105, 109, 101, 115, 34, 58, 49, 44, 34, 100, 117, 98, 98, 111, 80, 111, 114, 116, 34, 58, 50, 48, 56, 56, 56, 44, 34, 109, 101, 116, 104, 111, 100, 51, 0, 0, -87, 0, 50, 123, 34, 110, -20, 0, 10, -127, 0, -63, 115, 101, 114, 118, 105, 99, 101, 46, 73, 116, 101, 109, 126, 0, 19, 83, 17, 0, 64, 108, 105, 115, 116, 21, 0, -12, 6, 82, 105, 99, 104, 73, 110, 102, 111, 34, 44, 34, 114, 101, 112, 108, 97, 121, 79, 112, 101, 110, -43, 0, 2, 18, 0, -81, 82, 97, 116, 105, 111, 34, 58, 48, 125, 44, 101, 0, 37, 23, 115, -17, 0, 4, 86, 0, 15, 104, 0, 6, 16, 49, 17, 1, -63, 112, 114, 101, 72, 111, 115, 116, 65, 100, 100, 114, 101, 88, 1, -85, 49, 48, 46, 57, 46, 50, 51, 46, 51, 52, -86, 0, 3, -62, 1, 12, -85, 0, -78, 34, 100, 99, 99, 67, 111, 109, 112, 97, 114, 101, 37, 1, 34, 68, 79, 39, 1, 2, 57, 1, 0, 12, 0, 11, -84, 1, 57, 119, 119, 119, 47, 1, 52, 87, 119, 119, 45, 1, 33, 103, 101, 44, 1, 80, 74, 115, 111, 110, 34, 2, 2, 8, -29, 1, 8, 51, 0, 15, 98, 1, 2, 8, -3, 0, -87, 93, 44, 34, 111, 98, 106, 101, 99, 116, 73, -69, 1, 4, 20, 0, -14, 12, 70, 105, 101, 108, 100, 34, 58, 91, 34, 105, 100, 34, 93, 44, 34, 112, 97, 116, 104, 34, 58, 34, 47, 100, 97, 116, 97, 5, 0, -48, 99, 111, 109, 112, 111, 110, 101, 110, 116, 115, 47, 42, 34, 40, 1, -15, 1, 111, 114, 100, 101, 114, 73, 103, 110, 111, 114, 101, 100, 80, 97, 116, 104, -40, 0, 2, 44, 0, 0, -59, 2, 48, 115, 34, 93, -74, 1, 15, -12, 0, 6, 15, -67, 0, 14, 5, 31, 2, 0, -70, 0, 15, 108, 0, 12, 64, 47, 115, 107, 117, -32, 1, 10, 127, 0, -61, 93, 44, 34, 105, 110, 116, 101, 114, 102, 97, 99, 101, -113, 3, 15, 65, 1, 19, 20, 34, -27, 2, 52, 34, 58, 34, -82, 2, 8, 76, 1, -92, 44, 34, 111, 110, 108, 105, 110, 101, 73, 112, 31, 2, 96, 55, 48, 46, 51, 55, 34, 56, 1, -96, 114, 97, 109, 101, 116, 101, 114, 84, 121, 112, 65, 3, 12, -22, 1, 1, 32, 0, 15, -106, 3, 12, 16, 93, 83, 2, 97, 115, 112, 111, 110, 115, 101, -112, 3, 48, 111, 100, 101, 124, 3, 48, 48, 44, 34, -114, 1, -95, 34, 58, 91, 93, 44, 34, 115, 117, 99, 99, -98, 2, 3, 3, 4, 32, 111, 117, 16, 4, 16, 48, -72, 0, 116, 115, 115, 97, 103, 101, 34, 58, 35, 0, 48, 102, 117, 108, 57, 2, 14, 36, 4, -76, 97, 112, 105, 46, 99, 111, 109, 109, 111, 110, 46, 111, 0, -32, 46, 76, 105, 115, 116, 82, 101, 115, 117, 108, 116, 34, 125, 125};
            final Topic topic = new Topic(topicName);
            Message msg = Message.create(topic, compressed);
            byte[] byteInMsg = msg.getMessageBodyInByte();

            Assert.assertEquals(compressed, byteInMsg);

            NSQConfig config = this.getNSQConfig();
            config.setConsumerName(channel);
            config.setLookupAddresses(props.getProperty("lookup-addresses"));
            Producer producer = this.createProducer(config);
            try {
                //a topic is invalid enough
                producer.start();
                for (int i = 0; i < 10; i++)
                    producer.publish(compressed, topic);
            } finally {
                producer.close();
                logger.info("Producer closed");
            }


            //consume
            final List<NSQMessage> msgLst = new ArrayList<>();
            final CountDownLatch latch = new CountDownLatch(10);
            final Consumer consumer = new ConsumerImplV2(config, new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    byte[] msgBytes = message.getMessageBody();
                    Assert.assertEquals(compressed, msgBytes, "Bytes returned from nsq does not match origin. " + message.getReadableContent());
                    msgLst.add(message);
                    latch.countDown();
                }
            });
            consumer.subscribe(topic);
            consumer.start();
            Assert.assertTrue(latch.await(60, TimeUnit.SECONDS));
            Thread.sleep(5000);
            consumer.close();

            for (int i = 0; i < 10; i++) {
                byte[] byteReceived = msgLst.get(i).getMessageBody();
                Assert.assertEquals(byteReceived, compressed);
            }
        }finally {
            logger.info("[testSendConsumeCompressedBytes] ends.");
            TopicUtil.deleteTopic(adminHttp, topicName);
        }
    }

    @Test
    public void testCompressContent() throws Exception {
        logger.info("[testCompressContent] starts.");
        String adminHttp = "http://" + props.getProperty("admin-address");
        String topicName = "testSendConsumeContent_" + System.currentTimeMillis();
        String channel = "default";
        try {
            TopicUtil.createTopic(adminHttp, topicName, channel);
            Map<String, List<String>> target = new HashMap<>();
            List<String> aList = new ArrayList<>();
            aList.add("A");
            aList.add("A");
            aList.add("A");
            aList.add("A");
            target.put("AKey", aList);
            final byte[] compressed = CompressUtil.compress(target);

            final Topic topic = new Topic(topicName);
            Message msg = Message.create(topic, compressed);
            byte[] byteInMsg = msg.getMessageBodyInByte();

            Assert.assertEquals(compressed, byteInMsg);

            NSQConfig config = this.getNSQConfig();
            config.setConsumerName(channel);
            config.setLookupAddresses(props.getProperty("lookup-addresses"));
            Producer producer = this.createProducer(config);
            try {
                //a topic is invalid enough
                producer.start();
                for (int i = 0; i < 10; i++)
                    producer.publish(compressed, topic);
            } finally {
                producer.close();
                logger.info("Producer closed");
            }


            //consume
            final List<NSQMessage> msgLst = new ArrayList<>();
            final CountDownLatch latch = new CountDownLatch(10);
            final Consumer consumer = new ConsumerImplV2(config, new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    byte[] msgBytes = message.getMessageBody();
                    Assert.assertEquals(compressed, msgBytes, "Bytes returned from nsq does not match origin. " + message.getReadableContent());
                    msgLst.add(message);
                    latch.countDown();
                }
            });
            consumer.subscribe(topic);
            consumer.start();
            Assert.assertTrue(latch.await(60, TimeUnit.SECONDS));
            Thread.sleep(5000);
            consumer.close();

            for (int i = 0; i < 10; i++) {
                byte[] byteReceived = msgLst.get(i).getMessageBody();
                Map<String, List<String>> targetReceived = CompressUtil.decompress(byteReceived, target.getClass());
                List<String> aListRec = targetReceived.get("AKey");
                Assert.assertEquals(aListRec.size(), 4);
            }
        }finally {
            logger.info("[testCompressContent] ends");
            TopicUtil.deleteTopic(adminHttp, topicName);
        }
    }

    @Test
    public void testMessageWCompressedString() throws Exception {
        logger.info("[testMessageWCompressedString] starts.");
        String adminHttp = "http://" + props.getProperty("admin-address");
        String topicName = "testSendConsumeContent_" + System.currentTimeMillis();
        String channel = "default";
        TopicUtil.createTopic(adminHttp, topicName, channel);
        String raw  = "This is raw message for compress";
        final byte[] byteCompressed = IOUtil.compress(raw);
        final Topic topic = new Topic(topicName);
        Message msg = Message.create(topic, byteCompressed);
        byte[] byteInMsg = msg.getMessageBodyInByte();

        Assert.assertNotEquals(byteCompressed, new String(byteCompressed, IOUtil.DEFAULT_CHARSET).getBytes(IOUtil.DEFAULT_CHARSET));
        Assert.assertEquals(byteCompressed, byteInMsg);
        NSQConfig config = this.getNSQConfig();
        config.setConsumerName(channel);
        config.setLookupAddresses(props.getProperty("lookup-addresses"));
        Producer producer = this.createProducer(config);
        try{
            //a topic is invalid enough
            producer.start();
            producer.publish(byteCompressed, topic);
        }finally {
            producer.close();
            logger.info("Producer closed");
        }

        //consume
        final List<NSQMessage> msgLst = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);
        final Consumer consumer = new ConsumerImplV2(config, new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                byte[] msgBytes = message.getMessageBody();
                Assert.assertEquals(byteCompressed, msgBytes, "Bytes returned from nsq does not match origin.");
                msgLst.add(message);
                latch.countDown();
            }
        });
        consumer.setAutoFinish(false);
        consumer.subscribe(topic);
        consumer.start();
        try {
            Assert.assertTrue(latch.await(30, TimeUnit.SECONDS));
            consumer.finish(msgLst.get(0));
        }finally {
            consumer.close();
            TopicUtil.deleteTopic(adminHttp, topicName);
            logger.info("[testMessageWCompressedString] ends.");
        }
    }
}
