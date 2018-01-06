package it.youzan.nsq.client;

import com.youzan.nsq.client.*;
import com.youzan.nsq.client.entity.Message;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.utils.TopicUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Test(groups = {"ITProducer-Base"}, priority = 3)
public class ITProducer {

    private static final Logger logger = LoggerFactory.getLogger(ITProducer.class);

    final Random random = new Random();
    protected final NSQConfig config = new NSQConfig();
    protected String adminHttp;

    @BeforeClass
    public void init() throws Exception {
        logger.info("At {} , initialize: {}", System.currentTimeMillis(), this.getClass().getName());
        final Properties props = new Properties();
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }
        final String lookups = props.getProperty("lookup-addresses");
        final String connTimeout = props.getProperty("connectTimeoutInMillisecond");
        final String msgTimeoutInMillisecond = props.getProperty("msgTimeoutInMillisecond");
        final String threadPoolSize4IO = props.getProperty("threadPoolSize4IO");
        this.adminHttp = "http://" + props.getProperty("admin-address");
        config.setLookupAddresses(lookups);
        config.setConnectTimeoutInMillisecond(Integer.valueOf(connTimeout));
        config.setMsgTimeoutInMillisecond(Integer.valueOf(msgTimeoutInMillisecond));
        config.setThreadPoolSize4IO(Integer.valueOf(threadPoolSize4IO));
    }

    public void multiPublishBatchError2() throws Exception {
        logger.info("[ITProducer#multiPublishBatchError2] starts");
        Producer producer = null;
        Consumer consumer = null;
        Topic topic = new Topic("JavaTesting-Producer-Base");
        final CountDownLatch latch = new CountDownLatch(45);
        final AtomicBoolean failed = new AtomicBoolean(false);
        try {
            TopicUtil.emptyQueue(adminHttp, "JavaTesting-Producer-Base", "BaseConsumer");
            final String msgStr = "The quick brown fox jumps over the lazy dog, 那只迅捷的灰狐狸跳过了那条懒狗";
            final byte[] msgBytes = msgStr.getBytes(Charset.defaultCharset());

            //invalid message
            ByteBuffer bf = ByteBuffer.allocate(2000000);
            int i = 0;
            while (i++ < 2000000) {
                bf.put("M".getBytes());
            }

            config.setConsumerName("BaseConsumer");
            consumer = new ConsumerImplV2(config, new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    latch.countDown();
                }
            });
            consumer.subscribe(topic);
            consumer.start();

            producer = new ProducerImplV2(config);
            producer.start();
            List<byte[]> msgs = new ArrayList<>();
            for (int cnt = 0; cnt < 2345; cnt++) {
                if ((cnt + 1) % 100 == 0)
                    msgs.add(bf.array());
                else
                    msgs.add(msgBytes);
            }
            //last batch, add an invalid one
            List<byte[]> failedMsgs = producer.publishMulti(msgs, topic, 100);
            Assert.assertEquals(2300, failedMsgs.size());
            Assert.assertTrue(latch.await(1, TimeUnit.MINUTES));
        } finally {
            producer.close();
            consumer.close();
        }
    }

    public void multiPublishBatchError1() throws Exception {
        logger.info("[ITProducer#multiPublishBatchError1] starts");
        Producer producer = null;
        Consumer consumer = null;
        Topic topic = new Topic("JavaTesting-Producer-Base");
        final CountDownLatch latch = new CountDownLatch(2300);
        final AtomicBoolean failed = new AtomicBoolean(false);
        try{
            TopicUtil.emptyQueue(adminHttp, "JavaTesting-Producer-Base", "BaseConsumer");
            final String msgStr = "The quick brown fox jumps over the lazy dog, 那只迅捷的灰狐狸跳过了那条懒狗";
            final byte[] msgBytes = msgStr.getBytes(Charset.defaultCharset());

            //invalid message
            ByteBuffer bf =  ByteBuffer.allocate(2000000);
            int i = 0;
            while(i++ < 2000000) {
                bf.put("M".getBytes());
            }

            config.setConsumerName("BaseConsumer");
            consumer = new ConsumerImplV2(config, new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    latch.countDown();
                    if(!message.getReadableContent().equals(msgStr)) {
                        failed.set(true);
                    }
                }
            });
            consumer.subscribe(topic);
            consumer.start();

            producer = new ProducerImplV2(config);
            producer.start();
            List<byte[]> msgs = new ArrayList<>();
            for(int cnt = 0; cnt < 2344; cnt++) {
                msgs.add(msgBytes);
            }
            //last batch, add an invalid one
            msgs.add(bf.array());
            List<byte[]> failedMsgs = producer.publishMulti(msgs, topic, 100);
            Assert.assertEquals(45, failedMsgs.size());
            latch.await(1, TimeUnit.MINUTES);
            Assert.assertFalse(failed.get());
        }finally {
            producer.close();
            consumer.close();
        }
    }

    public void multiPublishBatch() throws Exception {
        logger.info("[ITProducer#multiPublishBatch] starts");
        Producer producer = null;
        Consumer consumer = null;
        Topic topic = new Topic("JavaTesting-Producer-Base");
        final CountDownLatch latch = new CountDownLatch(2345);
        final AtomicBoolean failed = new AtomicBoolean(false);
        try{
            TopicUtil.emptyQueue(adminHttp, "JavaTesting-Producer-Base", "BaseConsumer");
            final String msgStr = "The quick brown fox jumps over the lazy dog, 那只迅捷的灰狐狸跳过了那条懒狗";
            final byte[] msgBytes = msgStr.getBytes(Charset.defaultCharset());
            config.setConsumerName("BaseConsumer");
            consumer = new ConsumerImplV2(config, new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    latch.countDown();
                    if(!message.getReadableContent().equals(msgStr)) {
                        failed.set(true);
                    }
                }
            });
            consumer.subscribe(topic);
            consumer.start();

            producer = new ProducerImplV2(config);
            producer.start();
            List<byte[]> msgs = new ArrayList<>();
            for(int cnt = 0; cnt < 2345; cnt++) {
                msgs.add(msgBytes);
            }
            List<byte[]> failedMsgs = producer.publishMulti(msgs, topic, 100);
            Assert.assertEquals(0, failedMsgs.size());
            latch.await(1, TimeUnit.MINUTES);
            Assert.assertFalse(failed.get());
        }finally {
            producer.close();
            consumer.close();
        }
    }

    public void multiPublish() throws Exception {
        logger.info("[ITProducer#multiPublish] starts");
        Producer producer = null;
        try {
            TopicUtil.emptyQueue(adminHttp, "JavaTesting-Producer-Base", "BaseConsumer");
            String msgStr = "The quick brown fox jumps over the lazy dog, 那只迅捷的灰狐狸跳过了那条懒狗";
            producer = new ProducerImplV2(config);
            producer.start();
            List<byte[]> msgs = new ArrayList<>();
            Random ran = new Random();
            for (int i = 0; i < 30; i++) {
                String msgPart = msgStr.substring(0, ran.nextInt(msgStr.length() - 20) + 20);
                final byte[] message = (msgPart + "#end").getBytes();
                msgs.add(message);
            }
            producer.publishMulti(msgs, "JavaTesting-Producer-Base");
        }finally {
            producer.close();
            logger.info("[ITProducer#multiPublish] ends");
        }
    }

    public void publish() throws Exception {
        logger.info("[ITProducer#publish] starts");
        Producer producer = null;
        try {
            TopicUtil.emptyQueue(adminHttp, "JavaTesting-Producer-Base", "BaseConsumer");
            String msgStr = "The quick brown fox jumps over the lazy dog, 那只迅捷的灰狐狸跳过了那条懒狗";
            producer = new ProducerImplV2(config);
            producer.start();
            for (int i = 0; i < 10; i++) {
                final byte[] message = (msgStr + " #" + i).getBytes();
                producer.publish(message, "JavaTesting-Producer-Base");
            }
        }finally {
            producer.close();
            logger.info("[ITProducer#publish] ends");
        }
    }

    public void publishSnappy() throws Exception {
        logger.info("[ITProducer#publishSnappy] starts");
        TopicUtil.emptyQueue(adminHttp, "JavaTesting-Producer-Base", "BaseConsumer");
        config.setCompression(NSQConfig.Compression.SNAPPY);
        Producer producer = new ProducerImplV2(config);
        try {
            producer.start();
            String msgStr = "The quick brown fox jumps over the lazy dog, 那只迅捷的灰狐狸跳过了那条懒狗";
            for (int i = 0; i < 10; i++) {
                final byte[] message = (msgStr + " #" + i).getBytes();
                producer.publish(message, "JavaTesting-Producer-Base");
            }
        }finally {
            producer.close();
            logger.info("[ITProducer#publishSnappy] ends");
        }
    }

    public void publishDeflate() throws Exception {
        logger.info("[ITProducer#publishDeflate] starts");
        TopicUtil.emptyQueue(adminHttp, "JavaTesting-Producer-Base", "BaseConsumer");
        config.setCompression(NSQConfig.Compression.DEFLATE);
        config.setDeflateLevel(3);
        Producer producer = new ProducerImplV2(config);
        try {
            producer.start();
            String msgStr = "The quick brown fox jumps over the lazy dog, 那只迅捷的灰狐狸跳过了那条懒狗";
            for (int i = 0; i < 10; i++) {
                final byte[] message = (msgStr + " #" + i).getBytes();
                producer.publish(message, "JavaTesting-Producer-Base");
            }
        }finally {
            producer.close();
            logger.info("[ITProducer#publishDeflate] ends");
        }
    }

    public void publishTrace() throws Exception {
        logger.info("[ITProducer#publishTrace] starts");
        TopicUtil.emptyQueue(adminHttp, "JavaTesting-Producer-Base", "BaseConsumer");
        Producer producer = new ProducerImplV2(config);
        try {
            Topic topic = new Topic("JavaTesting-Producer-Base");
            producer.start();
            String msgStr = "The quick brown fox jumps over the lazy dog, 那只迅捷的灰狐狸跳过了那条懒狗";
            for (int i = 0; i < 10; i++) {
                Message msg = Message.create(topic, msgStr);
                msg.traced();
                MessageReceipt receipt = producer.publishAndGetReceipt(msg);
                Assert.assertNotNull(receipt.getTopicName());
                Assert.assertNotNull(receipt.getNsqdAddr());
                Assert.assertEquals(0, receipt.getTraceID());
                Assert.assertNotEquals(-1, receipt.getDiskQueueOffset());
                Assert.assertNotEquals(-1, receipt.getDiskQueueSize());
            }
        }finally {
            producer.close();
            TopicUtil.emptyQueue(adminHttp, "JavaTesting-Producer-Base", "BaseConsumer");
            logger.info("[ITProducer#publishDeflate] ends");
        }
    }
}
