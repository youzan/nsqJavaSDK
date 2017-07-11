package com.youzan.nsq.client;

import com.youzan.nsq.client.entity.Message;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.exception.NSQInvalidMessageException;
import com.youzan.nsq.client.exception.NSQTopicNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lin on 17/1/11.
 */
public class ProducerTest extends AbstractNSQClientTestcase {

    private static final Logger logger = LoggerFactory.getLogger(ProducerTest.class);

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
    public void testPubException2InvalidChannel() throws NSQException, IOException, InterruptedException {
        String adminUrlStr = "http://" + props.getProperty("admin-address");
        String topicName = "topicHasNoChannel_" + System.currentTimeMillis();
        //create topic
        createTopic(adminUrlStr, topicName);

        NSQConfig config = this.getNSQConfig();
        Producer producer = this.createProducer(config);
        try{
            //a topic is invalid enough
            Topic topicInvalid = new Topic(topicName);
            producer.start();
            Message msg = Message.create(topicInvalid, "should not be sent");
            producer.publish(msg);
        }finally {
            producer.close();
            logger.info("Producer closed");
            deleteTopic(adminUrlStr, topicName);
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
        config.setUserSpecifiedLookupAddress(true);
        config.setLookupAddresses(props.getProperty("old-lookup-addresses"));
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
    public void testCompensationPublish() throws NSQException, IOException, InterruptedException {
        String adminUrlStr = "http://" + props.getProperty("admin-address");
        String topicName = "topicCompensation_" + System.currentTimeMillis();
        //create topic
        try {
            createTopic(adminUrlStr, topicName);

            NSQConfig config = this.getNSQConfig();
            config.setLookupAddresses(props.getProperty("lookup-addresses"));
            config.setMaxRequeueTimes(0);
            config.setConsumerName("BaseConsumer");

            Topic topic = new Topic(topicName);
            final AtomicInteger cnt = new AtomicInteger(0);
            final CountDownLatch latch = new CountDownLatch(1);
            Consumer consumer = new ConsumerImplV2(this.config, new MessageHandler() {
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
            Producer producer = new ProducerImplV2(config);
            producer.start();
            //publish one message
            producer.publish(Message.create(topic, "message"));

            Assert.assertTrue(latch.await(3, TimeUnit.MINUTES));
        }finally {
            deleteTopic(adminUrlStr, topicName);
        }
    }

    //POST /api/topics
    private void createTopic(String adminUrl, String topicName) throws IOException, InterruptedException {
        String urlStr = String.format("%s/api/topics", adminUrl);
        URL url = new URL(urlStr);
        String contentStr = String.format("{\"topic\":\"%s\",\"partition_num\":\"2\", \"replicator\":\"1\", \"retention_days\":\"\", \"syncdisk\":\"\", \"channel\":\"default\"}", topicName);
        logger.debug("Prepare to open HTTP Connection...");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setDoOutput(true);
        con.setRequestMethod("POST");
        con.setRequestProperty("Accept", "application/vnd.nsq; version=1.0");
        con.setConnectTimeout(5 * 1000);
        con.setReadTimeout(10 * 1000);
        con.getOutputStream().write(contentStr.getBytes());
        InputStream is = con.getInputStream();
        is.close();
        if (logger.isDebugEnabled()) {
            logger.debug("Request to {} responses {}:{}.", url.toString(), con.getResponseCode(), con.getResponseMessage());
        }
        Thread.sleep(10000);
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
    public void testSendConsumeCompressedBytes() throws NSQException, InterruptedException {
        final byte[] compressed = new byte[]{-57,4,0,0,-16,51,123,34,97,112,112,78,97,109,101,34,58,34,105,99,34,44,34,97,114,103,115,34,58,91,123,34,119,105,116,104,68,101,108,101,116,101,34,58,102,97,108,115,101,44,34,107,100,116,73,100,34,58,51,53,49,49,57,50,44,34,105,116,101,109,73,100,46,0,-16,69,49,52,55,54,51,49,52,44,49,50,57,57,55,57,52,93,44,34,99,111,110,116,97,105,110,115,67,111,110,116,101,110,116,34,58,116,114,117,101,44,34,99,108,97,115,115,34,58,34,99,111,109,46,121,111,117,122,97,110,46,105,99,46,112,97,114,97,109,46,105,116,101,109,108,105,115,116,46,81,117,101,114,121,80,20,0,112,82,101,113,117,105,114,101,101,0,-14,45,125,93,44,34,98,97,115,105,99,67,111,110,102,105,103,68,79,34,58,123,34,99,104,101,99,107,84,105,109,101,115,34,58,49,44,34,100,117,98,98,111,80,111,114,116,34,58,50,48,56,56,56,44,34,109,101,116,104,111,100,51,0,0,-87,0,50,123,34,110,-20,0,10,-127,0,-63,115,101,114,118,105,99,101,46,73,116,101,109,126,0,19,83,17,0,64,108,105,115,116,21,0,-12,6,82,105,99,104,73,110,102,111,34,44,34,114,101,112,108,97,121,79,112,101,110,-43,0,2,18,0,-81,82,97,116,105,111,34,58,48,125,44,101,0,37,23,115,-17,0,4,86,0,15,104,0,6,16,49,17,1,-63,112,114,101,72,111,115,116,65,100,100,114,101,88,1,-85,49,48,46,57,46,50,51,46,51,52,-86,0,3,-62,1,12,-85,0,-78,34,100,99,99,67,111,109,112,97,114,101,37,1,34,68,79,39,1,2,57,1,0,12,0,11,-84,1,57,119,119,119,47,1,52,87,119,119,45,1,33,103,101,44,1,80,74,115,111,110,34,2,2,8,-29,1,8,51,0,15,98,1,2,8,-3,0,-87,93,44,34,111,98,106,101,99,116,73,-69,1,4,20,0,-14,12,70,105,101,108,100,34,58,91,34,105,100,34,93,44,34,112,97,116,104,34,58,34,47,100,97,116,97,5,0,-48,99,111,109,112,111,110,101,110,116,115,47,42,34,40,1,-15,1,111,114,100,101,114,73,103,110,111,114,101,100,80,97,116,104,-40,0,2,44,0,0,-59,2,48,115,34,93,-74,1,15,-12,0,6,15,-67,0,14,5,31,2,0,-70,0,15,108,0,12,64,47,115,107,117,-32,1,10,127,0,-61,93,44,34,105,110,116,101,114,102,97,99,101,-113,3,15,65,1,19,20,34,-27,2,52,34,58,34,-82,2,8,76,1,-92,44,34,111,110,108,105,110,101,73,112,31,2,96,55,48,46,51,55,34,56,1,-96,114,97,109,101,116,101,114,84,121,112,65,3,12,-22,1,1,32,0,15,-106,3,12,16,93,83,2,97,115,112,111,110,115,101,-112,3,48,111,100,101,124,3,48,48,44,34,-114,1,-95,34,58,91,93,44,34,115,117,99,99,-98,2,3,3,4,32,111,117,16,4,16,48,-72,0,116,115,115,97,103,101,34,58,35,0,48,102,117,108,57,2,14,36,4,-76,97,112,105,46,99,111,109,109,111,110,46,111,0,-32,46,76,105,115,116,82,101,115,117,108,116,34,125,125};
        final Topic topic = new Topic("JavaTesting-Producer-Base");
        Message msg = Message.create(topic, compressed);
        byte[] byteInMsg = msg.getMessageBodyInByte();

        Assert.assertEquals(compressed, byteInMsg);

        NSQConfig config = this.getNSQConfig();
        config.setConsumerName("BaseConsumer");
        config.setLookupAddresses(props.getProperty("lookup-addresses"));
        Producer producer = this.createProducer(config);
        try{
            //a topic is invalid enough
            producer.start();
            for(int i = 0; i < 100; i++)
                producer.publish(compressed, topic);
        }finally {
            producer.close();
            logger.info("Producer closed");
        }


        //consume
        final List<NSQMessage> msgLst = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(100);
        final Consumer consumer = new ConsumerImplV2(config, new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                byte[] msgBytes = message.getMessageBody();
                Assert.assertEquals(compressed, msgBytes, "Bytes returned from nsq does not match origin.");
                msgLst.add(message);
                latch.countDown();
            }
        });
        consumer.subscribe(topic);
        consumer.start();
        Assert.assertTrue(latch.await(60, TimeUnit.SECONDS));
        consumer.close();

        for(int i = 0; i < 100; i++) {
            byte[] byteReceived = msgLst.get(i).getMessageBody();
            Assert.assertEquals(byteReceived, compressed);
        }
    }

    @Test
    public void testCompressContent() throws NSQException, InterruptedException {
        Map<String, List<String>> target = new HashMap<>();
        List<String> aList = new ArrayList<>();
        aList.add("A");
        aList.add("A");
        aList.add("A");
        aList.add("A");
        target.put("AKey", aList);
        final byte[] compressed = CompressUtil.compress(target);

        final Topic topic = new Topic("JavaTesting-Producer-Base");
        Message msg = Message.create(topic, compressed);
        byte[] byteInMsg = msg.getMessageBodyInByte();

        Assert.assertEquals(compressed, byteInMsg);

        NSQConfig config = this.getNSQConfig();
        config.setConsumerName("BaseConsumer");
        config.setLookupAddresses(props.getProperty("lookup-addresses"));
        Producer producer = this.createProducer(config);
        try{
            //a topic is invalid enough
            producer.start();
            for(int i = 0; i < 100; i++)
                producer.publish(compressed, topic);
        }finally {
            producer.close();
            logger.info("Producer closed");
        }


        //consume
        final List<NSQMessage> msgLst = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(100);
        final Consumer consumer = new ConsumerImplV2(config, new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                byte[] msgBytes = message.getMessageBody();
                Assert.assertEquals(compressed, msgBytes, "Bytes returned from nsq does not match origin.");
                msgLst.add(message);
                latch.countDown();
            }
        });
        consumer.subscribe(topic);
        consumer.start();
        Assert.assertTrue(latch.await(60, TimeUnit.SECONDS));
        consumer.close();

        for(int i = 0; i < 100; i++) {
            byte[] byteReceived = msgLst.get(i).getMessageBody();
            Map<String, List<String>> targetReceived = CompressUtil.decompress(byteReceived, target.getClass());
            List<String> aListRec = targetReceived.get("AKey");
            Assert.assertEquals(aListRec.size(), 4);
        }
    }

    @Test
    public void testMessageWCompressedString() throws IOException, NSQException, InterruptedException {
        String raw  = "This is raw message for compress";
        final byte[] byteCompressed = IOUtil.compress(raw);
        final Topic topic = new Topic("JavaTesting-Producer-Base");
        Message msg = Message.create(topic, byteCompressed);
        byte[] byteInMsg = msg.getMessageBodyInByte();

        Assert.assertNotEquals(byteCompressed, new String(byteCompressed, IOUtil.DEFAULT_CHARSET).getBytes(IOUtil.DEFAULT_CHARSET));
        Assert.assertEquals(byteCompressed, byteInMsg);
        NSQConfig config = this.getNSQConfig();
        config.setConsumerName("BaseConsumer");
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
            Assert.assertTrue(latch.await(20, TimeUnit.SECONDS));
            consumer.finish(msgLst.get(0));
        }finally {
            consumer.close();
        }
    }

//    @Test
//    public void testMultiProducer() throws NSQException, InterruptedException {
//        int producerNum = 3;
//        int connectionNUm = 9;
//        NSQConfig configPro = new NSQConfig();
//        configPro.setConnectionPoolSize(connectionNUm);
//        final Topic topic = new Topic("JavaTesting-Stable");
//        configPro.setUserSpecifiedLookupAddress(true);
//        configPro.setLookupAddresses("sqs-qa.s.qima-inc.com:4161");
//        ExecutorService exec = Executors.newFixedThreadPool(20);
//        for(int i=0; i < producerNum;i++){
//            final Producer pro = new ProducerImplV2(configPro);
//            pro.start();
//            exec.submit(new Runnable() {
//                @Override
//                public void run() {
//                    while(true) {
//                        try {
//                            pro.publish("message from producer.", topic, 0);
//                            try {
//                                Thread.sleep(100);
//                            } catch (InterruptedException e) {
//                                logger.error(e.getLocalizedMessage(), e);
//                            }
//                        } catch (NSQException e) {
//                            logger.error(e.getLocalizedMessage(), e);
//                        }
//                    }
//                }
//            });
//        }
//        final CountDownLatch latch = new CountDownLatch(1);
//        latch.await();
//    }

    //DELETE /api/topics/:topic
    private void deleteTopic(String adminUrl, String topicName) throws IOException {
        String urlStr = String.format("%s/api/topics/%s", adminUrl, topicName);
        URL url = new URL(urlStr);
        logger.debug("Prepare to open HTTP Connection...");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("DELETE");
        con.setRequestProperty("Accept", "application/vnd.nsq; version=1.0");
        con.setConnectTimeout(5 * 1000);
        con.setReadTimeout(10 * 1000);
        InputStream is = con.getInputStream();
        is.close();
        if (logger.isDebugEnabled()) {
            logger.debug("Request to {} responses {}:{}.", url.toString(), con.getResponseCode(), con.getResponseMessage());
        }
    }

}
