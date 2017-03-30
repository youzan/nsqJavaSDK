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
        config.setUserSpecifiedLookupAddress(true);
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
        config.setUserSpecifiedLookupAddress(true);
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
        String contentStr = String.format("{\"topic\":\"%s\",\"partition_num\":\"2\", \"replicator\":\"1\", \"retention_days\":\"\", \"syncdisk\":\"\"}", topicName);
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
        Thread.sleep(1000);
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
