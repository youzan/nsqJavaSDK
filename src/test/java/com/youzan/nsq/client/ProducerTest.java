package com.youzan.nsq.client;

import com.youzan.nsq.client.entity.Message;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.exception.NSQInvalidMessageException;
import com.youzan.nsq.client.exception.NSQProducerNotFoundException;
import com.youzan.nsq.client.exception.NSQTopicNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;

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
    public void testPubException2InvalidChannel() throws NSQException, IOException {
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

    //POST /api/topics
    private void createTopic(String adminUrl, String topicName) throws IOException {
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
    }

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

//    @Test
//    public void testCreateChannel4binlog() throws IOException {
//        //1. fetch topic from admin
//        URL topicURL = new URL("http://sqs-qa.s.qima-inc.com:4171/api/topics");
//        JsonNode node = IOUtil.readFromUrl(topicURL);
//        JsonNode topicNodes = node.get("topics");
//        List<String> binlog_topics = new ArrayList<>();
//        for(JsonNode topicName : topicNodes){
//            String topicNameStr = topicName.asText();
//            if(topicNameStr.startsWith("binlog_")) {
//                binlog_topics.add(topicNameStr);
//            }
//        }
//
//        //2. create channel
//        String channelName = "default";
//        String channelURlStr = "http://sqs-qa.s.qima-inc.com:4171/api/topics/%s/%s";
//        for(String aBinlogTopic : binlog_topics){
//            String topicChannelURLStr = String.format(channelURlStr, aBinlogTopic, channelName);
//            URL channelUrl = new URL(topicChannelURLStr);
//
//            HttpURLConnection con = (HttpURLConnection) channelUrl.openConnection();
//            con.setRequestMethod("POST");
//            con.setRequestProperty("Accept", "application/vnd.nsq; version=1.0");
//            con.setDoOutput(true);
//            con.setConnectTimeout(5 * 1000);
//            con.setReadTimeout(10 * 1000);
//
//            con.getOutputStream().write("{\"action\":\"create\"}".getBytes());
//            InputStream inStream=con.getInputStream();
//            inStream.close();
//        }
//    }
}
