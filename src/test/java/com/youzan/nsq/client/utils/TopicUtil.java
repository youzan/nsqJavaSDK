package com.youzan.nsq.client.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.youzan.nsq.client.*;
import com.youzan.nsq.client.entity.Message;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.util.IOUtil;
import com.youzan.util.SystemUtil;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by lin on 17/7/11.
 */
public class TopicUtil {
    private static final Logger logger = LoggerFactory.getLogger(TopicUtil.class);

    public static void deleteTopics(final String nsqadminUrl, String topicPattern) throws IOException, InterruptedException {
        URL url = new URL(nsqadminUrl + "/api/topics");
        JsonNode root = SystemUtil.getObjectMapper().readTree(url);
        JsonNode topics = root.get("topics");
        ExecutorService exec = Executors.newFixedThreadPool(4);
        List<String> topicsNeedDel = new ArrayList<>();
        if(topics.isArray()) {
            for(JsonNode topicNode : topics) {
                if(topicNode.get("topic_name").asText().startsWith(topicPattern)) {
                    String topic = topicNode.get("topic_name").asText();
                    topicsNeedDel.add(topic);
                }
            }
            final CountDownLatch latch = new CountDownLatch(topicsNeedDel.size());
            for(final String topic:topicsNeedDel) {
                exec.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            TopicUtil.deleteTopic(nsqadminUrl, topic);
                        } catch (Exception e) {
                            logger.error("fail to delete topic {}", topic, e);
                        }
                        logger.info("topic {} deleted", topic);
                        latch.countDown();
                    }
                });
            }
            latch.await(topicsNeedDel.size() * 1000, TimeUnit.MILLISECONDS);
            logger.info("topics delete ends");
        }
    }

    public static void emptyQueue(String nsqadminUrl, String topic, String channel) throws Exception {
        URL channelUrl = new URL(nsqadminUrl + "/api/topics/" + topic + "/" + channel);
        String content = "{\"action\":\"empty\"}";
        IOUtil.postToUrl(channelUrl, content);
        Thread.sleep(5000);
    }

    public static void deleteTopicOld(String nsqdAddr, String topicName) throws IOException, InterruptedException {
        String urlStr = String.format("%s/topic/delete?topic=%s", nsqdAddr, topicName);
        URL url = new URL(urlStr);
        logger.debug("Prepare to open HTTP Connection...");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setDoOutput(true);
        con.setRequestMethod("POST");
        con.setRequestProperty("Accept", "application/vnd.nsq; version=1.0");
        con.setConnectTimeout(5 * 1000);
        con.setReadTimeout(10 * 1000);
        InputStream is = con.getInputStream();
        is.close();
        if (logger.isDebugEnabled()) {
            logger.debug("Request to {} responses {}:{}.", url.toString(), con.getResponseCode(), con.getResponseMessage());
        }
        Thread.sleep(20000);
    }

    public static void createTopicChannelOld(String nsqdAddr, String topicName, String channel) throws IOException, InterruptedException {
        String urlStr = String.format("%s/topic/create?topic=%s&channel=%s", nsqdAddr, topicName, channel);
        URL url = new URL(urlStr);
        logger.debug("Prepare to open HTTP Connection...");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setDoOutput(true);
        con.setRequestMethod("POST");
        con.setRequestProperty("Accept", "application/vnd.nsq; version=1.0");
        con.setConnectTimeout(5 * 1000);
        con.setReadTimeout(10 * 1000);
        InputStream is = con.getInputStream();
        is.close();
        if (logger.isDebugEnabled()) {
            logger.debug("Request to {} responses {}:{}.", url.toString(), con.getResponseCode(), con.getResponseMessage());
        }
        Thread.sleep(20000);
    }

    public static void createTopic(String adminUrl, String topicName, String channel) throws IOException, InterruptedException {
        createTopic(adminUrl, topicName, 2, 2, channel, false, false);
    }

    public static void createTopic(String adminUrl, String topicName, int parNum, int repNum, String channel) throws IOException, InterruptedException {
        createTopic(adminUrl, topicName, parNum, repNum, channel, false,  false);
    }

    public static void createTopic(String adminUrl, String topicName, int parNum, int repNum, String channel, boolean ordered, boolean ext) throws IOException, InterruptedException {
        String urlStr = String.format("%s/api/topics", adminUrl);
        URL url = new URL(urlStr);
        String contentStr = String.format("{\"topic\":\"%s\",\"partition_num\":\"%d\", \"replicator\":\"%d\", \"retention_days\":\"\", \"syncdisk\":\"\", \"channel\":\"%s\", \"orderedmulti\":\"%s\", \"extend\":\"%s\"}", topicName, parNum, repNum, channel, ordered, ext);
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
        Thread.sleep(20000);
    }

    public static void deleteTopic(String adminUrl, String topicName) throws Exception {
        URL channelUrl = new URL(adminUrl + "/api/topics/" + topicName);
        IOUtil.deleteToUrl(channelUrl);
    }

    public static void deleteTopicChannel(String adminUrl, String topicName, String channel) throws Exception {
        URL channelUrl = new URL(adminUrl + "/api/topics/" + topicName + "/" + channel);
        IOUtil.deleteToUrl(channelUrl);
    }

    public static void createTopicChannel(String adminUrl, String topicName, String channel) throws Exception {
        URL channelUrl = new URL(adminUrl + "/api/topics/" + topicName + "/" + channel);
        IOUtil.postToUrl(channelUrl, "{\"action\":\"create\"}");
    }

    public static void upgradeTopic(String lookupdAddr, String topicName) throws Exception {
        URL upgradeUrl = new URL(lookupdAddr + "/topic/meta/update?topic=" + topicName + "&upgradeext=true");
        IOUtil.postToUrl(upgradeUrl, null);
    }


    @Test
    /**
     * remove @Test comment and run as test cases to remove topics in qa
     * @throws IOException
     * @throws InterruptedException
     */
    public void deleteTopics() throws IOException, InterruptedException {
        logger.info("[testDeleteTopics] start.");
        Properties props = new Properties();
        logger.info("At {} , initialize: {}", System.currentTimeMillis(), this.getClass().getName());
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }
        String adminHttp = "http://" + props.getProperty("admin-address");
        String topicPattern = "topic_";
        TopicUtil.deleteTopics(adminHttp, topicPattern);
        logger.info("[testTopicUtil] ends.");
    }

//    @Test
    public void testTopicUtil() throws Exception {
        Calendar cal = Calendar.getInstance();
        cal.set(2017, 7, 9, 17, 20, 0);
        logger.info("{}", cal.getTimeInMillis());
        logger.info("[testTopicUtil] start.");
        Properties props = new Properties();
        logger.info("At {} , initialize: {}", System.currentTimeMillis(), this.getClass().getName());
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }
        String adminHttp = "http://" + props.getProperty("admin-address");
        String topicName = "topicUtilTest";
        TopicUtil.createTopic(adminHttp, topicName, "default");
        //publish to channel
        NSQConfig config = new NSQConfig("default");
        config.setLookupAddresses(props.getProperty("lookup-addresses"));
        Producer producer = new ProducerImplV2(config);
        Topic topic = new Topic(topicName);
        producer.start();
        for(int i = 0; i < 10; i++)
            producer.publish(Message.create(topic, "topic util test"));
        Thread.sleep(1000);
        TopicUtil.emptyQueue(adminHttp, topicName, "default");
        //consuemr should receive nothing
        final CountDownLatch latch = new CountDownLatch(1);
        Consumer consumer = new ConsumerImplV2(config, new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                logger.error("should not receive message after channel empty");
                latch.countDown();
            }
        });
        consumer.start();
        Assert.assertFalse(latch.await(30, TimeUnit.SECONDS));
        consumer.close();

        TopicUtil.deleteTopicChannel(adminHttp, topicName, "default");
        //request should return catch exception
        URL channelUrl = new URL(adminHttp + "/api/topics/" + topicName + "/" + "default");
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(channelUrl);
        Iterator<String> ite = root.fieldNames();
        int i = 0;
        while(ite.hasNext()){
            i++;
            ite.next();
        }
        Assert.assertEquals(1, i);
        TopicUtil.deleteTopic(adminHttp, topicName);
        URL topicUrl = new URL(adminHttp + "/api/topics/" + topicName);
        try {
            root = mapper.readTree(topicUrl);
            Assert.fail("topic should exist: " + topicName);
        } catch(IOException ioe) {

        }
        producer.close();
        logger.info("[testTopicUtil] ends.");
    }
}
