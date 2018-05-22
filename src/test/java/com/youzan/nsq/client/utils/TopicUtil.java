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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
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
        URL url = new URL(nsqadminUrl + "/topics");
        JsonNode root = SystemUtil.getObjectMapper().readTree(url);
        JsonNode topics = root.get("data").get("topics");
        ExecutorService exec = Executors.newFixedThreadPool(4);
        List<String> topicsNeedDel = new ArrayList<>();
        if(topics.isArray()) {
            for(JsonNode topicNode : topics) {
                String topicName = topicNode.asText();
                if(topicName.startsWith(topicPattern)) {
                    topicsNeedDel.add(topicName);
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

    private static void loopInPartitions(String lookupUrlStr, String topic, String channel, String requestUrlPattern) throws IOException, InterruptedException {
        JsonNode lookupRoot = lookupProducers(lookupUrlStr, topic);
        JsonNode partitionsRoot = lookupRoot.get("partitions");
        Iterator<Map.Entry<String, JsonNode>> ite =  partitionsRoot.fields();
        while(ite.hasNext()) {
            Map.Entry<String, JsonNode> partition = ite.next();
            String partitionHttp = String.format(requestUrlPattern, partition.getValue().get("broadcast_address").asText(), partition.getValue().get("http_port").asInt(),
                    topic, channel, partition.getKey());
            URL url = new URL(partitionHttp);
            logger.debug("Prepare to open HTTP Connection...");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setDoOutput(true);
            con.setRequestMethod("POST");
            con.setRequestProperty("Accept", "application/vnd.nsq; version=1.0");
            con.setConnectTimeout(5 * 1000);
            con.setReadTimeout(10 * 1000);
            //con.getOutputStream().write(contentStr.getBytes());
            InputStream is = con.getInputStream();
            is.close();
            if (logger.isDebugEnabled()) {
                logger.debug("Request to {} responses {}:{}.", url.toString(), con.getResponseCode(), con.getResponseMessage());
            }
        }
        Thread.sleep(10000);
    }

    public static void emptyQueue(String lookupUrlStr, String topic, String channel) throws Exception {
        loopInPartitions(lookupUrlStr, topic, channel, "http://%s:%d/channel/empty?topic=%s&channel=%s&partition=%s");
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

    public static void createTopic(String adminUrl, String topicName, String channel) throws Exception {
        createTopic(adminUrl, topicName, 2, 2, channel, false, false);
    }

    public static void createTopic(String adminUrl, String topicName, int parNum, int repNum, String channel) throws Exception {
        createTopic(adminUrl, topicName, parNum, repNum, channel, false,  false);
    }

    public static void createTopic(String adminUrl, String topicName, int parNum, int repNum, String channel, boolean ordered, boolean ext) throws Exception {
        String lookupLeaderAddr = lookupLeaderAddr(adminUrl);
        String urlStr = String.format("%s/topic/create?topic=%s&partition_num=%d&replicator=%d&orderedmulti=%b&extend=%b", lookupLeaderAddr, topicName, parNum, repNum, ordered, ext);
        URL url = new URL(urlStr);
        IOUtil.postToUrl(url, null);
        Thread.sleep(20000);
        createTopicChannel(adminUrl, topicName, channel);
    }

    public static void createTopicChannel(String lookupUrl, String topicName, String channel) throws IOException, InterruptedException {
        loopInPartitions(lookupUrl, topicName, channel, "http://%s:%d/channel/create?topic=%s&channel=%s&partition=%s");
    }

    private static JsonNode lookupProducers(String lookupUrlStr, String topicName) throws IOException {
        String urlStr = String.format("%s/lookup?topic=%s", lookupUrlStr, topicName);
        URL lookupUrl = new URL(urlStr);
        return IOUtil.readFromUrl(lookupUrl);
    }

    private static String lookupLeaderAddr(String lookupUrlStr) throws IOException {
        ObjectMapper om = SystemUtil.getObjectMapper();
        String urlStr = String.format("%s/listlookup", lookupUrlStr);
        URL lookupUrl = new URL(urlStr);
        JsonNode leaderNode = IOUtil.readFromUrl(lookupUrl).get("lookupdleader");
        String ip = leaderNode.get("NodeIP").asText();
        String httpPort = leaderNode.get("HttpPort").asText();
        return "http://" + ip + ":" + httpPort;
    }

    public static void deleteTopic(String lookupUrl, String topicName) throws Exception {
        String lookupLeaderAddr = lookupLeaderAddr(lookupUrl);
        String topicDeleteUrlStr = String.format("%s/topic/delete?topic=%s&partition=**", lookupLeaderAddr, topicName);
        URL topicDeleteUrl = new URL(topicDeleteUrlStr);
        IOUtil.postToUrl(topicDeleteUrl, null);
    }

    public static void deleteTopicChannel(String lookupUrlStr, String topicName, String channel) throws Exception {
        loopInPartitions(lookupUrlStr, topicName, channel, "http://%s:%d/channel/delete?topic=%s&channel=%s&partition=%s");
    }

    public static void upgradeTopic(String lookupdAddr, String topicName) throws Exception {
        String lookupLeaderAddr = lookupLeaderAddr(lookupdAddr);
        URL upgradeUrl = new URL(lookupLeaderAddr + "/topic/meta/update?topic=" + topicName + "&upgradeext=true");
        IOUtil.postToUrl(upgradeUrl, null);
    }


    /**
     * remove @Test comment and run as test cases to remove topics in qa
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void deleteTopics() throws IOException, InterruptedException {
        logger.info("[testDeleteTopics] start.");
        Properties props = new Properties();
        logger.info("At {} , initialize: {}", System.currentTimeMillis(), this.getClass().getName());
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }
        String adminHttp = "http://" + props.getProperty("admin-lookup-addresses");
        String topicPattern = "topic_";
        TopicUtil.deleteTopics(adminHttp, topicPattern);
        logger.info("[testTopicUtil] ends.");
    }

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
        String lookupHttp = "http://" + props.getProperty("admin-lookup-addresses");
        String topicName = "topicUtilTest";
        TopicUtil.createTopic(lookupHttp, topicName, "default");
        //publish to channel
        NSQConfig config = new NSQConfig("default");
        config.setLookupAddresses(props.getProperty("lookup-addresses"));
        Producer producer = new ProducerImplV2(config);
        Topic topic = new Topic(topicName);
        producer.start();
        for(int i = 0; i < 10; i++)
            producer.publish(Message.create(topic, "topic util test"));
        Thread.sleep(1000);
        TopicUtil.emptyQueue(lookupHttp, topicName, "default");
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

        TopicUtil.deleteTopicChannel(lookupHttp, topicName, "default");
        //request should return catch exception
        URL channelUrl = new URL(lookupHttp + "/api/topics/" + topicName + "/" + "default");
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = null;
        try {
            root = mapper.readTree(channelUrl);
            Assert.fail("topic channel should not exist: " + topicName);
        }catch (IOException ioe) {

        }
//        Iterator<String> ite = root.fieldNames();
//        int i = 0;
//        while(ite.hasNext()){
//            i++;
//            ite.next();
//        }
//        Assert.assertEquals(1, i);
        TopicUtil.deleteTopic(lookupHttp, topicName);
        URL topicUrl = new URL(lookupHttp + "/api/topics/" + topicName);
        try {
            root = mapper.readTree(topicUrl);
            Assert.fail("topic should not exist: " + topicName);
        } catch(IOException ioe) {

        }
        producer.close();
        logger.info("[testTopicUtil] ends.");
    }

    public static String findLookupLeader(String lookupUrlStr) throws IOException {
        URL lookupUrl = new URL(lookupUrlStr + "/listlookup");
        JsonNode root = IOUtil.readFromUrl(lookupUrl);
        if(null != root) {
            JsonNode leader = root.get("lookupdleader");
            String ip = leader.get("NodeIP").asText();
            String port = leader.get("HttpPort").asText();
            return ip + ":" + port;
        } else {
            return null;
        }
    }
}
