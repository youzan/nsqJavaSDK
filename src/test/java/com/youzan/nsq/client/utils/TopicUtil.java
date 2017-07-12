package com.youzan.nsq.client.utils;

import com.youzan.util.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by lin on 17/7/11.
 */
public class TopicUtil {
    private static final Logger logger = LoggerFactory.getLogger(TopicUtil.class);

    public static void emptyQueue(String nsqadminUrl, String topic, String channel) throws Exception {
        URL channelUrl = new URL(nsqadminUrl + "/api/topics/" + topic + "/" + channel);
        String content = "{\"action\":\"empty\"}";
        IOUtil.postToUrl(channelUrl, content);
    }

    public static void createTopic(String adminUrl, String topicName, String channel) throws IOException, InterruptedException {
        String urlStr = String.format("%s/api/topics", adminUrl);
        URL url = new URL(urlStr);
        String contentStr = String.format("{\"topic\":\"%s\",\"partition_num\":\"2\", \"replicator\":\"1\", \"retention_days\":\"\", \"syncdisk\":\"\", \"channel\":\"%s\"}", topicName, channel);
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

    public static void deleteTopicChannel(String adminUrl, String topicName, String channel) throws Exception {
        URL channelUrl = new URL(adminUrl + "/api/topics/" + topicName + "/" + channel);
        String content = "{\"action\":\"delete\"}";
        IOUtil.deleteToUrl(channelUrl, content);
    }
}
