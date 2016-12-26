package it.youzan.nsq.client;

import com.youzan.nsq.client.*;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.util.IOUtil;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public class ITComplexConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ITComplexConsumer.class);

    private final String consumerName = "BaseConsumer";

    private final AtomicInteger counter = new AtomicInteger(0);
    private final Random _r = new Random();
    private final NSQConfig config = new NSQConfig();
    private Producer producer;
    private Consumer consumer4ReQueue;
    private Consumer consumer4Finish;
    private HashSet<String> messages4Finish = new HashSet<>();


    @BeforeClass
    public void init() throws Exception {
        logger.info("At {} , initialize: {}", System.currentTimeMillis(), this.getClass().getName());
        final Properties props = new Properties();
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }

        final String lookups = props.getProperty("lookup-addresses");
        final String connTimeout = props.getProperty("connectTimeoutInMillisecond");
        final String admin = props.getProperty("admin-address");
        config.setUserSpecifiedLookupAddress(true);
        config.setLookupAddresses(lookups);
        config.setConnectTimeoutInMillisecond(Integer.valueOf(connTimeout));
        config.setThreadPoolSize4IO(Runtime.getRuntime().availableProcessors() * 2);

        // empty channel
        // curl -X POST http://127.0.0.1:4151/channel/empty?topic=name&channel=name
        // curl -X /api/topics/:topic/:channel    body: {"action":"empty"}
        String[] topics = new String[]{"JavaTesting-Finish", "JavaTesting-ReQueue"};
        for (String t : topics) {
            final String url = String.format("http://%s/api/topics/%s/%s", admin, t, consumerName);
            logger.debug("Empty channel {}", url);

            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(5 * 1000)
                    .setConnectionRequestTimeout(10 * 1000)
                    .build();
            HttpEntity entity = new StringEntity("{\"action\":\"empty\"}");

            final CloseableHttpClient httpclient = HttpClients.createDefault();
            final HttpPost httpPost = new HttpPost(url);
            httpPost.setConfig(requestConfig);
            httpPost.setEntity(entity);
            final CloseableHttpResponse response = httpclient.execute(httpPost);
            response.close();
        }
        // create new instances
        producer = new ProducerImplV2(config);
        producer.start();
    }

    @Test(priority = 9, groups = {"Finish"})
    public void produceFinish() throws NSQException {
        for (int i = 0; i < 10; i++) {
            String message = newMessage();
            producer.publish(message, "JavaTesting-Finish");
            messages4Finish.add(message);
        }
        logger.debug("======messages4Finish:{} ", messages4Finish);
    }

    @Test(priority = 10, dependsOnGroups = {"Finish"})
    public void consumeFinish() throws InterruptedException, NSQException {
        final int count = 10;
        final CountDownLatch latch = new CountDownLatch(count);
        final HashSet<String> actualMessages = new HashSet<>();
        final List<NSQMessage> actualNSQMessages = new ArrayList<>();
        final MessageHandler handler = new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                logger.debug("======================Be pushed.");
                logger.debug("======================From server {} , {}, Binary: {}", message.getReadableContent(), message.getMessageBody(), message);
                actualMessages.add(message.getReadableContent());
                actualNSQMessages.add(message);
                // finally
                latch.countDown();
            }
        };
        final NSQConfig config = (NSQConfig) this.config.clone();
        config.setMsgTimeoutInMillisecond(5 * 60 * 1000);
        config.setRdy(20);
        config.setConsumerSlowStart(false);
        config.setConsumerName(consumerName);
        config.setThreadPoolSize4IO(1);
        consumer4Finish = new ConsumerImplV2(config, handler);
        consumer4Finish.setAutoFinish(false);
        consumer4Finish.subscribe("JavaTesting-Finish");
        consumer4Finish.start();
        logger.debug("=======================start consumer.... topic : JavaTesting-Finish");
        boolean full = latch.await(2 * 60L, TimeUnit.SECONDS);
        final List<NSQMessage> received = new ArrayList<>(actualNSQMessages);
        logger.debug("=======================received: {}", received.size());
        for (NSQMessage m : received) {
            consumer4Finish.finish(m);
            logger.debug("=========================Finish one: {}", m.newHexString(m.getMessageID()));
        }
        logger.debug("======messages4Finish:{} ", messages4Finish);
        if (full) {
            Assert.assertEquals(actualNSQMessages.size(), count);
            Assert.assertEquals(actualMessages, messages4Finish);
        } else {
            Assert.assertTrue(false, "Not have got enough messages.");
        }
    }

    @Test(priority = 9, groups = {"ReQueue"})
    public void produceReQueue() throws NSQException {
        for (int i = 0; i < 10; i++) {
            final byte[] message = new byte[32];
            _r.nextBytes(message);
            producer.publish(message, "JavaTesting-ReQueue");
        }
    }

    @Test(priority = 10, dependsOnGroups = {"ReQueue"})
    public void consumeReQueue() throws InterruptedException, NSQException {
        final CountDownLatch latch = new CountDownLatch(10);
        final MessageHandler handler = new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                latch.countDown();
                try {
                    message.setNextConsumingInSecond(120);
                } catch (NSQException e) {
                    logger.error("Exception", e);
                }
            }
        };
        final NSQConfig config = (NSQConfig) this.config.clone();
        config.setRdy(4);
        config.setConsumerName(consumerName);
        config.setThreadPoolSize4IO(Math.max(2, Runtime.getRuntime().availableProcessors()));
        consumer4ReQueue = new ConsumerImplV2(config, handler);
        consumer4ReQueue.subscribe("JavaTesting-ReQueue");
        consumer4ReQueue.start();
        latch.await(1, TimeUnit.MINUTES);
    }


    private String newMessage() {
        return "String Message: " + counter.getAndIncrement() + " . At: " + System.currentTimeMillis();
    }

    @AfterClass
    public void close() {
        logger.debug("================Begin to close");
        IOUtil.closeQuietly(producer, consumer4ReQueue, consumer4Finish);
    }
}
