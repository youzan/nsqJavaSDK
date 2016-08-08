package it.youzan.nsq.client;

import com.youzan.nsq.client.*;
import com.youzan.nsq.client.core.lookup.LookupServiceImpl;
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

/**
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
@Test
public class ITComplexConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ITComplexConsumer.class);

    private final String consumerName = "BaseConsumer";

    private final Random _r = new Random();
    private final NSQConfig config = new NSQConfig();
    private Producer producer;
    private Consumer consumer4ReQueue;
    private Consumer consumer4Finish;
    private HashSet<byte[]> messages4ReQueue = new HashSet<>();
    private HashSet<byte[]> messages4Finish = new HashSet<>();


    @BeforeClass
    public void init() throws Exception {
        logger.info("Now initialize {} at {} .", this.getClass().getName(), System.currentTimeMillis());
        final Properties props = new Properties();
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }

        final String lookups = props.getProperty("lookup-addresses");
        final String connTimeout = props.getProperty("connectTimeoutInMillisecond");
        final String admin = props.getProperty("admin-address");
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

            CloseableHttpClient httpclient = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost(url);
            httpPost.setConfig(requestConfig);
            httpPost.setEntity(entity);
            CloseableHttpResponse response = httpclient.execute(httpPost);
            response.close();
        }
        // create new instances
        producer = new ProducerImplV2(config);
        producer.start();

    }

    @Test(groups = "Finish")
    public void produceFinish() throws NSQException {
        for (int i = 0; i < 10; i++) {
            final byte[] message = new byte[32];
            _r.nextBytes(message);
            producer.publish(message, "JavaTesting-Finish");
            messages4Finish.add(message);
        }
    }

    @Test(dependsOnGroups = "Finish")
    public void testFinish() throws InterruptedException, NSQException {
        final CountDownLatch latch = new CountDownLatch(10);
        final HashSet<byte[]> actualMessages = new HashSet<>();
        final List<NSQMessage> messages = new ArrayList<>();
        final MessageHandler handler = new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                latch.countDown();
                actualMessages.add(message.getMessageBody());
                messages.add(message);
            }
        };
        final NSQConfig config = (NSQConfig) this.config.clone();
        config.setRdy(2);
        config.setConsumerName(consumerName);
        config.setThreadPoolSize4IO(Math.max(2, Runtime.getRuntime().availableProcessors()));
        consumer4Finish = new ConsumerImplV2(config, handler);
        consumer4Finish.setAutoFinish(false);
        consumer4Finish.start();
        latch.await(2, TimeUnit.MINUTES);
        for (NSQMessage m : messages) {
            consumer4Finish.finish(m);
        }
        Assert.assertEquals(actualMessages, messages4Finish);
    }

    //    @Test(groups = "ReQueue")
    public void produceReQueue() throws NSQException {
        for (int i = 0; i < 10; i++) {
            final byte[] message = new byte[32];
            _r.nextBytes(message);
            producer.publish(message, "JavaTesting-ReQueue");
            messages4ReQueue.add(message);
        }
    }

    //    @Test(dependsOnGroups = "ReQueue")
    public void testReQueue() throws InterruptedException, NSQException {
        final CountDownLatch latch = new CountDownLatch(10);
        final HashSet<byte[]> actualMessages = new HashSet<>();
        final List<NSQMessage> messages = new ArrayList<>();
        final MessageHandler handler = new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                latch.countDown();
                actualMessages.add(message.getMessageBody());
                messages.add(message);
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
//        consumer4ReQueue.start();
        latch.await(2, TimeUnit.MINUTES);
        for (NSQMessage m : messages) {
            consumer4ReQueue.finish(m);
        }
        Assert.assertEquals(actualMessages, consumer4ReQueue);
    }

    @AfterClass
    public void close() {
        IOUtil.closeQuietly(producer, consumer4ReQueue, consumer4Finish);
    }
}
