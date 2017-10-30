package it.youzan.nsq.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.youzan.nsq.client.*;
import com.youzan.nsq.client.core.NSQConnection;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.DesiredTag;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.exception.NSQNoConnectionException;
import com.youzan.nsq.client.utils.ConnectionUtil;
import com.youzan.nsq.client.utils.TopicUtil;
import com.youzan.util.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by lin on 17/8/17.
 */
public class ITExtUpgradeTest {
    private static final Logger logger = LoggerFactory.getLogger(AbstractITConsumer.class);

    protected final NSQConfig config = new NSQConfig();
    protected Consumer consumer;
    private String adminHttp;
    private String lookupAddr;

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

        lookupAddr = lookups;
        adminHttp = "http://" + props.getProperty("admin-address");
        config.setLookupAddresses(lookups);
        config.setConnectTimeoutInMillisecond(Integer.valueOf(connTimeout));
        config.setMsgTimeoutInMillisecond(Integer.valueOf(msgTimeoutInMillisecond));
        config.setConsumerName("BaseConsumer");
    }

    @Test
    public void testPublishAndConsumeWhileUpgrade() throws Exception {
        if(lookupAddr.contains("qa")) {
            logger.info("testPublishAndConsumeWhileUpgrade not validated in QA.");
            return;
        }
        final String topicName = "textExtUpgrade_" + System.currentTimeMillis();
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        final Producer producer = new ProducerImplV2(config);
        Consumer consumer = null;
        try {
            //topic with ext false
            TopicUtil.createTopic(adminHttp, topicName, "BaseConsumer");
            TopicUtil.createTopicChannel(adminHttp, topicName, "BaseConsumer");

            //publish starts
            final AtomicLong success = new AtomicLong(0);
            final AtomicLong fail = new AtomicLong(0);
            producer.start(topicName);
            exec.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    try {
                        producer.publish("message keep sending.", topicName);
                        success.incrementAndGet();
                    } catch (NSQException e) {
                        logger.error("fail to send message.", e);
                        fail.incrementAndGet();
                    }
                }
            }, 0, 100, TimeUnit.MILLISECONDS);
            final AtomicLong consumeCnt = new AtomicLong(0);
            final AtomicLong consumeExtCnt = new AtomicLong(0);
            consumer = new ConsumerImplV2(config, new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    logger.info(message.getReadableContent());
                    consumeCnt.incrementAndGet();
                    if (message.getTopicInfo().isExt()) {
                        consumeExtCnt.incrementAndGet();
                    }
                }
            });
            consumer.subscribe(topicName);
            consumer.start();
            //sleep 20 sec to pub/consume normal topic
            Thread.sleep(20000);
            JsonNode lookupResp = null;
            lookupResp = IOUtil.readFromUrl(new URL("http://" + lookupAddr + "/lookup?topic=" + topicName + "&access=r&metainfo=true"));
            JsonNode partition = lookupResp.get("partitions").get("0");
            Address addr = new Address(partition.get("broadcast_address").asText(), partition.get("tcp_port").asText(), partition.get("version").asText(), topicName, 0, false);
            NSQConnection mConn = ConnectionUtil.connect(addr, "BaseConsumer", config);
            Assert.assertTrue(mConn.isConnected());
            mConn.close();

            Assert.assertEquals(fail.get(), 0);
            Assert.assertEquals(consumeExtCnt.get(), 0);

            //then start upgrade
            TopicUtil.upgradeTopic("http://" + lookupAddr, topicName);
            Thread.sleep(3000);
            long pubNormal = success.get();
            long conNormal = consumeCnt.get();
            //then wait for recover
            Thread.sleep(50000);

            boolean extSupport = false;
            int i=0;
            while(i++ < 3) {
                lookupResp = IOUtil.readFromUrl(new URL("http://" + lookupAddr + "/lookup?topic=" + topicName + "&access=r&metainfo=true"));
                extSupport = lookupResp.get("meta").get("extend_support").asBoolean();
                if(extSupport) {
                    break;
                } else {
                    Thread.sleep(10000);
                }
            }
            Assert.assertTrue(extSupport);

            addr = new Address(partition.get("broadcast_address").asText(), partition.get("tcp_port").asText(), partition.get("version").asText(), topicName, 0, false);
            try {
            mConn = ConnectionUtil.connect(addr, "BaseConsumer", config);
//            Assert.fail("connection should not connect.");
             } catch (NSQNoConnectionException | IllegalStateException e) {
                logger.error("intend failure to connect topic without ext support.", e);
            }
            mConn.close();

            //connect with ext true
            addr = new Address(partition.get("broadcast_address").asText(), partition.get("tcp_port").asText(), partition.get("version").asText(), topicName, 0, true);
            mConn = ConnectionUtil.connect(addr, "BaseConsumer", config);
            Assert.assertTrue(mConn.isConnected());
            mConn.close();

            Assert.assertTrue(success.get() > pubNormal);
            Assert.assertTrue(consumeCnt.get() > conNormal);
            Assert.assertTrue(consumeExtCnt.get() > 0);

        } finally {
            exec.shutdown();
            Thread.sleep(10000);
            producer.close();
            if (null != consumer) {
                consumer.close();
            }

            TopicUtil.deleteTopic(adminHttp, topicName);
        }
    }

    @Test
    public void testSubWTagToNormalTopic() throws Exception {
        final String topicName = "testSubWTagToNormalTopic";
        final Producer producer = new ProducerImplV2(config);
        Consumer consumer = null;
        try {
            //topic with ext false
            TopicUtil.createTopic(adminHttp, topicName, "BaseConsumer");
            TopicUtil.createTopicChannel(adminHttp, topicName, "BaseConsumer");

            producer.start(topicName);
            producer.publish("this is a message does not belong to new topic, but old", topicName);
            config.setConsumerDesiredTag(new DesiredTag("desiredTag123"));
            final CountDownLatch latch = new CountDownLatch(1);
            consumer = new ConsumerImplV2(config, new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    logger.info(message.getReadableContent());
                    latch.countDown();
                }
            });

            consumer.subscribe(topicName);
            consumer.start();
            Assert.assertTrue(latch.await(20, TimeUnit.SECONDS));
        }finally {
            producer.close();
            consumer.close();
            TopicUtil.deleteTopic(adminHttp, topicName);
        }
    }

}
