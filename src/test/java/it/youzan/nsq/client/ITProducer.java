package it.youzan.nsq.client;

import com.youzan.nsq.client.Producer;
import com.youzan.nsq.client.ProducerImplV2;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.utils.TopicUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

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
}
