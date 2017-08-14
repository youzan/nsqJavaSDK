package it.youzan.nsq.client;

import com.youzan.nsq.client.Producer;
import com.youzan.nsq.client.ProducerImplV2;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.util.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
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
    protected Producer producer;

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
        config.setLookupAddresses(lookups);
        config.setConnectTimeoutInMillisecond(Integer.valueOf(connTimeout));
        config.setMsgTimeoutInMillisecond(Integer.valueOf(msgTimeoutInMillisecond));
        config.setThreadPoolSize4IO(Integer.valueOf(threadPoolSize4IO));

        producer = new ProducerImplV2(config);
        producer.start();
    }

    @Test
    public void publish() throws NSQException {
        String msgStr = "The quick brown fox jumps over the lazy dog, 那只迅捷的灰狐狸跳过了那条懒狗";
        for (int i = 0; i < 10; i++) {
            final byte[] message = (msgStr + " #" + i).getBytes();
            producer.publish(message, "JavaTesting-Producer-Base");
        }
    }

    @Test
    public void publishSnappy() throws NSQException {
        config.setCompression(NSQConfig.Compression.SNAPPY);
        Producer producer = new ProducerImplV2(config);
        producer.start();
        String msgStr = "The quick brown fox jumps over the lazy dog, 那只迅捷的灰狐狸跳过了那条懒狗";
        for (int i = 0; i < 10; i++) {
            final byte[] message = (msgStr + " #" + i).getBytes();
            producer.publish(message, "JavaTesting-Producer-Base");
        }
        producer.close();
    }

    @Test
    public void publishDeflate() throws NSQException {
        config.setCompression(NSQConfig.Compression.DEFLATE);
        config.setDeflateLevel(3);
        Producer producer = new ProducerImplV2(config);
        producer.start();
        String msgStr = "The quick brown fox jumps over the lazy dog, 那只迅捷的灰狐狸跳过了那条懒狗";
        for (int i = 0; i < 10; i++) {
            final byte[] message = (msgStr + " #" + i).getBytes();
            producer.publish(message, "JavaTesting-Producer-Base");
        }
        producer.close();
    }

//    public void concurrentPublish() throws NSQException, InterruptedException {
//        final ExecutorService exec = Executors.newFixedThreadPool(100);
//        config.setConnectionPoolSize(200);
//        config.setThreadPoolSize4IO(Runtime.getRuntime().availableProcessors() * 2);
//        final Producer proCon = new ProducerImplV2(config);
//        proCon.start();
//        final Topic topic = new Topic("JavaTesting-Producer-Base");
//        ((ProducerImplV2) proCon).preAllocateNSQConnection(topic, 100);
//        while(true) {
//            exec.submit(new Runnable() {
//                @Override
//                public void run() {
//                    Message msg = Message.create(topic, "message");
//                    try {
//                        proCon.publish(msg);
//                    } catch (NSQException e) {
//                        logger.error("publish fail.", e);
//                    }
//                }
//            });
//            Thread.sleep(100L);
//        }
//    }

    @AfterClass
    public void close() {
        IOUtil.closeQuietly(producer);
    }
}
