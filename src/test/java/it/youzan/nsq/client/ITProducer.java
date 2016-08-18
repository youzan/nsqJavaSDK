package it.youzan.nsq.client;

import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

import com.youzan.nsq.client.entity.TraceInfo;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.util.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.youzan.nsq.client.Producer;
import com.youzan.nsq.client.ProducerImplV2;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;

@Test(groups = {"ITProducer-Base"}, priority = 3)
public class ITProducer {

    private static final Logger logger = LoggerFactory.getLogger(ITProducer.class);

    protected final Random random = new Random();
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

    public void publish() throws NSQException {
        final byte[] message = new byte[64];
        for (int i = 0; i < 10; i++) {
        random.nextBytes(message);
            producer.publish(message, "JavaTesting-Producer-Base");
        }
    }

    public void tracePublish() throws NSQException {
        final byte[] message = new byte[64];
        for (int i = 0; i < 10; i++) {
            random.nextBytes(message);
            producer.setTraceID(1l);
            producer.publish(message, new Topic("JavaTesting-Producer-Base", 0));
//            logger.info(info.toString());
        }
    }

    @AfterClass
    public void close() {
        IOUtil.closeQuietly(producer);
    }
}
