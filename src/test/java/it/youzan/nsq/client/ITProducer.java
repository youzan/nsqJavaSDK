package it.youzan.nsq.client;

import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.youzan.nsq.client.Producer;
import com.youzan.nsq.client.ProducerImplV2;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;

@Test(groups = "ITProducer")
public class ITProducer {

    private static final Logger logger = LoggerFactory.getLogger(ITProducer.class);

    private final Random random = new Random();
    private final NSQConfig config = new NSQConfig();
    private Producer producer;

    @BeforeClass
    public void init() throws Exception {
        logger.info("Now initialize {} at {} .", this.getClass().getName(), System.currentTimeMillis());
        final Properties props = new Properties();
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }
        final String env = props.getProperty("env");
        final String lookups = props.getProperty("lookup-addresses");
        final String connTimeout = props.getProperty("connectTimeoutInMillisecond");
        final String msgTimeoutInMillisecond = props.getProperty("msgTimeoutInMillisecond");
        final String threadPoolSize4IO = props.getProperty("threadPoolSize4IO");


        logger.debug("The environment is {} .", env);
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
}
