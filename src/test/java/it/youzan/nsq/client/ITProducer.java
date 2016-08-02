package it.youzan.nsq.client;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.youzan.nsq.client.Producer;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;

@Test(groups = "ITProducer")
public class ITProducer {

    private static final Logger logger = LoggerFactory.getLogger(ITProducer.class);

    private final Random random = new Random();
    private NSQConfig config;
    private Producer producer;

    @BeforeClass
    public void init() throws NSQException, Exception {
        logger.info("Now init {} at {} .", this.getClass().getName(), System.currentTimeMillis());
    }

    public void publish() throws NSQException {
        // final byte[] message = new byte[64];
        // random.nextBytes(message);
        // producer.publish(message, "JavaTesting");
    }
}
