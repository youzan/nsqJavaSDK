package it.youzan.nsq.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.youzan.nsq.client.Consumer;
import com.youzan.nsq.client.ConsumerImplV2;
import com.youzan.nsq.client.MessageHandler;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.exception.NSQException;

public class ITConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ITConsumer.class);

    private NSQConfig config;

    @BeforeClass
    public void init() throws NSQException, IOException {
        config = new NSQConfig();
        final Properties props = new Properties();
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }
        final String env = props.getProperty("env");
        final String consumeName = env + "-" + this.getClass().getName();

        config.setLookupAddresses(props.getProperty("lookup-addresses"));
        config.setTopic("test");
        config.setConsumerName(consumeName);
        config.setConnectTimeoutInMillisecond(Integer.valueOf(props.getProperty("connectTimeoutInMillisecond")));
        config.setTimeoutInSecond(Integer.valueOf(props.getProperty("timeoutInSecond")));
        config.setMsgTimeoutInMillisecond(Integer.valueOf(props.getProperty("msgTimeoutInMillisecond")));
        config.setThreadPoolSize4IO(2);
        props.clear();
    }

    @Test
    public void consume() throws NSQException {
        final AtomicInteger total = new AtomicInteger(0);
        final MessageHandler handler = new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                total.incrementAndGet();
            }
        };
        final Consumer consumer = new ConsumerImplV2(config, handler);
        consumer.start();
        sleep(3 * 60 * 1000);
        consumer.close();
        logger.info("It has {} messages received.", total.get());
    }

    void sleep(final long millisecond) {
        logger.debug("Sleep {} millisecond.", millisecond);
        try {
            Thread.sleep(millisecond);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Your machine is too busy! Please check it!");
        }
    }
}
