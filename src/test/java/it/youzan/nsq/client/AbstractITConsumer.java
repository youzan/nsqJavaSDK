package it.youzan.nsq.client;

import com.youzan.nsq.client.Consumer;
import com.youzan.nsq.client.ConsumerImplV2;
import com.youzan.nsq.client.MessageHandler;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.util.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lin on 16/8/19.
 */
public abstract class AbstractITConsumer {
    private static final Logger logger = LoggerFactory.getLogger(AbstractITConsumer.class);

    protected final int rdy = 2;
    protected final NSQConfig config = new NSQConfig();
    protected Consumer consumer;

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
        config.setRdy(rdy);
        config.setConsumerName("BaseConsumer");
    }

    @AfterClass
    public void close() {
        logger.info("Consumer closed.");
        IOUtil.closeQuietly(consumer);
    }
}
