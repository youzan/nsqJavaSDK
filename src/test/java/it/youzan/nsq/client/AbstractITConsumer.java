package it.youzan.nsq.client;

import com.youzan.nsq.client.Consumer;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.util.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by lin on 16/8/19.
 */
public abstract class AbstractITConsumer {
    private static final Logger logger = LoggerFactory.getLogger(AbstractITConsumer.class);

    protected final int rdy = 2;
    protected final NSQConfig config = new NSQConfig();
    protected Consumer consumer;
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
        adminHttp = props.getProperty("admin-lookup-addresses");
        config.setUserSpecifiedLookupAddress(true);
        config.setLookupAddresses(lookups);
        config.setConnectTimeoutInMillisecond(Integer.valueOf(connTimeout));
        config.setMsgTimeoutInMillisecond(Integer.valueOf(msgTimeoutInMillisecond));
        config.setThreadPoolSize4IO(Integer.valueOf(threadPoolSize4IO));
        config.setRdy(rdy);
        config.setConsumerName("BaseConsumer");
    }

    @AfterClass
    public void close() {
        if(null != consumer) {
            consumer.close();
        }
    }
}
