package it.youzan.nsq.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.youzan.nsq.client.Producer;
import com.youzan.nsq.client.ProducerImplV2;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;

@Test(groups = "ITProducer")
public class ITProducer {

    private static final Logger logger = LoggerFactory.getLogger(ITProducer.class);

    private final Random random = new Random();
    private NSQConfig config;

    @BeforeClass
    public void init() throws NSQException, IOException {
        logger.info("Now init {} at {} .", this.getClass().getName(), System.currentTimeMillis());
        config = new NSQConfig();
        final Properties props = new Properties();
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }
        final String env = props.getProperty("env");
        final String consumeName = env + "-" + this.getClass().getName();

        config.setLookupAddresses(props.getProperty("lookup-addresses"));
        config.setTopic("test");
        config.setConnectTimeoutInMillisecond(Integer.valueOf(props.getProperty("connectTimeoutInMillisecond")));
        config.setTimeoutInSecond(Integer.valueOf(props.getProperty("timeoutInSecond")));
        config.setMsgTimeoutInMillisecond(Integer.valueOf(props.getProperty("msgTimeoutInMillisecond")));
        config.setThreadPoolSize4IO(2);
        props.clear();
    }

    @DataProvider(name = "topics", parallel = false)
    public Object[][] createData() {
        return new Object[][] { { "test" }, { "test_finish" }, { "test_reQueue" } };
    }

    @Test(dataProvider = "topics")
    public void produce(String topic) throws NSQException {
        try (final Producer p = new ProducerImplV2(config);) {
            p.start();
            final byte[] message = new byte[1024];
            random.nextBytes(message);
            p.publish(message);
            for (int i = 0; i < 2; i++) {
                random.nextBytes(message);
                p.publish(message, topic);
            }
        }
    }
}
