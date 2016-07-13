package it.youzan.nsq.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.youzan.nsq.client.Producer;
import com.youzan.nsq.client.ProducerImplV2;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;

public class ITProducer {

    private final Random random = new Random();
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
        config.setConnectTimeoutInMillisecond(Integer.valueOf(props.getProperty("connectTimeoutInMillisecond")));
        config.setTimeoutInSecond(Integer.valueOf(props.getProperty("timeoutInSecond")));
        config.setMsgTimeoutInMillisecond(Integer.valueOf(props.getProperty("msgTimeoutInMillisecond")));
        config.setThreadPoolSize4IO(2);
        props.clear();
    }

    @Test
    public void produce() throws NSQException {
        final Producer p = new ProducerImplV2(config);
        p.start();
        final byte[] message = new byte[1024];
        random.nextBytes(message);
        p.publish(message, "test");
        p.close();
    }
}
