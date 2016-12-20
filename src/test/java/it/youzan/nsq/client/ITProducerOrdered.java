package it.youzan.nsq.client;

import com.youzan.nsq.client.Producer;
import com.youzan.nsq.client.ProducerImplV2;
import com.youzan.nsq.client.entity.Message;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

/**
 * Created by lin on 16/10/19.
 */
@Test(groups = {"ITProducerOrdered"}, priority = 5)
public class ITProducerOrdered {

    private static final Logger logger = LoggerFactory.getLogger(ITProducerOrdered.class);
    private NSQConfig config = new NSQConfig();
    private Producer producer;
    final Random random = new Random();

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
        config.setUserSpecifiedLookupAddress(true);
        config.setConnectTimeoutInMillisecond(Integer.valueOf(connTimeout));
        config.setMsgTimeoutInMillisecond(Integer.valueOf(msgTimeoutInMillisecond));
        config.setThreadPoolSize4IO(Integer.valueOf(threadPoolSize4IO));
        //turn on pub ordered
        config.setOrdered(true);
        producer = new ProducerImplV2(config);
        producer.start();
    }

    public void publishOrdered() throws NSQException {
        Topic topic = new Topic("JavaTesting-Order");
        for (int i = 0; i < 100; i++) {
            String message = ("Message #" + i);
            Message msg = Message.create(new Topic("JavaTesting-Order"), 1024L, message)
                    .setTopicShardingID(123L);
            producer.publish(msg);
        }
    }
}
