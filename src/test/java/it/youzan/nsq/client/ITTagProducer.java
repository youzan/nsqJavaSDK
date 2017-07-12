package it.youzan.nsq.client;

import com.youzan.nsq.client.Producer;
import com.youzan.nsq.client.ProducerImplV2;
import com.youzan.nsq.client.entity.DesiredTag;
import com.youzan.nsq.client.entity.Message;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.utils.TopicUtil;
import com.youzan.util.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by lin on 17/6/12.
 */
public class ITTagProducer {
    private static final Logger logger = LoggerFactory.getLogger(ITTagProducer.class);

    protected final NSQConfig config = new NSQConfig();
    protected Producer producer;
    protected Properties props = new Properties();

    @BeforeClass
    public void init() throws Exception {
        logger.info("At {} , initialize: {}", System.currentTimeMillis(), this.getClass().getName());
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
    public void publishWTag() throws Exception {
        TopicUtil.emptyQueue("http://" + props.getProperty("admin-address"), "testExt2Par2Rep", "BaseConsumer");
        Topic topic = new Topic("testExt2Par2Rep");
        DesiredTag tag = new DesiredTag("TAG1");
        for (int i = 0; i < 100; i++) {
            Message msg = Message.create(topic, "message");
            msg.setDesiredTag(tag);
            producer.publish(msg);
        }

        tag = new DesiredTag("TAG2");
        for (int i = 0; i < 100; i++) {
            Message msg = Message.create(topic, "message");
            msg.setDesiredTag(tag);
            producer.publish(msg);
        }
    }

    @Test
    public void publishWTagMix() throws Exception {
        TopicUtil.emptyQueue("http://" + props.getProperty("admin-address"), "testExt2Par2Rep", "BaseConsumer");
        Topic topic = new Topic("testExt2Par2Rep");
        DesiredTag tag = new DesiredTag("TAG1");
        for (int i = 0; i < 100; i++) {
            Message msg = Message.create(topic, "message");
            msg.setDesiredTag(tag);
            producer.publish(msg);
        }

        for (int i = 0; i < 100; i++) {
            Message msg = Message.create(topic, "message");
            producer.publish(msg);
        }
    }

    @AfterClass
    public void close() {
        IOUtil.closeQuietly(producer);
    }
}
