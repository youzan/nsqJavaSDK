package it.youzan.nsq.client;

import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

/**
 * Created by lin on 16/8/19.
 */
@Test(groups = {"ITProducerWPartition-Base"}, priority = 3)
public class ITProducerWPartition extends ITProducer{
    private static final Logger logger = LoggerFactory.getLogger(ITProducerWPartition.class);


    public void publish() throws NSQException {
        final byte[] message = new byte[64];
        for (int i = 0; i < 10; i++) {
            random.nextBytes(message);
            producer.publish(message, new Topic("JavaTesting-Producer-Base", 0));
        }
    }


    public void publishWTopicAndPartition(String topic, int partition) throws NSQException {
        final byte[] message = new byte[64];
        for (int i = 0; i < 10; i++) {
            random.nextBytes(message);
            producer.publish(message, new Topic(topic, 0));
        }
    }

    public void testPublishPartition0() throws NSQException {
        publishWTopicAndPartition("JavaTesting-Finish", 0);
    }
}
