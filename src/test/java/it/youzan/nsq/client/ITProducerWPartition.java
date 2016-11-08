package it.youzan.nsq.client;

import com.youzan.nsq.client.entity.Message;
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
        Topic topic = new Topic("JavaTesting-Producer-Base");
        for (int i = 0; i < 10; i++) {
            random.nextBytes(message);
            Message msg = Message.create(topic, new String(message))
                    .setTopicShardingID(321L);
            producer.publish(msg);
        }
    }


    public void publishWTopicAndPartition(String topic) throws NSQException {
        for (int i = 0; i < 10; i++) {
            producer.publish(("Message #" + i).getBytes(), new Topic(topic));
        }
    }

    public void testPublishPartition0() throws NSQException {
        publishWTopicAndPartition("JavaTesting-Partition");
    }
}
