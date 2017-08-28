package it.youzan.nsq.client;

import com.youzan.nsq.client.Producer;
import com.youzan.nsq.client.ProducerImplV2;
import com.youzan.nsq.client.entity.Message;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.utils.TopicUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

/**
 * Created by lin on 16/8/19.
 */
@Test(groups = {"ITProducerWPartition-Base"}, priority = 3)
public class ITProducerWPartition extends ITProducer{
    private static final Logger logger = LoggerFactory.getLogger(ITProducerWPartition.class);


    public void publish() throws Exception {
        final byte[] message = new byte[64];
        Topic topic = new Topic("JavaTesting-Producer-Base");

        String[] lookupds = config.getLookupAddresses();
        if(config.getUserSpecifiedLookupAddress() && null != lookupds && lookupds[0].contains("nsq-"))
            return;
        logger.info("[ITProducerWPartition#publish] starts");
        Producer producer = new ProducerImplV2(this.config);
        try {
            TopicUtil.emptyQueue(this.adminHttp, "JavaTesting-Producer-Base", "BaseConsumer");
            producer.start();
            for (int i = 0; i < 10; i++) {
                random.nextBytes(message);
                Message msg = Message.create(topic, new String(message))
                        .setTopicShardingIDLong(321L);
                producer.publish(msg);
            }
        }finally {
            producer.close();
            logger.info("[ITProducerWPartition#publish] ends");
        }
    }


    private void publishWTopicAndPartition(final Producer producer, String topic) throws NSQException {
        for (int i = 0; i < 2; i++) {
            producer.publish(("Message #" + i).getBytes(), new Topic(topic));
        }
    }

    public void testPublishPartition0() throws Exception {
        String[] lookupds = config.getLookupAddresses();
        if(config.getUserSpecifiedLookupAddress() && null != lookupds && lookupds[0].contains("nsq-"))
            return;

        logger.info("[ITProducerWPartition#testPublishPartition0] starts");
        Producer producer = new ProducerImplV2(this.config);
        try {
            TopicUtil.emptyQueue(this.adminHttp, "JavaTesting-Partition", "BaseConsumer");
            producer.start();
            publishWTopicAndPartition(producer, "JavaTesting-Partition");
        }finally {
            producer.close();
            logger.info("[ITProducerWPartition#testPublishPartition0] ends");
        }
    }
}
