package it.youzan.nsq.client;

import com.youzan.nsq.client.Producer;
import com.youzan.nsq.client.ProducerImplV2;
import com.youzan.nsq.client.entity.Message;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

/**
 * Created by lin on 16/10/19.
 */
@Test(groups = {"ITProducerTrace"}, priority = 3)
public class ITProducerTrace extends ITProducer {

    private static final Logger logger = LoggerFactory.getLogger(ITProducer.class);

    public void publishTrace() throws NSQException {
        //set trace id, which is a long(8-byte-length)
        logger.info("[ITProducerTrace#publishTrace] starts");
        Topic topic = new Topic("dcp_event");
        String[] lookupds = config.getLookupAddresses();
//        if(config.getUserSpecifiedLookupAddress() && null != lookupds && lookupds[0].contains("nsq-"))
//            return;
        config.setLookupAddresses("dcc://10.9.7.75:8089?env=qa");
        Producer producer = new ProducerImplV2(this.config);
        try {
            producer.start();
            for (int i = 0; i < 10; i++) {
                Message msg = Message.create(topic, 45678L, ("Message #" + i));
                producer.publish(msg);
            }
        }finally {
            producer.close();
            logger.info("[ITProducerTrace#publishTrace] ends");
        }
    }
}
