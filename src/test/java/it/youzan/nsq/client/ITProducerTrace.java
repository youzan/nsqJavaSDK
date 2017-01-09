package it.youzan.nsq.client;

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
        Topic topic = new Topic("JavaTesting-Trace");
        String[] lookupds = config.getLookupAddresses();
        if(config.getUserSpecifiedLookupAddress() && null != lookupds && lookupds[0].contains("nsq-"))
            return;
        for (int i = 0; i < 10; i++) {
            Message msg = Message.create(topic, 45678L, ("Message #" + i));
            producer.publish(msg);
        }
    }
}
