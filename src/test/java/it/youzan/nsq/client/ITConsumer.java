package it.youzan.nsq.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;

public class ITConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ITConsumer.class);

    @Test
    public void consumeOK() throws NSQException {
        final NSQConfig config = new NSQConfig();
        config.setLookupAddresses("");
        config.setConnectTimeoutInMillisecond(100);
        config.setTimeoutInSecond(3);
        config.setThreadPoolSize4IO(2);
        config.setMsgTimeoutInMillisecond(120 * 1000);
        config.setTopic("");
        config.setConsumerName("");

    }

    /**
     * @param millisecond
     */
    private void sleep(final long millisecond) {
        try {
            Thread.sleep(millisecond);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("System is too busy! Please check it!", e);
        }
    }

}
