package com.youzan.nsq.client.it;

import java.io.UnsupportedEncodingException;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.youzan.nsq.client.Producer;
import com.youzan.nsq.client.ProducerImplV2;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.util.IOUtil;

public class ITProducer {

    private static final Logger logger = LoggerFactory.getLogger(ITProducer.class);

    // Integration Testing
    private static final String lookup = "10.9.80.209:4161";
    // private static final String lookup = "127.0.0.1:4161";

    @Test
    public void produceUsingSimpleProducer() throws NSQException, UnsupportedEncodingException {
        NSQConfig config = new NSQConfig();
        config.setTopic("test");
        config.setLookupAddresses(lookup);
        config.setTimeoutInSecond(3);
        config.setThreadPoolSize4IO(2);
        config.setMsgTimeoutInMillisecond(60 * 1000);
        final Producer p = new ProducerImplV2(config);
        p.start();

        long sucess = 0L, total = 0L;
        final long end = System.currentTimeMillis() + 1 * 3600 * 1000L;
        while (System.currentTimeMillis() <= end) {
            try {
                total++;
                p.publish(randomString().getBytes(IOUtil.DEFAULT_CHARSET));
                sucess++;
            } catch (Exception e) {
                logger.error("Exception", e);
            }
            logger.info("OK");
            sleep(5);
            assert true;
        }
        p.close();
        logger.info("Total: {} , Sucess: {}", total, sucess);
    }

    @Test
    public void pubMulti() throws NSQException {
    }

    // @Test
    public void newOneProducer() throws NSQException {
        final NSQConfig config = new NSQConfig();
        config.setLookupAddresses(lookup);
        config.setTopic("test");
        final ProducerImplV2 p = new ProducerImplV2(config);
        p.close();
    }

    private String randomString() {
        return "Message" + new Date().getTime();
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
