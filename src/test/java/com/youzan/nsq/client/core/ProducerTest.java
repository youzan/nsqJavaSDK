package com.youzan.nsq.client.core;

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

public class ProducerTest {

    private static final Logger logger = LoggerFactory.getLogger(ProducerTest.class);

    private static final String lookup = "10.9.80.209:4161";

    @Test
    public void produceUsingSimpleProducer() throws NSQException, UnsupportedEncodingException {
        NSQConfig config = new NSQConfig();
        config.setTopic("test");
        config.setLookupAddresses(lookup);
        config.setTimeoutInSecond(60);
        config.setMsgTimeoutInMillisecond(60 * 1000);
        config.setThreadPoolSize4IO(1);
        final Producer p = new ProducerImplV2(config);
        p.start();
        for (int i = 0; i < 1000; i++) {
            p.publish(randomString().getBytes(IOUtil.DEFAULT_CHARSET));
            logger.info("OK");
            sleep(200);
            assert true;
        }
        p.close();
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
    private void sleep(final int millisecond) {
        try {
            Thread.sleep(millisecond);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("System is too busy! Please check it!", e);
        }
    }
}
