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

    @Test
    public void produceUsingSimpleProducer() throws NSQException, UnsupportedEncodingException {
        NSQConfig config = new NSQConfig();
        config.setTopic("test");
        config.setLookupAddresses("127.0.0.1:4161");
        config.setTimeoutInSecond(1);
        config.setThreadPoolSize4IO(1);
        Producer p = new ProducerImplV2(config);
        p.start();
        for (int i = 0; i < 1000; i++) {
            p.publish(randomString().getBytes(IOUtil.DEFAULT_CHARSET));
            logger.info("OK");
            assert true;
        }
        p.close();
    }

    @Test
    public void pubMulti() throws NSQException {
    }

    @Test
    public void newOneProducer() throws NSQException {
        final NSQConfig config = new NSQConfig();
        config.setLookupAddresses("127.0.0.1:4161");
        config.setTopic("test");
        final ProducerImplV2 p = new ProducerImplV2(config);
        p.close();
    }

    private String randomString() {
        return "Message" + new Date().getTime();
    }
}
