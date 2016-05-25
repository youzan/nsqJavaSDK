package com.youzan.nsq.client.core;

import java.io.UnsupportedEncodingException;

import org.testng.annotations.Test;

import com.youzan.nsq.client.Producer;
import com.youzan.nsq.client.ProducerImplV2;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;

public class ProducerTest {

    public static final String DEFAULT_CHARSET_NAME = "UTF-8";

    @Test
    public void pub() throws NSQException, UnsupportedEncodingException {
        NSQConfig config = new NSQConfig();
        config.setTopic("test");
        config.setLookupAddresses("127.0.0.1:4161");
        config.setTimeoutInSecond(120);
        config.setThreadPoolSize4IO(1);
        Producer p = new ProducerImplV2(config);
        p.start();
        p.publish("zhaoxi-test".getBytes(DEFAULT_CHARSET_NAME));
        p.close();
    }

    // @Test
    public void pubMulti() throws NSQException {
        Address address = new Address("127.0.0.1", 4150);
        NSQConfig config = new NSQConfig();
        config.setTopic("test");
        config.setTimeoutInSecond(60);
    }

    // @Test
    public void newOneProducer() throws NSQException {
        final NSQConfig config = new NSQConfig();
        config.setLookupAddresses("127.0.0.1:4161");
        config.setTopic("test");
        final ProducerImplV2 p = new ProducerImplV2(config);
        p.start();
        p.close();
    }

}
