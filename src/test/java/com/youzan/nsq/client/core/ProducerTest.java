package com.youzan.nsq.client.core;

import java.io.IOException;

import org.testng.annotations.Test;

import com.youzan.nsq.client.Producer;
import com.youzan.nsq.client.ProducerImplV2;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;

public class ProducerTest {

    public static final String DEFAULT_CHARSET_NAME = "UTF-8";

    @Test
    public void pub() throws NSQException, IOException {
        Address address = new Address("127.0.0.1", 4150);
        NSQConfig config = new NSQConfig();
        config.setTimeoutInSecond(60);
        Producer p = new ProducerImplV2(config);
        p.start();
        p.publish("test", "zhaoxi-test".getBytes(DEFAULT_CHARSET_NAME));
        p.close();
    }

    @Test
    public void pubMulti() throws NSQException {
        Address address = new Address("127.0.0.1", 4150);
        NSQConfig config = new NSQConfig();
        config.setTimeoutInSecond(60);
    }

}
