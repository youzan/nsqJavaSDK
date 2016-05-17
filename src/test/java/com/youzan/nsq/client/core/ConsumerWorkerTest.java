package com.youzan.nsq.client.core;

import org.testng.annotations.Test;

import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;

public class ConsumerWorkerTest {

    @Test
    public void newPool() {
        Address address = new Address("127.0.0.1", 4150);
        NSQConfig config = new NSQConfig();
        MessageHandler handler = null;
        ConsumerWorker worker = new ConsumerWorkerImpl(address, config, handler);
        worker.start();
    }

    @Test
    public void createConnection() {
    }
}
