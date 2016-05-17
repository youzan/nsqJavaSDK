package com.youzan.nsq.client.core;

import org.testng.annotations.Test;

import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;

public class ConsumerWorkerTest {

    @Test
    public void newPool() throws NSQException {
        Address address = new Address("127.0.0.1", 4150);
        NSQConfig config = new NSQConfig();
        config.setTimeoutInSecond(60);
        ConsumerWorker worker = new ConsumerWorkerImpl(address, config, (msg, ex) -> {
            if (null != msg) {
                if (null != ex) {
                    return false;
                }
                return true;
            } else {
                return false;
            }
        });
        worker.start();
    }

    @Test
    public void createConnection() {
    }
}
