package com.youzan.nsq.client.core;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.testng.annotations.Test;

import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;

public class KeyedPooledConnectionFactoryTest {
    @Test(expectedExceptions = Exception.class)
    public void createBigPoolFail() {
        NSQConfig config = null;
        GenericKeyedObjectPoolConfig poolConfig = null;
        GenericKeyedObjectPool<Address, NSQConnection> bigPool = new GenericKeyedObjectPool<>(
                new KeyedPooledConnectionFactory(config, null), poolConfig);
    }
}
