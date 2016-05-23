package com.youzan.nsq.client.core;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.testng.annotations.Test;

import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;

public class KeyedConnectionPoolFactoryTest {
    @Test
    public void createBigPool() {
        NSQConfig config = null;
        GenericKeyedObjectPoolConfig poolConfig = null;
        GenericKeyedObjectPool<Address, Connection> bigPool = new GenericKeyedObjectPool<>(
                new KeyedConnectionPoolFactory(config), poolConfig);
    }
}
