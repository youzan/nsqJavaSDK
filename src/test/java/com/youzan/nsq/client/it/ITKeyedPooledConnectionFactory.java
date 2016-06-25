package com.youzan.nsq.client.it;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.testng.annotations.Test;

import com.youzan.nsq.client.core.Client;
import com.youzan.nsq.client.core.KeyedPooledConnectionFactory;
import com.youzan.nsq.client.core.NSQConnection;
import com.youzan.nsq.client.core.NSQSimpleClient;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;

public class ITKeyedPooledConnectionFactory {
    @Test
    public void createBigPool() throws Exception {
        final Client simpleClient = new NSQSimpleClient("127.0.0.1:4161", "test");
        final NSQConfig config = new NSQConfig();
        final GenericKeyedObjectPoolConfig poolConfig = new GenericKeyedObjectPoolConfig();

        config.setThreadPoolSize4IO(Runtime.getRuntime().availableProcessors() - 1);
        config.setTimeoutInSecond(10);
        // pool config
        poolConfig.setFairness(false);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setJmxEnabled(false);
        poolConfig.setMinIdlePerKey(0);
        poolConfig.setMinEvictableIdleTimeMillis(30 * 1000);
        poolConfig.setMaxIdlePerKey(2);
        poolConfig.setMaxTotalPerKey(2);
        poolConfig.setMaxWaitMillis(1 * 1000);
        poolConfig.setBlockWhenExhausted(false);
        poolConfig.setTestWhileIdle(true);

        GenericKeyedObjectPool<Address, NSQConnection> bigPool = new GenericKeyedObjectPool<>(
                new KeyedPooledConnectionFactory(config, simpleClient), poolConfig);
        Address addr = new Address("127.0.0.1", 4150);

        NSQConnection conn = null;
        conn = bigPool.borrowObject(addr);
        bigPool.returnObject(addr, conn);

        conn = bigPool.borrowObject(addr);
        bigPool.returnObject(addr, conn);

        conn = bigPool.borrowObject(addr);
        bigPool.returnObject(addr, conn);
    }
}
