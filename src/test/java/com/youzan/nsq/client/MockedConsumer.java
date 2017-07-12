package com.youzan.nsq.client;

import com.youzan.nsq.client.core.ConnectionManager;
import com.youzan.nsq.client.core.pool.consumer.FixedPool;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;

import java.util.Map;
import java.util.Set;

/**
 * Created by lin on 17/7/11.
 */
public class MockedConsumer extends ConsumerImplV2 {
    /**
     * @param config  NSQConfig
     * @param handler the client code sets it
     */
    public MockedConsumer(NSQConfig config, MessageHandler handler) {
        super(config, handler);
    }

    public void connect() throws NSQException {
        super.connect();
    }

    public void connect(Address addr, Set<String> topics) throws Exception {
        super.connect(addr, topics);
    }

    public void setConnectionManager(final ConnectionManager conMgr) {
        this.conMgr = conMgr;
    }

    public Map<Address, FixedPool> getAddress2Pool() {
        return super.address_2_pool;
    }

    public void start() {
        super.started = true;
    }
}
