package com.youzan.nsq.client;

import com.youzan.nsq.client.core.ConnectionManager;
import com.youzan.nsq.client.core.NSQConnection;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.exception.NSQException;

import java.util.Map;

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

    public void connect(Address addr) throws Exception {
        super.connect(addr);
    }

    public void setConnectionManager(final ConnectionManager conMgr) {
        this.conMgr = conMgr;
    }

    public Map<Address, NSQConnection> getAddress2Conn() {
        return super.address_2_conn;
    }

    public void start() {
        super.started.set(Boolean.TRUE);
    }

    public void startParent() throws NSQException {
        super.start();
    }

    public boolean needSkip4MsgKV(NSQMessage msg) {
        return super.needSkip4MsgKV(msg);
    }

    /**
     * unscribe topic from consumer
     * @param topic topic to remove
     */
    public void unsubscribe(String topic) {
        if(topics2Partitions.containsKey(topic)) {
            topics2Partitions.remove(topic);
        }
    }
}
