package com.youzan.nsq.client;

import com.youzan.nsq.client.core.NSQConnectionImpl;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.Topic;
import io.netty.channel.Channel;

/**
 * Created by lin on 17/6/26.
 */
public class MockedNSQConnectionImpl extends NSQConnectionImpl {

    public MockedNSQConnectionImpl(int id, Address address, Channel channel, NSQConfig config) {
        super(id, address, channel, config);
    }

    public boolean isConnected() {
        return this.channel.isActive();
    }

    public void setTopic(String topic) {
        super.setTopic(new Topic(topic));
    }
}
