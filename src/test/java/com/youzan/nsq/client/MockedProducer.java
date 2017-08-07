package com.youzan.nsq.client;

import com.youzan.nsq.client.core.NSQConnection;
import com.youzan.nsq.client.entity.Context;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQException;

/**
 * Created by lin on 17/8/3.
 */
public class MockedProducer extends ProducerImplV2 {
    /**
     * @param config NSQConfig
     */
    public MockedProducer(NSQConfig config) {
        super(config);
    }

    public NSQConnection getConnection(final Topic topic, Object objectShardingID, final Context cxt) throws NSQException {
        return this.getNSQConnection(topic, objectShardingID, cxt);
    }
}
