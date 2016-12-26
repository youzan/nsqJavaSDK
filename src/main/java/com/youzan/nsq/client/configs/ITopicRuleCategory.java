package com.youzan.nsq.client.configs;

import com.youzan.nsq.client.entity.Topic;

/**
 * Created by lin on 16/12/2.
 */
public interface ITopicRuleCategory {
    String category(Topic topic);

    String category(String topic);
}
