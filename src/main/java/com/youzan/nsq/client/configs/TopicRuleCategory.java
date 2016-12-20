package com.youzan.nsq.client.configs;

import com.youzan.nsq.client.entity.Role;
import com.youzan.nsq.client.entity.Topic;

/**
 * topic category maintain mapping from topic to categorization.
 * Created by lin on 16/12/7.
 */
public class TopicRuleCategory implements ITopicRuleCategory {
    public static final String TOPIC_CATEGORIZATION_USER_SPECIFIED = "categorization.sdk.nsq.default";
    public static final String TOPIC_CATEGORIZATION_SUFFIX = "%s.nsq.lookupd.addr:%s";

    private final Role role;

    private final static TopicRuleCategory categoryProducer = new TopicRuleCategory(Role.Producer);
    private final static TopicRuleCategory categoryConsumer = new TopicRuleCategory(Role.Consumer);

    public static TopicRuleCategory getInstance(Role role) {
        switch (role) {
            case Consumer: {
                return categoryConsumer;
            }
            default: {
                return categoryProducer;
            }
        }
    }

    /**
     * return {@link Role} of current topic rule category
     */
    public Role getRole() {
        return this.role;
    }

    public TopicRuleCategory(final Role role) {
        this.role = role;
    }

    @Override
    public String category(Topic topic) {
        return category(topic.getTopicText());
    }

    @Override
    public String category(String topic) {
        return String.format(TOPIC_CATEGORIZATION_SUFFIX, trimTopic(topic), this.role.getRoleTxt());
    }

    //TODO: what is final solution
    private String trimTopic(String topicText) {
        String[] parts = topicText.split("\\.", 2);
        if (null != parts && parts.length > 0)
            return parts[0];
        else
            return topicText;
    }
}
