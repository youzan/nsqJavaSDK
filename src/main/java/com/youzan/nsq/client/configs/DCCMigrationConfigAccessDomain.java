package com.youzan.nsq.client.configs;

import com.youzan.nsq.client.entity.Topic;

/**
 * Created by lin on 16/12/9.
 */
public class DCCMigrationConfigAccessDomain extends AbstractConfigAccessDomain<Topic> {
    private String domain;

    private static final String DOMAIN_SUFFIX = "%s.nsq.lookupd.addr";
    private static final String TOPIC_BINLOG_PATTERN = "binlog_";

    public DCCMigrationConfigAccessDomain(Topic domain) {
        super(domain);
    }

    @Override
    public String toDomain() {
        if (null == domain)
            this.domain = String.format(DOMAIN_SUFFIX, trimTopic(this.innerDomain.getTopicText()));
        return this.domain;
    }

    private String trimTopic(String topicText) {
        if(null  ==  topicText || topicText.isEmpty())
            throw new IllegalArgumentException("pass in topic text should not be empty");
        String[] parts;
        if(topicText.startsWith(TOPIC_BINLOG_PATTERN)){
            parts = topicText.split("_", 3);
        }else{
            parts = topicText.split("_", 2);
        }
        if (null != parts && parts.length > 0) {
            if(topicText.startsWith(TOPIC_BINLOG_PATTERN))
                return TOPIC_BINLOG_PATTERN + parts[1];
            else
                return parts[0];
        }else
            return topicText;
    }

    public static AbstractConfigAccessDomain getInstance(final Topic topic) {
        return new DCCMigrationConfigAccessDomain(topic);
    }
}
