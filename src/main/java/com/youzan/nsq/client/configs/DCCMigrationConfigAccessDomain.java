package com.youzan.nsq.client.configs;

import com.youzan.nsq.client.entity.Topic;

/**
 * Created by lin on 16/12/9.
 */
public class DCCMigrationConfigAccessDomain extends AbstractConfigAccessDomain<Topic> {
    private String domain;

    private static final String DOMAIN_SUFFIX = "%s.nsq.lookupd.addr";

    public DCCMigrationConfigAccessDomain(Topic domain) {
        super(domain);
    }

    @Override
    public String toDomain() {
        if (null == domain)
            this.domain = String.format(DOMAIN_SUFFIX, trimTopic(this.innerDomain.getTopicText()));
        return this.domain;
    }

    //TODO: what is final solution
    private String trimTopic(String topicText) {
        String[] parts = topicText.split("\\.", 2);
        if (null != parts && parts.length > 0)
            return parts[0];
        else
            return topicText;
    }

    public static AbstractConfigAccessDomain getInstance(final Topic topic) {
        return new DCCMigrationConfigAccessDomain(topic);
    }
}
