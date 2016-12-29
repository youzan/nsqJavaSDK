package com.youzan.nsq.client.configs;

import com.youzan.nsq.client.entity.Topic;

/**
 * Migration config access domain for DCC.
 * Created by lin on 16/12/9.
 */
public class DCCMigrationConfigAccessDomain extends AbstractConfigAccessDomain<Topic> {
    private String domain;
    private static final String DOMAIN_SUFFIX = "%s.nsq.lookupd.addr";

    private DCCMigrationConfigAccessDomain(Topic domain) {
        super(domain);
    }

    @Override
    public String toDomain() {
        if (null == domain)
            this.domain = String.format(DOMAIN_SUFFIX, TopicRuleCategory.trimTopic(this.innerDomain.getTopicText()));
        return this.domain;
    }

    public static AbstractConfigAccessDomain getInstance(final Topic topic) {
        return new DCCMigrationConfigAccessDomain(topic);
    }
}
