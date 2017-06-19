package com.youzan.nsq.client.configs;

/**
 * Migration config access domain for DCC.
 * Created by lin on 16/12/9.
 */
public class DCCMigrationConfigAccessDomain extends AbstractConfigAccessDomain<String> {
    private String domain;
    private static final String DOMAIN_SUFFIX = "%s.nsq.lookupd.addr";

    private DCCMigrationConfigAccessDomain(String domain) {
        super(domain);
    }

    @Override
    public String toDomain() {
        if (null == domain)
            this.domain = String.format(DOMAIN_SUFFIX, TopicRuleCategory.trimTopic(this.innerDomain));
        return this.domain;
    }

    public static AbstractConfigAccessDomain getInstance(final String topic) {
        return new DCCMigrationConfigAccessDomain(topic);
    }
}
