package com.youzan.nsq.client.configs;

import com.youzan.nsq.client.entity.Role;
import com.youzan.nsq.client.entity.Topic;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by lin on 16/12/27.
 */
public class TopicRuleCategoryTestcase {

    @Test
    public void testCategory() {
        String expectedBinlogCategorization = "binlog_admin.nsq.lookupd.addr:consumer";
        Topic aTopic = new Topic("binlog_admin_change");
        TopicRuleCategory category = TopicRuleCategory.getInstance(Role.Consumer);
        String result = category.category(aTopic);
        Assert.assertEquals(result, expectedBinlogCategorization);

        String expectedNormalCategorization = "normalTopic.nsq.lookupd.addr:producer";
        aTopic = new Topic("normalTopic_shouldbe_skipped");
        category = TopicRuleCategory.getInstance(Role.Producer);
        result = category.category(aTopic);
        Assert.assertEquals(result, expectedNormalCategorization);

        String expectedSpecialCategorization = "JavaTesting-Producer-Base.nsq.lookupd.addr:producer";
        aTopic = new Topic("JavaTesting-Producer-Base");
        category = TopicRuleCategory.getInstance(Role.Producer);
        result = category.category(aTopic);
        Assert.assertEquals(result, expectedSpecialCategorization);

        String expectedAbnormalCategorization = "binlog_.nsq.lookupd.addr:producer";
        aTopic = new Topic("binlog_");
        category = TopicRuleCategory.getInstance(Role.Producer);
        result = category.category(aTopic);
        Assert.assertEquals(result, expectedAbnormalCategorization);

        try {
            aTopic = new Topic("");
            category = TopicRuleCategory.getInstance(Role.Producer);
            category.category(aTopic);
            Assert.fail("should not pass previous statement");
        }catch(Exception e) {
            Assert.assertNotNull(e);
        }
    }
}
