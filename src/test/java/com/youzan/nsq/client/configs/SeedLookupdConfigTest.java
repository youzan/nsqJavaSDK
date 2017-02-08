package com.youzan.nsq.client.configs;

import com.youzan.nsq.client.core.LookupAddressUpdate;
import com.youzan.nsq.client.entity.Role;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.entity.lookup.AbstractControlConfig;
import com.youzan.nsq.client.entity.lookup.AbstractSeedLookupdConfig;
import com.youzan.nsq.client.entity.lookup.NSQLookupdAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by lin on 16/12/15.
 */
public class SeedLookupdConfigTest {
    private final static Logger logger = LoggerFactory.getLogger(SeedLookupdConfigTest.class);

    protected final static String invalidControlCnfStr = "{" +
            "\"previous\":[\"http://invalid.qima-inc.com:4161\"]," +
            "\"current\":[\"http://invalid.s.qima-inc.com:4161\", \"http://invalid.s.qima-inc.com:4161\"]," +
            "\"gradation\":{" +
            "\"*\":{\"percent\":10.0}," +
            "\"bc-pifa0\":{\"percent\":10.0}," +
            "\"bc-pifa1\":{\"percent\":20.0}," +
            "\"bc-pifa2\":{\"percent\":30.0}" +
            "}" +
            "}";

    @Test
    public void testLookupdConfigCreate() {
        try{
            logger.info("[testLookupdConfigCreate] starts.");
            TopicRuleCategory category = TopicRuleCategory.getInstance(Role.Producer);
            Topic aTopic = new Topic("testLookupdConfigCreate");
            String categorization = category.category(aTopic);
            AbstractSeedLookupdConfig lookupdConfig = AbstractSeedLookupdConfig.create(categorization);

            AbstractControlConfig ctrlCnf = AbstractControlConfig.create(ControlConfigTestcase.controlCnfStr);
            lookupdConfig.putTopicCtrlCnf(LookupAddressUpdate.formatCategorizationTopic(categorization, aTopic.getTopicText()), ctrlCnf);
            AbstractControlConfig ctrlCnfRe = lookupdConfig.getTopicCtrlCnf(LookupAddressUpdate.formatCategorizationTopic(categorization, aTopic.getTopicText()));

            Assert.assertTrue(ctrlCnfRe == ctrlCnf);

            AbstractControlConfig anotherCtrlCnf = AbstractControlConfig.create(ControlConfigTestcase.controlCnfStr);
            lookupdConfig.putTopicCtrlCnf(LookupAddressUpdate.formatCategorizationTopic(categorization, aTopic.getTopicText()), anotherCtrlCnf);
            ctrlCnfRe = lookupdConfig.getTopicCtrlCnf(LookupAddressUpdate.formatCategorizationTopic(categorization, aTopic.getTopicText()));

            Assert.assertTrue(ctrlCnfRe != ctrlCnf);
        }finally {
            logger.info("[testLookupdConfigCreate] ends.");
        }
    }

    @Test
    public void testPunchLookupdAddress() {
        try{
            logger.info("[testPunchLookupdAddress] starts.");

            TopicRuleCategory category = TopicRuleCategory.getInstance(Role.Producer);
            Topic aTopic = new Topic("testPunchLookupdAddress");
            String categorization = category.category(aTopic);
            AbstractSeedLookupdConfig lookupdConfig = AbstractSeedLookupdConfig.create(categorization);

            AbstractControlConfig ctrlCnf = AbstractControlConfig.create(invalidControlCnfStr);
            lookupdConfig.putTopicCtrlCnf(LookupAddressUpdate.formatCategorizationTopic(categorization, aTopic.getTopicText()), ctrlCnf);

            NSQLookupdAddress aNsqLookupd = lookupdConfig.punchLookupdAddress(categorization, aTopic, false);
            Assert.assertNull(aNsqLookupd);

        }finally {
            logger.info("[testPunchLookupdAddress] ends.");
        }
    }

    @Test
    public void testQueryTopicWhichNotConfigedInConfigAccess() {
        try{
            logger.info("[testQueryTopicWhichNotConfigedInConfigAccess] starts.");

            TopicRuleCategory category = TopicRuleCategory.getInstance(Role.Producer);
            Topic aTopic = new Topic("testQueryTopicWhichNotConfigedInConfigAccess");
            String categorization = category.category(aTopic);
            AbstractSeedLookupdConfig lookupdConfig = AbstractSeedLookupdConfig.create(categorization);

            AbstractControlConfig ctrlCnf = AbstractControlConfig.create(invalidControlCnfStr);
            lookupdConfig.putTopicCtrlCnf(LookupAddressUpdate.formatCategorizationTopic(categorization, aTopic.getTopicText()), ctrlCnf);

            Assert.assertNull(lookupdConfig.punchLookupdAddress(categorization, aTopic, false));

        }finally {
            logger.info("[testQueryTopicWhichNotConfigedInConfigAccess] ends.");
        }
    }
}
