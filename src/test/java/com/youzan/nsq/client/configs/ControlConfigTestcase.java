package com.youzan.nsq.client.configs;

import com.youzan.nsq.client.entity.lookup.AbstractControlConfig;
import com.youzan.nsq.client.entity.lookup.SeedLookupdAddress;
import com.youzan.util.HostUtil;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Created by lin on 16/12/14.
 */
public class ControlConfigTestcase {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ConfigAccessAgent.class);
    protected final static String controlCnfStr = "{" +
                            "\"previous\":[\"http://global.s.qima-inc.com:4161\"]," +
                            "\"current\":[\"http://sqs.s.qima-inc.com:4161\", \"http://sqs.s.qima-inc.com:4161\"]," +
                            "\"gradation\":{" +
                            "\"*\":{\"percent\":10.0}," +
                            "\"bc-pifa0\":{\"percent\":10.0}," +
                            "\"bc-pifa1\":{\"percent\":20.0}," +
                            "\"bc-pifa2\":{\"percent\":30.0}" +
                            "}" +
                            "}";

    protected final static String controlCnfStrContainHost = "{" +
            "\"previous\":[\"http://global.s.qima-inc.com:4161\"]," +
            "\"current\":[\"http://sqs.s.qima-inc.com:4161\", \"http://sqs.s.qima-inc.com:4161\"]," +
            "\"gradation\":{" +
            "\"*\":{\"percent\":10.0}," +
            "\"bc-pifa0\":{\"percent\":10.0}," +
            "\"bc-pifa1\":{\"percent\":20.0}," +
            "\"%s\":{\"percent\":50.0}," +
            "\"bc-pifa2\":{\"percent\":30.0}" +
            "}" +
            "}";

    protected final static String invalidControlCnfStr = "{" +
            "\"previous\":[\"http://global.s.qima-inc.com:4161\"]," +
            "\"current\":[]," +
            "\"gradation\":{" +
            "\"bc-pifa0\":{\"percent\":10.0}," +
            "\"bc-pifa1\":{\"percent\":20.0}," +
            "\"bc-pifa2\":{\"percent\":30.0}" +
            "}" +
            "}";

    @Test
    public void testCreateControlConfig() {
        try {
            logger.info("[testCreateControlConfig] starts.");
            AbstractControlConfig ctrlCnf = AbstractControlConfig.create(controlCnfStr);
            Assert.assertTrue(ctrlCnf.isValid());

            AbstractControlConfig.Gradation aGradation = ctrlCnf.getGradation();
            Assert.assertEquals(aGradation.getPercentage().getFactor(), 10d);

            List<SeedLookupdAddress> curSeedRefs = ctrlCnf.getCurrentReferences();
            Assert.assertEquals(curSeedRefs.size(), 2);
            for (SeedLookupdAddress aSeed : curSeedRefs) {
                Assert.assertNotNull(aSeed);
                Assert.assertEquals("http://sqs.s.qima-inc.com:4161", aSeed.getAddress());
                Assert.assertEquals("http://sqs.s.qima-inc.com:4161", aSeed.getClusterId());
                String lookupdNonexist = aSeed.punchLookupdAddressStr(false);
                Assert.assertNull(lookupdNonexist);
            }

            List<SeedLookupdAddress> preSeedRefs = ctrlCnf.getPreviousReferences();
            Assert.assertEquals(preSeedRefs.size(), 1);
            for (SeedLookupdAddress aSeed : preSeedRefs) {
                Assert.assertNotNull(aSeed);
                Assert.assertEquals("http://global.s.qima-inc.com:4161", aSeed.getAddress());
                Assert.assertEquals("http://global.s.qima-inc.com:4161", aSeed.getClusterId());
                String lookupdNonexist = aSeed.punchLookupdAddressStr(false);
                Assert.assertNull(lookupdNonexist);
            }
        }finally {
            logger.info("[testCreateControlConfig] ends.");
        }
    }

    @Test
    public void testFetchGradation(){
        try {
            logger.info("[testFetchGradation] starts.");
            String hostname = HostUtil.getHostname();
            if (null == hostname) {
                logger.warn("host name not found for current host. Testcase skipped");
                return;
            }

            String ctrlCnfWHostname = String.format(controlCnfStrContainHost, hostname);
            AbstractControlConfig ctrlCnf = AbstractControlConfig.create(ctrlCnfWHostname);
            Assert.assertTrue(ctrlCnf.isValid());
            AbstractControlConfig.Gradation aGradation =  ctrlCnf.getGradation();
            double factor = aGradation.getPercentage().getFactor();
            Assert.assertEquals(factor, 50d);
        }finally {
            logger.info("[testFetchGradation] ends.");
        }
    }

    @Test
    public void testParseInvalidControlConfig() {
        try{
            logger.info("[testParseInvalidControlConfig] starts");
            AbstractControlConfig ctrlCnf = AbstractControlConfig.create(invalidControlCnfStr);
            Assert.assertNull(ctrlCnf);

        }finally {
            logger.info("[testParseInvalidControlConfig] ends.");
        }
    }
}
