package com.youzan.nsq.client.configs;

import com.youzan.nsq.client.PubCmdFactory;
import com.youzan.nsq.client.core.LookupAddressUpdate;
import com.youzan.nsq.client.core.command.Pub;
import com.youzan.nsq.client.core.command.PubTrace;
import com.youzan.nsq.client.entity.Message;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.Role;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.ConfigAccessAgentException;
import com.youzan.nsq.client.exception.NSQConfigAccessException;
import com.youzan.nsq.client.exception.NSQPubFactoryInitializeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Created by lin on 16/10/31.
 */
public class ConfigAccessAgentTestcase {
    private static Logger logger = LoggerFactory.getLogger(ConfigAccessAgent.class);
    private ConfigAccessAgent agent;
    private Properties props = new Properties();

    @BeforeClass
    public void init() throws IOException {
        logger.info("init of [ConfigAccessAgentTestcase].");
        logger.info("Initialize ConfigAccessAgent from system specified config.");
        System.setProperty("nsq.sdk.configFilePath", "src/test/resources/configClientTest.properties");
        //load properties from configClientTest.properties
        InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties");
        Properties proTest = new Properties();
        proTest.load(is);
        is.close();
        logger.info("init of [ConfigAccessAgentTestcase] ends.");
    }

    @Test
    public void testConfigAccessAgentInitFromSystemProperty() throws ConfigAccessAgentException {
        try{
            logger.info("[testConfigAccessAgentInitFromSystemProperty] starts.");
            System.clearProperty("nsq.sdk.configFilePath");
            System.setProperty("nsq.sdk.env", "qa");
            agent = ConfigAccessAgent.getInstance();
            String metas = agent.metadata();
            Assert.assertTrue(metas.contains("env: [qa]"));
            Assert.assertTrue(metas.contains("urls: [http://10.9.7.75:8089;http://10.9.28.182:8089;http://10.9.17.150:8089;]"));
        }finally {
            logger.info("[testConfigAccessAgentInitFromSystemProperty] ends.");
            System.setProperty("nsq.sdk.configFilePath", "src/test/resources/configClientTest.properties");
        }
    }

    @Test
    public void testInitConfigAccessAgentViaSystemProperty() throws IllegalAccessException, NoSuchMethodException, InvocationTargetException, ConfigAccessAgentException {
        //specify system property of
        logger.info("[testInitConfigAccessAgentViaSystemProperty] starts.");

        agent = ConfigAccessAgent.getInstance();
        Assert.assertNotNull(agent);
        release();

        logger.info("Initialize ConfigAccessAgent from internal config.");
        String configFileSys =  System.clearProperty("nsq.sdk.configFilePath");
        agent = ConfigAccessAgent.getInstance();
        Assert.assertNotNull(agent);
        System.setProperty("nsq.sdk.configFilePath", configFileSys);
        release();

        logger.info("[testInitConfigAccessAgentViaSystemProperty] ends.");
    }

    @Test
    public void testGetTraceConfigAccessProperties() throws ConfigAccessAgentException {
        try {
            logger.info("[testGetTraceConfigAccessProperties] starts.");
            //a config access instance needs initialization before getting properties from config access.
            agent = ConfigAccessAgent.getInstance();
            DCCTraceConfigAccessDomain domain = (DCCTraceConfigAccessDomain) DCCTraceConfigAccessDomain.getInstacne();
            String domainStr = domain.toDomain();
            //verify
            Assert.assertEquals(domainStr, "nsq");

            DCCTraceConfigAccessKey aKey = (DCCTraceConfigAccessKey) DCCTraceConfigAccessKey.getInstacne();
            String keyStr = aKey.toKey();
            Assert.assertEquals(keyStr, "trace");
        }finally {
            logger.info("[testGetTraceConfigAccessProperties] ends.");
        }
    }

    @Test
    public void testGetMigrationConfigAccessProperties() throws ConfigAccessAgentException {
        try{
            logger.info("[testGetMigrationConfigAccessProperties] starts.");
            agent = ConfigAccessAgent.getInstance();
            Topic topic = new Topic("migrationConfigAccessTopic");
            DCCMigrationConfigAccessDomain domain = (DCCMigrationConfigAccessDomain) DCCMigrationConfigAccessDomain.getInstance(topic);
            String expectedDomain = "migrationConfigAccessTopic.nsq.lookupd.addr";
            Assert.assertEquals(domain.toDomain(), expectedDomain);

            Role aRole = Role.getInstance("producer");
            DCCMigrationConfigAccessKey key = (DCCMigrationConfigAccessKey) DCCMigrationConfigAccessKey.getInstance(aRole);
            String expectedKey = "producer";
            Assert.assertEquals(key.toKey(), expectedKey);
        }finally {
            logger.info("[testGetMigrationConfigAccessProperties] ends.");
        }
    }

    @Test
    public void testTopicRuleCategory() {
        try{
            logger.info("[testTopicRuleCategory] starts.");

            TopicRuleCategory ruleCategory = TopicRuleCategory.getInstance(Role.Consumer);
            Assert.assertEquals(Role.Consumer, ruleCategory.getRole());
            Topic topic = new Topic("testTopic.Rule.Category");
            String topicCategory = ruleCategory.category(topic);
            Assert.assertEquals(topicCategory, "testTopic.nsq.lookupd.addr:consumer");
        }finally {
            logger.info("[testTopicRuleCategory] ends");
        }
    }

    @Test
    public void testSubscribeLookupAddressUpdate() throws InterruptedException, NSQConfigAccessException, ConfigAccessAgentException {
        try {
            logger.info("[testSubscribeLookupAddressUpdate] starts.");
            //set testConfigAccessAgent
            TestConfigAccessAgent testConfigAccessAgent = (TestConfigAccessAgent) ConfigAccessAgent.getInstance();
            LookupAddressUpdate lookupUpdate = LookupAddressUpdate.getInstance();
            final Set<String> expectedKeySet = new HashSet<>();
            final SortedMap<String, String> valueMap = new TreeMap<>();
            valueMap.put("topic1",
                "{" +
                    "\"previous\":[\"http://global.s.qima-inc.com:4161\"]," +
                    "\"current\":[\"http://sqs.s.qima-inc.com:4161\"]," +
                    "\"gradation\":{" +
                    "\"*\":{\"percent\":10.0}," +
                    "\"bc-pifa0\":{\"percent\":10.0}," +
                    "\"bc-pifa1\":{\"percent\":20.0}," +
                    "\"bc-pifa2\":{\"percent\":30.0}" +
                    "}" +
                "}"
                );
            expectedKeySet.add("addr1");

            Topic topic = new Topic("subscribeLookupAddressUpdate.Topic");
            DCCMigrationConfigAccessDomain domain = (DCCMigrationConfigAccessDomain) DCCMigrationConfigAccessDomain.getInstance(topic);
            Role aRole = Role.getInstance("producer");
            DCCMigrationConfigAccessKey key = (DCCMigrationConfigAccessKey) DCCMigrationConfigAccessKey.getInstance(aRole);
            TestConfigAccessAgent.updateValue(domain, new AbstractConfigAccessKey[]{key}, valueMap, true);

            ConfigAccessAgent.IConfigAccessCallback aCallback = new ConfigAccessAgent.IConfigAccessCallback<SortedMap<String, String>>() {
                @Override
                public void process(SortedMap<String, String> newItems) {
                    logger.info("process enters.");
                    for(String key : newItems.keySet()){
                        Assert.assertTrue(expectedKeySet.contains(key));
                        Assert.assertEquals(valueMap.get(key), newItems.get(key));
                    }
                }

                @Override
                public void fallback(SortedMap itemsInCache, Object... objs) {
                    Assert.fail("Should not invoked in fallback.");
                }
            };

            //first subscribe, nothing happen
            lookupUpdate.subscribe(testConfigAccessAgent, domain, new AbstractConfigAccessKey[]{key}, aCallback);

            valueMap.put("topic2",
                    "{" +
                    "\"previous\":[\"http://global.s.qima-inc.com:4161\"]," +
                    "\"current\":[\"http://sqs.s.qima-inc.com:4161\"]," +
                    "\"gradation\":{" +
                    "\"*\":{\"percent\":10.0}," +
                    "\"bc-pifa0\":{\"percent\":10.0}," +
                    "\"bc-pifa1\":{\"percent\":20.0}," +
                    "\"bc-pifa2\":{\"percent\":30.0}" +
                    "}" +
                    "}"
            );

            valueMap.put("topic3",
                    "{" +
                    "\"previous\":[\"http://global.s.qima-inc.com:4161\"]," +
                    "\"current\":[\"http://sqs.s.qima-inc.com:4161\"]," +
                    "\"gradation\":{" +
                    "\"*\":{\"percent\":10.0}," +
                    "\"bc-pifa0\":{\"percent\":10.0}," +
                    "\"bc-pifa1\":{\"percent\":20.0}," +
                    "\"bc-pifa2\":{\"percent\":30.0}" +
                    "}" +
                    "}"
            );

            valueMap.put("topic4",
                    "{" +
                    "\"previous\":[\"http://global.s.qima-inc.com:4161\"]," +
                    "\"current\":[\"http://sqs.s.qima-inc.com:4161\"]," +
                    "\"gradation\":{" +
                    "\"*\":{\"percent\":10.0}," +
                    "\"bc-pifa0\":{\"percent\":10.0}," +
                    "\"bc-pifa1\":{\"percent\":20.0}," +
                    "\"bc-pifa2\":{\"percent\":30.0}" +
                    "}" +
                    "}"
            );
            TestConfigAccessAgent.updateValue(domain, new AbstractConfigAccessKey[]{key}, valueMap, true);
            Thread.sleep(1000L);

            //close config access
            testConfigAccessAgent.close();
        }finally {
            logger.info("[testSubscribeLookupAddressUpdate] ends.");
        }
    }

    @Test
    public void testGetConfigKeysFromPubCmdFactory() throws NSQPubFactoryInitializeException {
        try {
            logger.info("[testGetConfigKeysFromPubCmdFactory] starts.");
            PubCmdFactory pubFactory = PubCmdFactory.getInstance(true);
            String pubFactoryDomain = new DCCTraceConfigAccessDomain().toDomain();
            Assert.assertEquals(pubFactoryDomain, props.getProperty("nsq.app.val"));
            String aPubFactoryKey = new DCCTraceConfigAccessKey().toKey();
            Assert.assertEquals(aPubFactoryKey, props.getProperty("nsq.key.topic.trace"));
            //verify
        }finally {
            logger.info("[testGetConfigKeysFromPubCmdFactory] ends.");
        }
    }

    @Test
    public void testPubCmdFactoryInLocalThenConnectRemote() throws NSQPubFactoryInitializeException, ConfigAccessAgentException {
        try{
            logger.info("[testPubCmdFactoryInLocalThenConnectRemote] starts.");
            ConfigAccessAgent.getInstance();
            //set up test config access agent
            final SortedMap<String, String> valueMap = new TreeMap<>();
            valueMap.put("JavaTesting-Trace1", "1");
            DCCTraceConfigAccessKey KEY = new DCCTraceConfigAccessKey();
            DCCTraceConfigAccessDomain DOMAIN = new DCCTraceConfigAccessDomain();
            TestConfigAccessAgent.updateValue(DOMAIN, new AbstractConfigAccessKey[]{KEY}, valueMap, true);

            //create a pubcmd factory in "local mode"
            NSQConfig config = new NSQConfig();
            config.setLookupAddresses("dummy-lookupd-address");
            config.turnOnLocalTrace("JavaTesting-Trace2");
            PubCmdFactory pubFactory = PubCmdFactory.getInstance(false);
            Pub pubCmd =  pubFactory.create(Message.create(new Topic("JavaTesting-Trace2"), "msg"), config);
            Assert.assertTrue(pubCmd instanceof PubTrace);

            PubCmdFactory pubFactorySame = PubCmdFactory.getInstance(true);
            Assert.assertTrue(pubFactorySame == pubFactory);

            config = new NSQConfig();
            //dummy dcc url, but valid in format
            config.setLookupAddresses("dcc://localhost:8089?env=qa");
            pubCmd =  pubFactory.create(Message.create(new Topic("JavaTesting-Trace1"), "msg"), config);
            Assert.assertTrue(pubCmd instanceof PubTrace);
        }finally {
            logger.info("[testPubCmdFactoryInLocalThenConnectRemote] ends.");
        }
    }

    @AfterMethod
    public void release() throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        Method method = ConfigAccessAgent.class.getDeclaredMethod("release");
        method.setAccessible(true);
        method.invoke(agent);
    }

}
