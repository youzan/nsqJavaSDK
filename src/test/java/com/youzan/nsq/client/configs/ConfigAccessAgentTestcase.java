package com.youzan.nsq.client.configs;

import com.youzan.nsq.client.PropertyNotFoundException;
import com.youzan.nsq.client.PubCmdFactory;
import com.youzan.nsq.client.core.LookupAddressUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.*;
import java.util.List;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

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
        System.setProperty("nsq.configs.configFilePath", "src/test/resources/configClientTest.properties");
        //load properties from configClientTest.properties
        InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties");
        Properties proTest = new Properties();
        proTest.load(is);
        is.close();
        //initialize system property form config client properties
        String configPath = proTest.getProperty("configClientPath.test");
        System.setProperty("nsq.sdk.configFilePath", configPath);

        Path configFielPath = Paths.get(configPath);
        is = Files.newInputStream(configFielPath.toAbsolutePath(), StandardOpenOption.READ);
        props.load(is);
        logger.info("init of [ConfigAccessAgentTestcase] ends.");
    }

    @Test
    public void testInitConfigAccessAgentViaSystemProperty() throws IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        //specify system property of
        logger.info("[testInitConfigAccessAgentViaSystemProperty] starts.");

        agent = ConfigAccessAgent.getInstance();
        Assert.assertNotNull(agent);
        release();

        logger.info("Initialize ConfigAccessAgent from internal config.");
        System.clearProperty("nsq.sdk.configFilePath");
        agent = ConfigAccessAgent.getInstance();
        Assert.assertNotNull(agent);
        release();

        logger.info("[testInitConfigAccessAgentViaSystemProperty] ends.");
    }

    @Test
    public void testGetConfigKeysFromLookupAddressesUpdate() {
        try {
            logger.info("[testGetConfigKeysFromLookupAddressesUpdate] starts.");
            LookupAddressUpdate lookupUpdate = LookupAddressUpdate.getInstance();
            String lookupDomain = lookupUpdate.getDomain();
            Assert.assertEquals(lookupDomain, props.getProperty("nsq.app.val"));
            String[] lookupKeys = lookupUpdate.getKeys();
            Assert.assertEquals(lookupKeys[0], props.getProperty("nsq.key.lookupd.addr"));
            //verify
        }finally {
            logger.info("[testGetConfigKeysFromLookupAddressesUpdate] ends.");
        }
    }

    @Test
    public void testSubscribeLookupAddressUpdate() throws InterruptedException {
        try {
            logger.info("[testSubscribeLookupAddressUpdate] starts.");
            //set testConfigAccessAgent
            TestConfigAccessAgent testConfigAccessAgent = (TestConfigAccessAgent) ConfigAccessAgent.getInstance();
            LookupAddressUpdate lookupUpdate = LookupAddressUpdate.getInstance();
            SortedMap<String, String> valueMap = new TreeMap<>();
            valueMap.put("addr1", "http://url1.com");
            TestConfigAccessAgent.updateValue(lookupUpdate.getDomain(), lookupUpdate.getKeys(), valueMap, true);
            //first subscribe, nothing happen
            lookupUpdate.subscribe(testConfigAccessAgent);
            List<String> lookupAddresses = lookupUpdate.getLookupAddresses();
            Assert.assertNotNull(lookupAddresses);
            Assert.assertEquals(lookupAddresses.size(), 1);

            valueMap.put("addr2", "http://url2.com");
            valueMap.put("addr3", "http://url3.com");
            valueMap.put("addr4", "http://url4.com");
            TestConfigAccessAgent.updateValue(lookupUpdate.getDomain(), lookupUpdate.getKeys(), valueMap, true);
            Thread.sleep(1000L);

            lookupAddresses = lookupUpdate.getLookupAddresses();
            Assert.assertNotNull(lookupAddresses);
            Assert.assertEquals(lookupAddresses.size(), 4);
            //close config access
            testConfigAccessAgent.close();
        }finally {
            logger.info("[testSubscribeLookupAddressUpdate] ends.");
        }
    }

    @Test
    public void testGetConfigKeysFromPubCmdFactory() {
        try {
            logger.info("[testGetConfigKeysFromPubCmdFactory] starts.");
            PubCmdFactory pubFactory = PubCmdFactory.getInstance();
            String pubFactoryDomain = pubFactory.getDomain();
            Assert.assertEquals(pubFactoryDomain, props.getProperty("nsq.app.val"));
            String[] pubFactoryKeys = pubFactory.getKeys();
            Assert.assertEquals(pubFactoryKeys[0], props.getProperty("nsq.key.topic.trace"));
            //verify
        }finally {
            logger.info("[testGetConfigKeysFromPubCmdFactory] ends.");
        }
    }

    @AfterMethod
    public void release() throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        Method method = ConfigAccessAgent.class.getDeclaredMethod("release");
        method.setAccessible(true);
        method.invoke(agent);
    }

}
