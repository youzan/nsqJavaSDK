package com.youzan.nsq.client.entity.lookup;

import com.youzan.nsq.client.configs.ConfigAccessAgent;
import com.youzan.nsq.client.configs.DCCConfigAccessAgent;
import com.youzan.nsq.client.entity.NSQConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by lin on 16/12/21.
 */
public class NSQConfigTestcase {

    private final static Logger logger = LoggerFactory.getLogger(NSQConfigTestcase.class);
    private Properties props = new Properties();

    @BeforeClass
    public void init() throws IOException {
        logger.info("init of [NSQConfigTestcase].");
        logger.info("Initialize ConfigAccessAgent from system specified config.");
        System.setProperty("nsq.sdk.configFilePath", "src/main/resources/configClient.properties");
        logger.info("init of [NSQConfigTestcase] ends.");
    }

    @Test
    public void testSetDCCUrlsInNSQConfig() {
        String dumyUrl = "http://invalid.dcc.url:1234";
        ConfigAccessAgent.setConfigAccessRemotes(dumyUrl);
        NSQConfig config = new NSQConfig();
        DCCConfigAccessAgent agent = (DCCConfigAccessAgent) ConfigAccessAgent.getInstance();
        Assert.assertEquals(dumyUrl, DCCConfigAccessAgent.getUrls()[0]);
    }

    @AfterMethod
    public void release(){
        System.clearProperty("nsq.sdk.configFilePath");
    }
}
