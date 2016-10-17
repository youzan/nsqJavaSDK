package com.youzan.nsq.client;

import com.youzan.nsq.client.entity.NSQConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * basic functions and utilities defined here
 * Created by lin on 16/9/24.
 */
public class AbstractNSQClientTestcase {

    private static final Logger logger = LoggerFactory.getLogger(AbstractNSQClientTestcase.class);
    Properties props = new Properties();
    protected final NSQConfig config = new NSQConfig();

    @BeforeClass
    public void init() throws IOException {
        logger.info("At {} , initialize: {}", System.currentTimeMillis(), this.getClass().getName());
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }
        final String env = props.getProperty("env");
        logger.debug("The environment is {} .", env);
        final String lookups = props.getProperty("lookup-addresses");
        final String connTimeout = props.getProperty("connectTimeoutInMillisecond");
        final String msgTimeoutInMillisecond = props.getProperty("msgTimeoutInMillisecond");
        final String threadPoolSize4IO = props.getProperty("threadPoolSize4IO");


        config.setLookupAddresses(lookups);
        config.setConnectTimeoutInMillisecond(Integer.valueOf(connTimeout));
        config.setMsgTimeoutInMillisecond(Integer.valueOf(msgTimeoutInMillisecond));
        config.setThreadPoolSize4IO(Integer.valueOf(threadPoolSize4IO));

        //for trace switch and lookupd update
        NSQConfig.setSDKEnvironment(props.getProperty("configAgentEnv"));
        NSQConfig.overrideConfigServerUrls(props.getProperty("urls"));
        NSQConfig.setConfigAgentBackupPath(props.getProperty("backupFilePath"));
        NSQConfig.tunrnOnConfigServerLookup();
    }

    public NSQConfig getNSQConfig(){
        return this.config;
    }

    //simple function to give you a producer
    public static Producer createProducer(final NSQConfig config){
        return new ProducerImplV2(config);
    }

    public static Consumer createConsumer(final NSQConfig config, final MessageHandler handler){
        return new ConsumerImplV2(config, handler);
    }
}
