package com.youzan.nsq.client;

import com.youzan.nsq.client.configs.ConfigAccessAgent;
import com.youzan.nsq.client.core.LookupAddressUpdate;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.ConfigAccessAgentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;

/**
 * basic functions and utilities defined here
 * Created by lin on 16/9/24.
 */
public class AbstractNSQClientTestcase {

    private static final Logger logger = LoggerFactory.getLogger(AbstractNSQClientTestcase.class);
    Properties props = new Properties();
    protected NSQConfig config;

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

        config = new NSQConfig();
        config.setLookupAddresses(lookups);
        config.setConnectTimeoutInMillisecond(Integer.valueOf(connTimeout));
        config.setMsgTimeoutInMillisecond(Integer.valueOf(msgTimeoutInMillisecond));
        config.setThreadPoolSize4IO(Integer.valueOf(threadPoolSize4IO));
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

    public static LookupAddressUpdate createLookupAddressUpdateInstance() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Class claz = LookupAddressUpdate.class;
        Constructor con = claz.getConstructor();
        con.setAccessible(true);
        LookupAddressUpdate lau = (LookupAddressUpdate) con.newInstance();
        return lau;
    }

    @AfterMethod
    public void relase() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, ConfigAccessAgentException {
        Class<ConfigAccessAgent> clazz = ConfigAccessAgent.class;
        Method method = clazz.getDeclaredMethod("release");
        method.setAccessible(true);
        method.invoke(ConfigAccessAgent.getInstance());
    }
}
