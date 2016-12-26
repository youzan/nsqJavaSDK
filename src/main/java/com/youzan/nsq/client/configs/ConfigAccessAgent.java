package com.youzan.nsq.client.configs;

import com.youzan.nsq.client.PropertyNotFoundException;
import com.youzan.nsq.client.entity.NSQConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;
import java.util.SortedMap;

/**
 * config agent subscribes trace configs to confgi server and update new udpated trace configs response
 * Created by lin on 16/9/20.
 * Modified by lin on 16/10/26
 */
public abstract class ConfigAccessAgent implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(ConfigAccessAgent.class);

    //singleton instance&lock
    private static final Object LOCK = new Object();
    private static ConfigAccessAgent INSTANCE = null;

    private static Class<? extends ConfigAccessAgent> CAA_CLAZZ;
    //config access agent properties
    private static Properties props = null;

    //configs config file path for nsq sdk
    static final String NSQDCCCONFIGPRO = "nsq.sdk.configFilePath";
    private static final String CONFIG_ACCESS_CLASSNAME = "nsq.sdk.configAccessClass";

    public static ConfigAccessAgent getInstance() {
        if (null == INSTANCE) {
            synchronized (LOCK) {
                if (null == INSTANCE) {
                    initConfigAccessAgentProperties();
                    initConfigAccessAgentClass();
                    //read config client from static client config in NSQConfig, if either config or urls is specified, throws an exception
                    //create config request
                    try {
                        //As NSQConfig is invoked here, which means static variables like properties will be initialized before trace agent is invoked
                        Constructor<? extends ConfigAccessAgent> construactor = CAA_CLAZZ.getConstructor();
                        INSTANCE = construactor.newInstance();
                        INSTANCE.kickoff();
                    } catch (Exception e) {
                        logger.error("Fail to start config access agent {}. ", CAA_CLAZZ);
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return INSTANCE;
    }

    /**
     * release resources allocated by ConfigAccessAgent
     */
    private static void release() {
        if (null != INSTANCE) {
            synchronized (LOCK) {
                if (null != INSTANCE) {
                    INSTANCE = null;
                    CAA_CLAZZ = null;
                    props = null;
                }
            }
        }
    }

    private static void initConfigAccessAgentProperties() {
        InputStream is = null;
        try {
            is = NSQConfig.loadClientConfigInputStream();
            props = new Properties();
            props.load(is);
        } catch (FileNotFoundException configNotFoundE) {
            logger.error("Config properties for nsq sdk to configs not found. Make sure properties file located under {} system property", NSQDCCCONFIGPRO);
            throw new RuntimeException(configNotFoundE);
        } catch (IOException IOE) {
            logger.error("Could not load properties from nsq config properties.");
            throw new RuntimeException(IOE);
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                //swallow it
            }
        }
    }

    //load config access agent class according to configClient.properties
    private static void initConfigAccessAgentClass() {
        //touch NSQConfig to initialize sdk environment var
        String env = NSQConfig.getSDKEnv();
        try {
            //load class
            String clzName = props.getProperty(CONFIG_ACCESS_CLASSNAME);
            if (null == clzName) {
                throw new PropertyNotFoundException(CONFIG_ACCESS_CLASSNAME, "Could not find config access agent property " + CONFIG_ACCESS_CLASSNAME + ".");
            }
            try {
                CAA_CLAZZ = (Class<? extends ConfigAccessAgent>) ConfigAccessAgent.class.getClassLoader().loadClass(clzName);
            } catch (ClassNotFoundException e) {
                //try with context class loader
                CAA_CLAZZ = (Class<? extends ConfigAccessAgent>) Thread.currentThread().getContextClassLoader().loadClass(clzName);
                if (null == CAA_CLAZZ)
                    throw new ClassNotFoundException("ConfigAccessClass: " + clzName + " not found.");
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        logger.info("ConfigAccessAgentClass: {}", CAA_CLAZZ.getName());
    }

    public static String getProperty(String key) {
//        getInstance();
        return props.getProperty(key);
    }

    /**
     * Handle subscribe on pass in domain:key, register callback on values under domain:key updated.
     * Implementation should be threadsafe.
     *
     * @param domain   domain value of config, implementation varies.
     * @param keys     key values of config, implementation varies.
     * @param callback callback to handle new update items.
     * @return string[] first subscribe result
     */
    public abstract SortedMap<String, String> handleSubscribe(AbstractConfigAccessDomain domain, AbstractConfigAccessKey[] keys, final IConfigAccessCallback callback);

    public interface IConfigAccessCallback<T extends SortedMap<String, String>> {
        void process(T newItems);

        void fallback(T itemsInCache, Object... objs);
    }

    /**
     * start config access agent
     */
    protected abstract void kickoff();

    /**
     * release resource allocated to config access agent
     */
    abstract public void close();

    /**
     * return meta datas of config access agent, help in debugging
     * @return
     */
    abstract public String metadata();
}

