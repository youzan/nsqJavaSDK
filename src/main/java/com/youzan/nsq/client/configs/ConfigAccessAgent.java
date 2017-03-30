package com.youzan.nsq.client.configs;

import com.youzan.nsq.client.PropertyNotFoundException;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.ConfigAccessAgentException;
import com.youzan.nsq.client.exception.ConfigAccessAgentInitializeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.SortedMap;

/**
 * Config agent subscribes trace configs to config server and update new updated trace configs response
 * Created by lin on 16/9/20.
 * Modified by lin on 16/10/26
 */
public abstract class ConfigAccessAgent implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(ConfigAccessAgent.class);

    //singleton instance&lock
    private static final Object LOCK = new Object();
    private static ConfigAccessAgent INSTANCE = null;
    private static boolean TRIEDINITIALIZE = false;

    private static Class<? extends ConfigAccessAgent> CAA_CLAZZ;
    private static String env;
    private static String[] configAccessRemotes;
    private static String backupFilePath;
    //config access agent properties
    protected static Properties props = null;
    private volatile boolean connected = true;

    //configs config file path for nsq sdk
    private static final String NSQDCCCONFIGPRO = "nsq.sdk.configFilePath";
    private static final String CONFIG_ACCESS_CLASSNAME = "nsq.sdk.configAccessClass";

    //property of environment
    static final String NSQ_DCCCONFIG_ENV = "nsq.sdk.env";

    //default config file name, user is allow to use another by setting $nsq.configs.configFilePath
    private static final String dccConfigFile = "configClient.properties";

    public static void setConfigAccessAgentBackupPath(String path) {
        if(null != ConfigAccessAgent.backupFilePath){
            logger.info("Override backupPath {} to {}", ConfigAccessAgent.backupFilePath, path);
        }
        ConfigAccessAgent.backupFilePath = path;
    }

    public static String getConfigAccessAgentBackupPath() {
        return ConfigAccessAgent.backupFilePath;
    }

    /**
     * Set the environment value of config client
     * @param env environment value of config access remote, which config access agent talks to.
     */
    public static void setEnv(String env){
        if(null != ConfigAccessAgent.env) {
            logger.info("Override env {} to {}", ConfigAccessAgent.env, env);
        }
        ConfigAccessAgent.env = env;
    }

    public static String getEnv(){
        return ConfigAccessAgent.env;
    }

    public static ConfigAccessAgent getInstance() throws ConfigAccessAgentException {
        if (null == INSTANCE) {
            synchronized (LOCK) {
                if (null == INSTANCE) {
                    if(TRIEDINITIALIZE == true){
                        //if we hit this line, it means SDK used to initialze ConfigAccessAgent, but failed.
                        throw new ConfigAccessAgentInitializeException("Fail to initialize ConfigAccessAgent: " + CAA_CLAZZ);
                    }
                    //properties read from SDK, could be null, since we need to remove clientConfig.properties out of SDK.
                    initConfigAccessAgentProperties();
                    initConfigAccessAgentClass();
                    //read config client from static client config in NSQConfig, if either config or urls is specified, throws an exception
                    //create config request
                    try {
                        TRIEDINITIALIZE = true;
                        //As NSQConfig is invoked here, which means static variables like properties will be initialized before trace agent is invoked
                        Constructor<? extends ConfigAccessAgent> constructor = CAA_CLAZZ.getConstructor();
                        //invoke constructor of config access agent.
                        INSTANCE = constructor.newInstance();
                        INSTANCE.kickoff();
                    } catch (Exception e) {
                        logger.error("Fail to start config access agent {}. ", CAA_CLAZZ);
                        throw new ConfigAccessAgentInitializeException("Fail to initialize ConfigAccessAgent: " + CAA_CLAZZ);
                    }
                }
            }
        }
        return INSTANCE;
    }

    public boolean isConnected() {
        return this.connected;
    }

    public void setConnected(boolean connected) {
        this.connected = connected;
    }

    /**
     * Update config access urls. By default, NSQ sdk pick what is defined in nested client config properties file for
     * config access remote urls. if user set config access remotes with this function, sdk uses pass in config access
     * remotes before initialize config access agent.
     * Note: pls invoke this function BEFORE starting any NSQ client({@link com.youzan.nsq.client.Producer}, {@link com.youzan.nsq.client.Consumer})
     * @param configAccessRemotes config access remotes, separated by comma.
     */
    public static void setConfigAccessRemotes(String configAccessRemotes){
        if(null != configAccessRemotes) {
            String[] newConfigAccessRemotes = configAccessRemotes.split(",");
            if(null != ConfigAccessAgent.configAccessRemotes)
                logger.info("Override Config Access Remote URLs {} to {}", ConfigAccessAgent.configAccessRemotes, newConfigAccessRemotes);
            ConfigAccessAgent.configAccessRemotes = newConfigAccessRemotes;
        }
    }

    public static void setConfigAccessRemotes(String[] configAccessRemotes){
        if(null != configAccessRemotes && configAccessRemotes.length > 0) {
            if(null != ConfigAccessAgent.configAccessRemotes)
                logger.info("Override Config Access Remote URLs {} to {}", ConfigAccessAgent.configAccessRemotes, configAccessRemotes);
            ConfigAccessAgent.configAccessRemotes = configAccessRemotes;
        }
    }

    public static String[] getConfigAccessRemotes() {
        return ConfigAccessAgent.configAccessRemotes;
    }

    /**
     * release resources allocated by ConfigAccessAgent
     */
    public static void release() {
        if (null != INSTANCE) {
            synchronized (LOCK) {
                if (null != INSTANCE) {
                    INSTANCE.close();
                    TRIEDINITIALIZE = false;
                    CAA_CLAZZ = null;
                    props = null;
                    env = null;
                    configAccessRemotes = null;
                    backupFilePath = null;
                    INSTANCE = null;
                    logger.info("Config access agent released.");
                }
            }
        }
    }

    private static void initConfigAccessAgentProperties() {
        InputStream is = null;
        try {
            is = loadClientConfigInputStream();
            if(null != is) {
                props = new Properties();
                props.load(is);
            }
        } catch (FileNotFoundException configNotFoundE) {
            logger.error("Config properties for nsq sdk to configs not found. Make sure properties file located under {} system property", NSQDCCCONFIGPRO);
            throw new RuntimeException(configNotFoundE);
        } catch (IOException IOE) {
            logger.error("Could not load properties from nsq config properties.");
            throw new RuntimeException(IOE);
        } finally {
            try {
                if(null != is)
                    is.close();
            } catch (Exception e) {
                //swallow it
                logger.warn("Exception in closing client config input stream.", e.getLocalizedMessage());
            }
        }
    }

    /**
     *load config access agent class according to configClient.properties
     */
    private static void initConfigAccessAgentClass() {
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
                    throw new ClassNotFoundException("ConfigAccessClass: " + clzName + " not found. Fail to initialize config access agent.");
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        logger.info("ConfigAccessAgentClass: {}", CAA_CLAZZ.getName());
    }

    private static InputStream loadClientConfigInputStream(){
        //read from system config
        InputStream is = null;
        String dccConfigProPath = System.getProperty(NSQDCCCONFIGPRO);
        try {
            if (null != dccConfigProPath) {
                Path path = Paths.get(dccConfigProPath);
                is = new FileInputStream(path.toAbsolutePath().toString());
            } else
                logger.info("{} property from system not specified.", NSQDCCCONFIGPRO);
        }catch (FileNotFoundException e) {
            logger.warn("Could not find config access properties file from {} System property.", NSQDCCCONFIGPRO);
        }
        //try default location
        if(null == is){
            is = NSQConfig.class.getClassLoader()
                    .getResourceAsStream(dccConfigFile);
        }
        if(null == is) {
            logger.info("Config access properties from SDK config properties file not found.");
        }
        return is;
    }

    public static String getProperty(String key) {
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
     * return meta data of config access agent, help in debugging
     * @return metadata string
     */
    abstract public String metadata();
}
