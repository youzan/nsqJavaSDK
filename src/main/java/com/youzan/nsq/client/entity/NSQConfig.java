package com.youzan.nsq.client.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.youzan.dcc.client.ConfigClient;
import com.youzan.dcc.client.ConfigClientBuilder;
import com.youzan.dcc.client.entity.config.Config;
import com.youzan.dcc.client.entity.config.ConfigRequest;
import com.youzan.dcc.client.entity.config.interfaces.IResponseCallback;
import com.youzan.dcc.client.exceptions.ConfigParserException;
import com.youzan.dcc.client.exceptions.InvalidConfigException;
import com.youzan.dcc.client.util.inetrfaces.ClientConfig;
import com.youzan.nsq.client.Version;
import com.youzan.util.HostUtil;
import com.youzan.util.IPUtil;
import com.youzan.util.NotThreadSafe;
import com.youzan.util.SystemUtil;
import io.netty.handler.ssl.SslContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * It is used for Producer or Consumer, and not both two.
 *
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
@NotThreadSafe
public class NSQConfig implements java.io.Serializable, Cloneable {

    private static final long serialVersionUID = 6624842850216901700L;
    private static final Logger logger = LoggerFactory.getLogger(NSQConfig.class);

    private static final AtomicInteger id = new AtomicInteger(0);

    private boolean havingMonitoring = false;

    private enum Compression {
        NO_COMPRESSION, DEFLATE, SNAPPY
    }

    /**
     * One lookup cluster
     */
    private String[] backupLookupAddresses;
    private volatile static Timestamp lookupAddrUpdated  = new Timestamp(0L);
    private static String[] lookupAddresses;
    /**
     * In NSQ, it is a channel.
     */
    private String consumerName;
    /**
     * The set of messages is ordered in one specified partition
     */
    private boolean ordered = false;
    /**
     * <pre>
     * Set the thread_pool_size for IO running.
     * It is also used for Netty.
     * </pre>
     */
    private int threadPoolSize4IO = 1;
    private final String clientId;
    private final String hostname;
    private boolean featureNegotiation;

    /*-
     * =========================================================================
     *                             All of Timeout
     */
    /**
     * the message interactive timeout between client and server
     */
    private int msgTimeoutInMillisecond = 60 * 1000;
    /**
     * Perform a TCP connecting action
     */
    private int connectTimeoutInMillisecond = 500;
    /**
     * Perform one interactive action between request and response underlying
     * Netty handling TCP
     */
    private int queryTimeoutInMillisecond = 5000;
    /**
     * the timeout after which any data that NSQd has buffered will be flushed
     * to this client
     */
    private Integer outputBufferTimeoutInMillisecond = null;
    /*-
     *                             All of Timeout
     * =========================================================================
     */

    /*-
     * ==========================dcc properties=================================
     */
    //default config file name, user is allow to use another by setting $nsq.dcc.configFilePath
    private static String envOverride;
    private static String dccUrlsOverride;
    private static final String dccConfigFile = "configClient.properties";
    //dcc config file path for nsq sdk
    private static final String NSQDCCCONFIGPRO = "nsq.dcc.configFilePath";
    //property urls to dcc remote
    private static final String NSQ_DCCCONFIG_URLS = "nsq.dcc.%s.urls";
    //property of backup file path
    private static final String NSQ_DCCCONFIG_BACKUP_PATH = "nsq.dcc.backupPath";
    //property of environemnt
    private static final String NSQ_DCCCONFIG_ENV = "nsq.sdk.env";

    //app value dcc client need to specify to fetch lookupd config from dcc
    private static final String NSQ_APP_VAL_PRO = "nsq.app.val";
    //default value for app value
    private static final String DEFAULT_NSQ_APP_VAL = "nsq";
    public static String NSQ_APP_VAL = null;

    //key value dcc client need to specify to fetch lookupd config from dcc
    private static final String NSQ_LOOKUP_KEY_PRO = "nsq.lookupd.addr.key";
    //default value for key value
    private static final String DEFAULT_NSQ_LOOKUP_KEY = "lookupd.addr";
    private static String NSQ_LOOKUP_KEY = null;

    private static final String NSQ_TOPIC_TRACE_PRO = "nsq.topic.trace.key";
    private static final String DEFAULT_NSQ_TOPIC_TRACE = "nsq.topic.trace";
    public static String NSQ_TOPIC_TRACE = null;


    /*-
     *  ==================dcc configure-able objects, urls to dcc and client config==========================
     *  make them static so that one process has unified dcc config.
     */
    private static String[] urls;

    /*-
     * by default, clientConfig overrides what is defined in ConfigClientBuilder.
     * in nsq's case there is two interface to configure dcc:
     * 1. use static functions, NSQConfig.setUrls() and so on, this could be specified by user in code;
     * 2. use properties file, this is processed in the initialized of NSQConfig class
     *
     * As #2 take precedence over #1, which implements user specified function overrides system's default.
     * while #2 use interface in #1 actually.
     */
    private static ClientConfig dccConfig = new ClientConfig();


    /*-
     * ==========================dcc agent for lookup discovery=================
     */
    private static final Object lookupSubscriberLock = new Object();
    private static ConfigClient lookupSubscriber;
    private static boolean kickOff = false;
    private static boolean compressTrace = false;
    private static volatile boolean dccOn = false;


    private Integer heartbeatIntervalInMillisecond = null;

    private Integer outputBufferSize = null;

    private boolean tlsV1 = false;
    private Integer deflateLevel = null;
    private Integer sampleRate = null;
    private final String userAgent = "Java/com.youzan/" + Version.ARTIFACT_ID + "/" + Version.VERSION;
    private Compression compression = Compression.NO_COMPRESSION;
    // ...
    private SslContext sslContext = null;
    private int rdy = 3;

    //Use system properties to initialize config client config
    static {
        initDCCProperties();
    }

    public NSQConfig() {
        try {
            hostname = HostUtil.getLocalIP();
            // JDK8, string contact is OK.
            clientId = "IP:" + IPUtil.ipv4(hostname) + ", PID:" + SystemUtil.getPID() + ", ID:"
                    + (id.getAndIncrement());
        } catch (Exception e) {
            throw new RuntimeException("System can't get the IPv4!", e);
        }
    }

    /**
     * Turn on lookup config server switch. nsq sdk will check lookup addresses and topic trace from config server.
     * Default lookup config server option is off, and sdk uses lookup addresses
     * specified by user via {@link NSQConfig#setLookupAddresses(String)}.
     */
    public static void tunrnOnConfigServerLookup(){
        dccOn = true;
    }

    public static boolean isConfigServerLookupOn(){
        return dccOn;
    }

    /**
     * Specify environment of current nsq sdk. If sdk env is not specified, default environment "prod" is choosed.
     * @param evnStr environment variable of nsq
     */
    public static void setSDKEnvironment(final String evnStr){
        envOverride = evnStr;
        setConfigAgentEnv(envOverride);
        //update dcc urls accordlly
        InputStream is = null;
        try {
            is = loadClientConfigInputStream();
            Properties props = new Properties();
            props.load(is);
            String env = props.getProperty(NSQConfig.NSQ_DCCCONFIG_ENV);
            assert null != envOverride;
            setConfigAgentEnv(envOverride);

            String urlsKey = String.format(NSQ_DCCCONFIG_URLS, envOverride);

            String urls = props.getProperty(urlsKey);
            assert null != urls;
            setUrls(urls);
        } catch(FileNotFoundException configNotFoundE) {
            logger.warn("Config properties for nsq sdk to dcc not found. Make sure properties file located under {} system property", NSQDCCCONFIGPRO);
        } catch (IOException IOE) {
            logger.info("Could not load properties from nsq config properties.", IOE);
        }
    }

    /**
     *Override config server urls of sdk's default for current environment. By default, nsq sdk picks config server's
     * urls according to environment variable.
     * @param urls
     */
    public static void overrideConfigServerUrls(final String urls){
        dccUrlsOverride = urls;
        setUrls(dccUrlsOverride);
    }

    private static InputStream loadClientConfigInputStream() throws FileNotFoundException {
        InputStream is = NSQConfig.class.getClassLoader()
                .getResourceAsStream(dccConfigFile);
        if(null == is){
            //read from system config
            String dccConfigProPath = System.getProperty(NSQDCCCONFIGPRO);
            if (null != dccConfigProPath) {
                is = new FileInputStream(dccConfigProPath);
            }else
                logger.warn("Could not find {} from system.", NSQDCCCONFIGPRO);
        }
        return is;
    }

    /**
     * function to initialize dcc lookup properties, function tries to file something to read properties from in
     * following order, default config file name is "clientConfig.properties":
     * 1. try search default config file in classpath;
     * 2. If #1 fails, try get config file path from system properties, the try loading properties from that path
     */
    private static void initDCCProperties(){
        InputStream is = null;
        try {
            is = loadClientConfigInputStream();
            if (null != is) {
                initClientConfig(is);
            }else
                logger.warn("Could load properties for config server access configuration. User needs to specify them in NSQConfig.");
        }catch (FileNotFoundException configNotFoundE) {
            logger.warn("Config properties for nsq sdk to dcc not found. Make sure properties file located under {} system property", NSQDCCCONFIGPRO);
        } catch (IOException IOE) {
            logger.info("Could not load properties from nsq config properties.", IOE);
        }finally {
            try {
                is.close();
            } catch (IOException e) {
                //swallow it
            }
        }
    }

    private static void initClientConfig(final InputStream is) throws IOException {
        Properties props = new Properties();
        props.load(is);
        //initialize lookup app keys in dcc
        String app = props.getProperty(NSQConfig.NSQ_APP_VAL_PRO);
        if(null != app)
            NSQConfig.NSQ_APP_VAL = app;
        else
            NSQConfig.NSQ_APP_VAL = NSQConfig.DEFAULT_NSQ_APP_VAL;
        logger.info("{}:{}", NSQConfig.NSQ_APP_VAL_PRO, NSQConfig.NSQ_APP_VAL);

        String key = props.getProperty(NSQConfig.NSQ_LOOKUP_KEY_PRO);
        if(null != key)
            NSQConfig.NSQ_LOOKUP_KEY = key;
        else
            NSQConfig.NSQ_LOOKUP_KEY = NSQConfig.DEFAULT_NSQ_LOOKUP_KEY;
        logger.info("{}:{}", NSQConfig.NSQ_LOOKUP_KEY_PRO, NSQConfig.NSQ_LOOKUP_KEY);

        //initialize dcc config properties in nsq dcc config file
        String backupPath = props.getProperty(NSQConfig.NSQ_DCCCONFIG_BACKUP_PATH);
        assert null != backupPath;
        setConfigAgentBackupPath(backupPath);

        String env = props.getProperty(NSQConfig.NSQ_DCCCONFIG_ENV);
        assert null != env;
        setConfigAgentEnv(env);

        String urlsKey = String.format(NSQ_DCCCONFIG_URLS, env);

        String urls = props.getProperty(urlsKey);
        assert null != urls;
        setUrls(urls);

        //trace config properties
        String topicTrace = props.getProperty(NSQConfig.NSQ_TOPIC_TRACE_PRO);
        if(null != topicTrace)
            NSQConfig.NSQ_TOPIC_TRACE = topicTrace;
        else
            NSQConfig.NSQ_TOPIC_TRACE = NSQConfig.DEFAULT_NSQ_TOPIC_TRACE;
        logger.info("{}:{}", NSQConfig.NSQ_TOPIC_TRACE_PRO, NSQConfig.NSQ_TOPIC_TRACE);
    }

    private static void specifyEnv(String env){

    }

    //synchronization is performed outside
    private void initLookupSubscriber() throws IOException {
        lookupSubscriber = ConfigClientBuilder.create()
                .setRemoteUrls(getUrls())
                .setBackupFilePath(getConfigAgentBackupPath())
                .setConfigEnvironment(getConfigAgentEnv())
                .setClientConfig(dccConfig)
                .build();
        if(logger.isInfoEnabled())
            logger.info("nsq dcc config client initialized.");
    }

    /**
     * kick off subscribing on lookupd, this is a synchronized process
     */
    private void kickOff() {
        if(!kickOff && dccOn) {
            synchronized (lookupSubscriberLock) {
                if(!kickOff && dccOn) {
                    try {
                        //initialize dcc client
                        initLookupSubscriber();

                        //build config request
                        ConfigRequest request = null;
                        try {
                            request = (ConfigRequest) ConfigRequest.create(lookupSubscriber)
                                    .setApp(null == NSQ_APP_VAL ? DEFAULT_NSQ_APP_VAL : NSQ_APP_VAL)
                                    .setKey(null == NSQ_LOOKUP_KEY ? DEFAULT_NSQ_LOOKUP_KEY : DEFAULT_NSQ_LOOKUP_KEY)
                                    .build();
                            if(logger.isInfoEnabled()){
                                logger.info("Lookupd config request: {}", request.getContent());
                            }
                        } catch (ConfigParserException e) {
                            logger.error("Error on building config request. Pls contact nsq dev.", e);
                        }
                        List<ConfigRequest> requests = new ArrayList<>();
                        requests.add(request);
                        if (null != lookupSubscriber) {
                            List<Config> newConfigs = lookupSubscriber.subscribe(new IResponseCallback() {
                                @Override
                                public void onChanged(List<Config> list) throws Exception {
                                    parse2LookupdList(list);
                                }

                                @Override
                                public void onFailed(List<Config> list, Exception e) throws Exception {
                                    logger.warn("Error in connection to dcc remote.", e);
                                }
                            }, requests);
                            if(null != newConfigs && !newConfigs.isEmpty()) {
                                parse2LookupdList(newConfigs);
                                kickOff = true;
                            }
                            if(logger.isInfoEnabled())
                                logger.info("Lookup subscriber starts.");
                        }
                    } catch (IOException e) {
                        logger.warn("NSQ dcc config agent could not be initialized. Pls check pass in nsq dcc config properties.");
                    } catch (InvalidConfigException e) {
                        logger.error("Error parsing configs into lookup addresses. Pls contact nsq dev.", e);
                    } catch (Exception e) {
                        if(!compressTrace) {
                            logger.warn("Fail to subscribe to lookupd address.", e);
                            compressTrace = Boolean.TRUE;
                        }else
                            logger.warn("Fail to subscribe to lookupd address.");
                    }
                }
            }
        }

    }

    private void parse2LookupdList(final List<Config> configs) throws InvalidConfigException, IOException {
        assert null != configs;
        //update look up addresses
        Config lookupConfig = configs.get(0);
        assert null != lookupConfig;
        JsonNode root = SystemUtil.getObjectMapper().readTree(lookupConfig.getContent());
        JsonNode array = root.get("value");
        List<String> newLookupds = new ArrayList<>();
        for(JsonNode node : array){
            newLookupds.add(node.get("value").asText());
        }
        updateLookupAddresses(newLookupds.toArray(new String[newLookupds.size()]));
    }

    private static void updateLookupAddresses(final String[] newLookupList){
        NSQConfig.lookupAddresses = newLookupList;
        NSQConfig.lookupAddrUpdated.setTime(System.currentTimeMillis());
    }

    /**
     * Get lookup addresses, based on pass in lastUpdateTimestamp.
     * If lookup addresses is updated after specified timestamp, lookup addresses returns. If subscriber to config
     * server is not initialized(often caused by config), user specified lookup address by
     * {@link NSQConfig#setLookupAddresses(String)}, will be always returned, until subscriber kicks off.
     *
     * @param updateTimeStamp timestamp to compare with, function returns lookup address when it is updated after
     *                        updateTimestamp.
     * @return the lookupAddresses
     */
    public String[] getLookupAddresses(Timestamp updateTimeStamp) {
        kickOff();
        if(!NSQConfig.kickOff || !dccOn){
            if(logger.isDebugEnabled())
                logger.debug("lookup addresses from config server not available. Use backup look up address passed in by user.");
            //try with backup lookup address
            return this.backupLookupAddresses;
        } else if(NSQConfig.lookupAddrUpdated.after(updateTimeStamp)){
            updateTimeStamp.setTime(NSQConfig.lookupAddrUpdated.getTime());
            return NSQConfig.lookupAddresses;
        }
        //lookup info is not updated
        return null;
    }

    /**
     * Specified backup lookup address for user. By default, user specified lookup address is used when access to lookup
     * info from remote config server is invalid at the very beginning. If access to lookup break down in the middle of
     * nsq sdk process, cached lookup address takes precedence of backup lookup address.
     * @param lookupAddresses the lookupAddresses to set
     */
    public void setLookupAddresses(final String lookupAddresses) {
        this.backupLookupAddresses = lookupAddresses.trim().replaceAll(" ", "").split(",");
    }

    /**
     * @return the connectTimeoutInMillisecond
     */
    public int getConnectTimeoutInMillisecond() {
        return connectTimeoutInMillisecond;
    }

    /**
     * @param connectTimeoutInMillisecond the connectTimeoutInMillisecond to set
     */
    public void setConnectTimeoutInMillisecond(int connectTimeoutInMillisecond) {
        this.connectTimeoutInMillisecond = connectTimeoutInMillisecond;
    }

    /**
     * @return the serialversionuid
     */
    public static long getSerialversionuid() {
        return serialVersionUID;
    }

    /**
     * @return the consumerName
     */
    public String getConsumerName() {
        return consumerName;
    }

    /**
     * @param consumerName the consumerName to set
     */
    public void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
    }

    /**
     * @return the ordered
     */
    public boolean isOrdered() {
        return ordered;
    }

    /**
     * @param ordered the ordered to set
     */
    public void setOrdered(boolean ordered) {
        this.ordered = ordered;
    }

    /**
     * @return the threadPoolSize4IO
     */
    public int getThreadPoolSize4IO() {
        return threadPoolSize4IO;
    }

    /**
     * @param threadPoolSize4IO the threadPoolSize4IO to set
     */
    public void setThreadPoolSize4IO(int threadPoolSize4IO) {
        this.threadPoolSize4IO = threadPoolSize4IO;
        if (threadPoolSize4IO > 1) {
            logger.warn("SDK does not recommend the size > 1.");
        }
    }

    /**
     * @return the clientId
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * @return the hostname
     */
    public String getHostname() {
        return hostname;
    }

    /**
     * @return the msgTimeoutInMillisecond
     */
    public int getMsgTimeoutInMillisecond() {
        return msgTimeoutInMillisecond;
    }

    /**
     * @param msgTimeoutInMillisecond the msgTimeoutInMillisecond to set
     */
    public void setMsgTimeoutInMillisecond(int msgTimeoutInMillisecond) {
        this.msgTimeoutInMillisecond = msgTimeoutInMillisecond;
    }

    /**
     * the server's heartbeat max-interval is 60 sec
     *
     * @return the heartbeatIntervalInMillisecond
     */
    public Integer getHeartbeatIntervalInMillisecond() {
        final int max = 50 * 1000;
        if (heartbeatIntervalInMillisecond == null) {
            return Math.min(Integer.valueOf(getMsgTimeoutInMillisecond() / 3), max);
        }
        return Math.min(heartbeatIntervalInMillisecond, max);
    }

    /**
     * @param heartbeatIntervalInMillisecond the heartbeatIntervalInMillisecond to set
     */
    public void setHeartbeatIntervalInMillisecond(Integer heartbeatIntervalInMillisecond) {
        this.heartbeatIntervalInMillisecond = heartbeatIntervalInMillisecond;
    }

    /**
     * @return the featureNegotiation
     */
    public boolean isFeatureNegotiation() {
        return featureNegotiation;
    }

    /**
     * @param featureNegotiation the featureNegotiation to set
     */
    public void setFeatureNegotiation(boolean featureNegotiation) {
        this.featureNegotiation = featureNegotiation;
    }

    /**
     * @return the outputBufferSize
     */
    public Integer getOutputBufferSize() {
        return outputBufferSize;
    }

    /**
     * @param outputBufferSize the outputBufferSize to set
     */
    public void setOutputBufferSize(Integer outputBufferSize) {
        this.outputBufferSize = outputBufferSize;
    }

    /**
     * @return the outputBufferTimeoutInMillisecond
     */
    public Integer getOutputBufferTimeoutInMillisecond() {
        return outputBufferTimeoutInMillisecond;
    }

    /**
     * @param outputBufferTimeoutInMillisecond the outputBufferTimeoutInMillisecond to set
     */
    public void setOutputBufferTimeoutInMillisecond(Integer outputBufferTimeoutInMillisecond) {
        this.outputBufferTimeoutInMillisecond = outputBufferTimeoutInMillisecond;
    }

    /**
     * @return the userAgent
     */
    public String getUserAgent() {
        return userAgent;
    }

    /**
     * @return the havingMonitoring
     */
    public boolean isHavingMonitoring() {
        return havingMonitoring;
    }

    /**
     * @param havingMonitoring the havingMonitoring to set
     */
    public void setHavingMonitoring(boolean havingMonitoring) {
        this.havingMonitoring = havingMonitoring;
    }

    public SslContext getSslContext() {
        return sslContext;
    }

    public void setSslContext(SslContext sslContext) {
        if (null == sslContext) {
            throw new NullPointerException();
        }
        tlsV1 = true;
        this.sslContext = sslContext;
    }

    /**
     * @return the tlsV1
     */
    public boolean isTlsV1() {
        return tlsV1;
    }

    /**
     * @param tlsV1 the tlsV1 to set
     */
    public void setTlsV1(boolean tlsV1) {
        this.tlsV1 = tlsV1;
    }

    /**
     * @return the compression
     */
    public Compression getCompression() {
        return compression;
    }

    /**
     * @param compression the compression to set
     */
    public void setCompression(Compression compression) {
        this.compression = compression;
    }

    /**
     * @return the deflateLevel
     */
    public Integer getDeflateLevel() {
        return deflateLevel;
    }

    /**
     * @param deflateLevel the deflateLevel to set
     */
    public void setDeflateLevel(Integer deflateLevel) {
        this.deflateLevel = deflateLevel;
    }

    /**
     * @return the sampleRate
     */
    public Integer getSampleRate() {
        return sampleRate;
    }

    /**
     * @param sampleRate the sampleRate to set
     */
    public void setSampleRate(Integer sampleRate) {
        this.sampleRate = sampleRate;
    }

    /**
     * @return the rdy
     */
    public int getRdy() {
        return rdy;
    }

    /**
     * @param rdy the rdy to set , it is ready to receive the pushing message
     *            count
     */
    public void setRdy(int rdy) {
        if (rdy <= 0) {
            this.rdy = 1;
            throw new IllegalArgumentException("Are you kidding me? The rdy should be positive.");
        }
        this.rdy = rdy;
    }

    /**
     * @return the queryTimeoutInMillisecond
     */
    public int getQueryTimeoutInMillisecond() {
        return queryTimeoutInMillisecond;
    }

    /**
     * @param queryTimeoutInMillisecond the queryTimeoutInMillisecond to set
     */
    public void setQueryTimeoutInMillisecond(int queryTimeoutInMillisecond) {
        this.queryTimeoutInMillisecond = queryTimeoutInMillisecond;
    }

    private static Logger getLogger() {
        return logger;
    }

    /**
     * set the environment value of config client
     * @param env
     */
    static void setConfigAgentEnv(String env){
        dccConfig.setProperty(ConfigClient.KEY_ENV, env);
    }

    public static String getConfigAgentEnv(){
        return (String) dccConfig.readConfig(ConfigClient.KEY_ENV, null);
    }

    public static void setConfigAgentBackupPath(String backupPath){
        dccConfig.setProperty(ConfigClient.KEY_BACKUP_PATH, backupPath);
    }

    private String getConfigAgentBackupPath(){
        return (String) dccConfig.readConfig(ConfigClient.KEY_BACKUP_PATH, null);
    }

    public static ClientConfig getTraceAgentConfig(){
        return dccConfig;
    }


    /**
     * specify config remote server urls, passin String is a list of urls separated by comma
     * @param urlsStr
     */
    static void setUrls(String urlsStr){
        String[] passinUrls = urlsStr.replaceAll(" ", "").split(",");
        urls = passinUrls;
    }

    public static String[] getUrls(){
        return urls;
    }

    /**
     * @return the id
     */
    public static AtomicInteger getId() {
        return id;
    }

    public String identify() {
        final StringBuffer buffer = new StringBuffer(300);
        buffer.append("{\"client_id\":\"" + clientId + "\", ");
        buffer.append("\"hostname\":\"" + hostname + "\", ");
        buffer.append("\"feature_negotiation\": true, ");
        if (outputBufferSize != null) {
            buffer.append("\"output_buffer_size\":" + outputBufferSize + ", ");
        }
        if (outputBufferTimeoutInMillisecond != null) {
            buffer.append("\"output_buffer_timeout\":" + outputBufferTimeoutInMillisecond.toString() + ", ");
        }
        if (tlsV1) {
            buffer.append("\"tls_v1\":" + tlsV1 + ", ");
        }
        if (compression == Compression.SNAPPY) {
            buffer.append("\"snappy\": true, ");
        }
        if (compression == Compression.DEFLATE) {
            buffer.append("\"deflate\": true, ");
            if (deflateLevel != null) {
                buffer.append("\"deflate_level\":" + deflateLevel.toString() + ", ");
            }
        }
        if (sampleRate != null) {
            buffer.append("\"sample_rate\":" + sampleRate.toString() + ",");
        }
        if (getHeartbeatIntervalInMillisecond() != null) {
            buffer.append("\"heartbeat_interval\":" + String.valueOf(getHeartbeatIntervalInMillisecond()) + ", ");
        }
        buffer.append("\"msg_timeout\":" + String.valueOf(msgTimeoutInMillisecond) + ",");
        buffer.append("\"user_agent\": \"" + userAgent + "\"}");
        return buffer.toString();
    }

    @Override
    public Object clone() {
        NSQConfig newCfg = null;
        try {
            newCfg = (NSQConfig) super.clone();
        } catch (CloneNotSupportedException e) {
            logger.error("Exception", e);
        }
        assert newCfg != null;
        return newCfg;
    }

}
