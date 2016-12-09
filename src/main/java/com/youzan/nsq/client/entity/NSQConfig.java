package com.youzan.nsq.client.entity;

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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
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
    private static String sdkEnv;
    private boolean havingMonitoring = false;

    private enum Compression {
        NO_COMPRESSION, DEFLATE, SNAPPY
    }

    /**
     * One lookup cluster
     */
    private String[] lookupAddresses;
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
    //connection pool size for producer
    private int connectionSize = 10;
    private final String clientId;
    private final String hostname;
    private boolean featureNegotiation;
    private boolean slowStart = true;
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
    private int connectTimeoutInMillisecond = 2500;
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
     * ==========================configs properties=================================
     */
    //default config file name, user is allow to use another by setting $nsq.configs.configFilePath
    private static final String dccConfigFile = "configClient.properties";
    //configs config file path for nsq sdk
    private static final String NSQDCCCONFIGPRO = "nsq.sdk.configFilePath";
    //property of environemnt
    private static final String NSQ_DCCCONFIG_ENV = "nsq.sdk.env";

    /*-
     *  ==================configs configure-able objects, urls to configs and client config==========================
     *  make them static so that one process has unified configs config.
     */
    private static String[] urls;

    /*-
     * ==========================configs agent for lookup discovery=================
     */
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
        initSDKEnv();
    }

    @Deprecated
    /**
     * Deprecated. Pls uses {@link NSQConfig#NSQConfig(String, String)} for {@link com.youzan.nsq.client.Consumer}
     * or {@link com.youzan.nsq.client.Producer}, and
     */
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

    @SuppressWarnings("deprecation")
    /**
     * NSQConfig constructor for {@link com.youzan.nsq.client.Consumer} or {@link com.youzan.nsq.client.Producer}.
     * @param lookupAddress lookup address.
     * @param consumerName  channel name for consumer.
     */
    public NSQConfig(final String lookupAddress, final String consumerName) {
        this();
        //update lookup address
        setLookupAddresses(lookupAddress);
        //and channel name
        setConsumerName(consumerName);
    }

    @SuppressWarnings("deprecation")
    /**
     * NSQConfig constructor for {@link com.youzan.nsq.client.Producer}.
     * @param lookupAddress lookup address.
     * @param consumerName  channel name for consumer.
     */
    public NSQConfig(final String lookupAddress){
        this();
        setLookupAddresses(lookupAddress);
    }

    private static void initSDKEnv() {
        //read from system property first
        String envSys = System.getProperty(NSQ_DCCCONFIG_ENV);
        if(null == envSys) {
            //read from system config
            InputStream is = loadClientConfigInputStream();
            Properties props = new Properties();
            try {
                props.load(is);
                String env = props.getProperty(NSQ_DCCCONFIG_ENV);
                if(null == env)
                    logger.error("Could not find {} in config access file. Pls check configClient.properties file.");
                else {
                    sdkEnv = env;
                    logger.info("NSQ sdk works in {}.", sdkEnv);
                }
            } catch (IOException e) {
                logger.error("Could load NSQ sdk environment from config access properties file.", e);
            }
        }
    }

    public static InputStream loadClientConfigInputStream(){
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
            logger.error("Could not load config access properties from client config properties file.");
            throw new RuntimeException("Could not load config access properties from client config properties file.");
        }
        return is;
    }

    /**
     * Specified backup lookup address for user. By default, user specified lookup address is used when access to lookup
     * info from remote config server is invalid at the very beginning. If access to lookup break down in the middle of
     * nsq sdk process, cached lookup address takes precedence of backup lookup address.
     * @param lookupAddresses the lookupAddresses to set
     */
    public NSQConfig setLookupAddresses(final String lookupAddresses) {
        if(null == lookupAddresses || lookupAddresses.isEmpty()) {
            if(logger.isWarnEnabled())
                logger.warn("Empty lookup address is not accepted.");
            return this;
        }
        String[] newLookupAddresses = lookupAddresses.replaceAll(" ", "").split(",");
        Arrays.sort(newLookupAddresses);
        this.lookupAddresses = newLookupAddresses;
        return this;
    }

    /**
     * return user specified lookupaddresses
     * @return
     */
    public String[] getLookupAddresses() {
        return this.lookupAddresses;
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
    public NSQConfig setConnectTimeoutInMillisecond(int connectTimeoutInMillisecond) {
        this.connectTimeoutInMillisecond = connectTimeoutInMillisecond;
        return this;
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
    public NSQConfig setConsumerName(String consumerName) {
        this.consumerName = consumerName;
        return this;
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
    public NSQConfig setOrdered(boolean ordered) {
        this.ordered = ordered;
        return this;
    }

    /**
     * @return the threadPoolSize4IO
     */
    public int getThreadPoolSize4IO() {
        return threadPoolSize4IO;
    }

    /**
     * For specifying connection pool size for producer.
     * NOTE: setting threadPoolSizepls use {@link NSQConfig#setConnectionPoolSize(int)}.
     * @param threadPoolSize4IO the threadPoolSize4IO to set
     */
    public NSQConfig setThreadPoolSize4IO(int threadPoolSize4IO) {
        this.threadPoolSize4IO = threadPoolSize4IO;
        if (threadPoolSize4IO > 1) {
            logger.warn("SDK does not recommend the size > 1 when this client is a consumer. If you are a producer, please ignore the info.");
        }
        return this;
    }

    /**
     * Specify connection pool size for producer,
     * @param connectionPoolSize
     * @return current NSQConfig
     */
    public NSQConfig setConnectionPoolSize(int connectionPoolSize) {
        if(connectionPoolSize < 1) {
            logger.warn("SDK does not accept connection pool size which smaller than 1.");
            return this;
        }
        this.connectionSize = connectionPoolSize;
        return this;
    }

    /**
     * @return connection
     */
    public int getConnectionSize(){
        return this.connectionSize;
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
    public NSQConfig setMsgTimeoutInMillisecond(int msgTimeoutInMillisecond) {
        this.msgTimeoutInMillisecond = msgTimeoutInMillisecond;
        return this;
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
    public NSQConfig setHeartbeatIntervalInMillisecond(Integer heartbeatIntervalInMillisecond) {
        this.heartbeatIntervalInMillisecond = heartbeatIntervalInMillisecond;
        return this;
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
    public NSQConfig setFeatureNegotiation(boolean featureNegotiation) {
        this.featureNegotiation = featureNegotiation;
        return this;
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
    public NSQConfig setOutputBufferSize(Integer outputBufferSize) {
        this.outputBufferSize = outputBufferSize;
        return this;
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
    public NSQConfig setOutputBufferTimeoutInMillisecond(Integer outputBufferTimeoutInMillisecond) {
        this.outputBufferTimeoutInMillisecond = outputBufferTimeoutInMillisecond;
        return this;
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
    public NSQConfig setHavingMonitoring(boolean havingMonitoring) {
        this.havingMonitoring = havingMonitoring;
        return this;
    }

    public SslContext getSslContext() {
        return sslContext;
    }

    public NSQConfig setSslContext(SslContext sslContext) {
        if (null == sslContext) {
            throw new NullPointerException();
        }
        tlsV1 = true;
        this.sslContext = sslContext;
        return this;
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
    public NSQConfig setTlsV1(boolean tlsV1) {
        this.tlsV1 = tlsV1;
        return this;
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
    public NSQConfig setCompression(Compression compression) {
        this.compression = compression;
        return this;
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
    public NSQConfig setDeflateLevel(Integer deflateLevel) {
        this.deflateLevel = deflateLevel;
        return this;
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
    public NSQConfig setSampleRate(Integer sampleRate) {
        this.sampleRate = sampleRate;
        return this;
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
    public NSQConfig setRdy(int rdy) {
        if (rdy <= 0) {
            this.rdy = 1;
            throw new IllegalArgumentException("Are you kidding me? The rdy should be positive.");
        }
        this.rdy = rdy;
        return this;
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
    public NSQConfig setQueryTimeoutInMillisecond(int queryTimeoutInMillisecond) {
        this.queryTimeoutInMillisecond = queryTimeoutInMillisecond;
        return this;
    }

    private static Logger getLogger() {
        return logger;
    }

    /**
     * set the environment value of config client
     * @param env
     */
    static void setSDKEnv(String env){
        sdkEnv = env;
    }

    public static String getSDKEnv(){
        return sdkEnv;
    }

    public static String[] getConfigServerUrls(){
        return urls;
    }

    public boolean isConsumerSlowStart() {
        return this.slowStart;
    }

    public NSQConfig setConsumerSlowStart(boolean allowSlowStart) {
        this.slowStart = allowSlowStart;
        return this;
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
