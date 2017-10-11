package com.youzan.nsq.client.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.youzan.nsq.client.Version;
import com.youzan.util.HostUtil;
import com.youzan.util.NotThreadSafe;
import com.youzan.util.SystemUtil;
import io.netty.handler.ssl.SslContext;
import org.apache.commons.collections.map.UnmodifiableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * It is used for Producer or Consumer, and not both two.
 *
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
@NotThreadSafe
public class NSQConfig implements java.io.Serializable, Cloneable {

    private static final long serialVersionUID = 6624842850216901700L;
    private static final Logger logger = LoggerFactory.getLogger(NSQConfig.class);
    private static final ObjectMapper MAPPER_CONFIG = new ObjectMapper();
    static {
        MAPPER_CONFIG.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    /**
     * parse nsqconfig meta info
     * @param config
     */
    public static String parseNSQConfig(final NSQConfig config) {
        String configJson = null;
        try {
            configJson = MAPPER_CONFIG.writeValueAsString(config);
        }catch (Throwable e) {
            logger.error("fail to parse NSQConfig to string", e);
        }finally {
            return configJson;
        }
    }

    private IExpectedRdyUpdatePolicy DEFAULT_EXP_RDY_POLICY = new IExpectedRdyUpdatePolicy() {
        @Override
        public int expectedRdyIncrease(int currentExpRdy, int expectedRdyConf) {
            int newExpRdy = (int)(Math.round(currentExpRdy * 1.5));
            if(newExpRdy <= expectedRdyConf)
                return newExpRdy;
            else {
                return expectedRdyConf;
            }
        }

        @Override
        public int expectedRdyDecline(int currentExpRdy, int expectedRdy) {
            int newExpRdy = (int)(currentExpRdy - Math.round(currentExpRdy * 0.25));
            if(newExpRdy > 0) {
                return newExpRdy;
            } else {
                return 1;
            }
        }
    };
    private volatile IExpectedRdyUpdatePolicy expRdyUpdatePolicy = DEFAULT_EXP_RDY_POLICY;

    /**
     * specify {@link IExpectedRdyUpdatePolicy} for current config. {@link com.youzan.nsq.client.Consumer}
     * which has current config applied uses expected rdy update poliy for increase/decline expected rdy.
     * @param policyClass class name of policy class
     */
    public void setExpectedRdyUpdatePolicy(String policyClass) {
        try {
            Class<?> clazz;
            try {
                clazz = Class.forName(policyClass, true,
                        Thread.currentThread().getContextClassLoader());
            } catch (ClassNotFoundException e) {
                clazz = Class.forName(policyClass);
            }
            Object policy = clazz.newInstance();
            if (policy instanceof IRdyUpdatePolicy) {
                @SuppressWarnings("unchecked")
                        IExpectedRdyUpdatePolicy expRdyPolicy = (IExpectedRdyUpdatePolicy) policy;
                this.expRdyUpdatePolicy = expRdyPolicy;
            }
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(
                    "Unable to create ExpectedRdyUpdatePolicy instance of type " +
                            policyClass, e);
        } catch (InstantiationException e) {
            throw new IllegalArgumentException(
                    "Unable to create ExpectedRdyUpdatePolicy instance of type " +
                            policyClass, e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException(
                    "Unable to create ExpectedRdyUpdatePolicy instance of type " +
                            policyClass, e);
        }
    }

    public IExpectedRdyUpdatePolicy getExpectedRdyUpdatePolicy() {
        return this.expRdyUpdatePolicy;
    }


    private boolean havingMonitoring = false;

    public enum Compression {
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
     * </pre>
     */
    private int threadPoolSize4IO = Runtime.getRuntime().availableProcessors() * 2;
    private int nettyPoolSize = Runtime.getRuntime().availableProcessors() * 2;
    private int consumerWorkerPoolSize = Runtime.getRuntime().availableProcessors() * 4;
    //connection pool size for producer
    private int connectionSize = 30;
    private final String clientId;
    private final String hostname;
    private boolean featureNegotiation;
    private boolean userSpecifiedLookupd = false;
    private Map<String, String> localTraceMap = new ConcurrentHashMap<>();
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
    private int queryTimeoutInMillisecond = 3000;
    /**
     * query timeout for topic based seed lookup address check
     */
    private static int queryTimeout4TopicSeedInMillisecond = 500;

    /**
     * interval between two list lookup operation for all seed lookups
     */
    private static int listLookupIntervalInSecond = 60;

    private static String[] configAccessURLs;

    private static String configAccessEnv;

    /**
     * @Deprecated
     * interval base for producer retry interval
     */
    private int producerRetryIntervalBase = 100;

    /**
     * the timeout after which any data that NSQd has buffered will be flushed
     * to this client
     */
    private Integer outputBufferTimeoutInMillisecond = null;

    // 1 seconds
    public static final int _MIN_NEXT_CONSUMING_IN_SECOND = 0;
    // one hour is the limit
    public static final int _MAX_NEXT_CONSUMING_IN_SECOND = 3600;
    private int nextConsumingInSecond = 60;
    private long maxConnWait = 200L;
    private int minIdleConn = 2;
    private int pubRetry = 3;
    private static final int PUB_MAX_RETRY = 6;

    /*-
     *                             All of Timeout
     * =========================================================================
     */

    /*-
     * ==========================configs agent for lookup discovery=================
     */
    //default heartbeat value, also the max value NSQd allow
    private final Integer MAX_HEARTBEAT_INTERVAL_IN_MILLISEC = 60 * 1000;
    private final Integer DEFAULT_HEARTBEAT_INTERVAL_IN_MILLISEC = 30 * 1000;
    private Integer heartbeatIntervalInMillisecond = DEFAULT_HEARTBEAT_INTERVAL_IN_MILLISEC;

    private Integer outputBufferSize = null;

    private boolean tlsV1 = false;
    private Integer deflateLevel = null;
    private Integer sampleRate = null;
    private final String userAgent = "Java/com.youzan/" + Version.ARTIFACT_ID + "/" + Version.VERSION;
    private Compression compression = Compression.NO_COMPRESSION;
    // ...
    private SslContext sslContext = null;
    private int rdy = 3;

    private long producerConnectionEvictIntervalMillSec = 10 * 60 * 1000;

    static {
        PerfTune.getInstance();
    }

    /**
     * NSQConfig constructor for {@link com.youzan.nsq.client.Producer}.
     */
    public NSQConfig() {
        try {
            hostname = HostUtil.getLocalIP();
            // JDK8, string contact is OK.
            clientId =  ""+SystemUtil.getPID();
        } catch (Exception e) {
            throw new RuntimeException("System can't get the IPv4!", e);
        }
    }

    /**
     * NSQConfig constructor for {@link com.youzan.nsq.client.Consumer}.
     * @param consumerName  channel name for consumer.
     */
    public NSQConfig(final String consumerName) {
        this();
        //and channel name
        setConsumerName(consumerName);
    }

    /**
     * Turn on passin topic for Pub/Sub Trace. Local trace works when {@link com.youzan.nsq.client.configs.ConfigAccessAgent}
     * running in local mode.
     * @param topics topics whose trace to turn on.
     */
    public void turnOnLocalTrace(String... topics) {
        for(String aTopic:topics)
            this.localTraceMap.put(aTopic, "1");
    }


    /**
     * Remove pass in topics from local topic trace mapping. Local trace works when {@link com.youzan.nsq.client.configs.ConfigAccessAgent}
     * running in local mode.
     * @param topics topic to remove from local trace map
     */
    public void turnOffLocalTrace(String... topics) {
        for(String aTopic:topics)
            this.localTraceMap.remove(aTopic);
    }

    /**
     * get local trace mapping.
     * @return local trace mapping.
     */
    public Map<String, String> getLocalTraceMap() {
        return this.localTraceMap;
    }

    public boolean getUserSpecifiedLookupAddress() {
        return this.userSpecifiedLookupd;
    }

    /**
     * Note: Function is deprecated since 2.3.20170309-RELEASE, In succeed release, when NSQ seed lookupd address pass into
     * {@link NSQConfig#setLookupAddresses(String)}, that means user specified lookupd address is set {@link Boolean#TRUE}
     *
     * Switch on/off (default is OFF) config access agent to remote for NSQ Seed lookup discovery.
     * Once it is set to {@link Boolean#TRUE}, {@link NSQConfig#setLookupAddresses(String)} need to be invoked to pass
     * in seed lookup address.
     * @param userLookupd
     *      {@link Boolean#TRUE} to override with user specified seed lookup address, {@link Boolean#FALSE} to turn off
     *      user specified lookup address.
     * @return current NSQConfig
     */
    @Deprecated
    public NSQConfig setUserSpecifiedLookupAddress(boolean userLookupd) {
        this.userSpecifiedLookupd = userLookupd;
        return this;
    }

    /**
     * Note: This function is changed since 2.3.20170309-RELEASE.
     * Specify NSQ seed lookupd address for client which current NSQConfig applied to, and remote config access for global.
     * if passin parameter is NSQ seed lookupd address, this function acts as what it does in SDK 2.2.
     *
     * In succeed release, current function accepts remote config access URLs for global seed lookupd address config. DCC
     * remote config access URLs accepted in format as dcc://{host}:{port}?env={env}, example: dcc://127.0.0.1:8089?env=prod
     *
     * When function has DCC config access URLs passed in, it configures global config access. That means one can use any
     * NSQConfig object to configure global config access. If function has seed lookupd address passed in, that only applies
     * to client(producer&consumer) which has current NSQConfig applied. If one client has seed lookupd address URLs configured
     * global config access URLs will be override.
     *
     * @param lookupAddresses the lookupAddresses to set for producer&consumer, and remote config access URL(DCC) for global.
     * @throws IllegalArgumentException exception raised when lookup address and config access URLs are mixed.
     */
    public NSQConfig setLookupAddresses(final String lookupAddresses) {
        if(null == lookupAddresses || lookupAddresses.isEmpty()) {
            if(logger.isWarnEnabled())
                logger.warn("Empty lookup address is not accepted.");
            return this;
        }
        String[] newLookupAddresses = lookupAddresses.replaceAll(" ", "").split(",");
        Arrays.sort(newLookupAddresses);
        String conversalEnv = null;
        //validate that config access urls and lookup address are not mixed.
        boolean configAccess = false, lookupaddress = false;
        String[] newLookupAddressesParsed = new String[newLookupAddresses.length];
        for(int idx = 0; idx < newLookupAddresses.length; idx++){
            if(newLookupAddresses[idx].startsWith("dcc://")) {
                configAccess = true;
                try {
                    URI uri = new URI(newLookupAddresses[idx]);
                    String[] params = uri.getQuery().split("&");
                    for(String param:params){
                        if(param.startsWith("env=")) {
                            String env = param.split("=")[1];
                            if(null == conversalEnv && null != env) {
                                conversalEnv = env;
                                break;
                            }
                            else if(null != env && !conversalEnv.equals(env))
                                throw new IllegalArgumentException("pass in config access URLs must has same env environment value.");
                        }
                    }
                    newLookupAddressesParsed[idx] = newLookupAddresses[idx].split("\\?")[0].replace("dcc://", "http://");
                } catch (URISyntaxException e) {
                    throw new IllegalArgumentException("pass in config access URLs must confirm to format \"dcc://{host}:{port}?env={env}\".");
                }
            } else lookupaddress = true;
        }

        if(configAccess && lookupaddress){
            throw new IllegalArgumentException("Local lookup addresses & config access URLs could not mix.");
        }

        if(lookupaddress) {
            this.lookupAddresses = newLookupAddresses;
            this.userSpecifiedLookupd = true;
            logger.info("Setup seed lookupd address URLs with: {}", newLookupAddresses);
        } else {
            //replace static config access URLs with new one.
            configAccessURLs = newLookupAddressesParsed;
            configAccessEnv = conversalEnv;
            logger.info("Setup config access URLs with: {}, env: {}", newLookupAddresses, conversalEnv);
            if(this.userSpecifiedLookupd == true)
                logger.info("NSQ lookupd address discovery for Consumer&Producer has current NSQConfig applied is still running under local mode.");
        }
        return this;
    }

    /**
     * return user specified lookupaddresses
     * @return lookupd addresses specified via {@link NSQConfig#setLookupAddresses(String)}
     */
    public String[] getLookupAddresses() {
        return this.lookupAddresses;
    }

    /**
     * @return the TCP connection timeout
     */
    public int getConnectTimeoutInMillisecond() {
        return connectTimeoutInMillisecond;
    }

    /**
     * TCP connection timeout when invoke a new connection to nsqd.
     * @param connectTimeoutInMillisecond the connectTimeoutInMillisecond to set
     * @return {@link NSQConfig}
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


    private DesiredTag desiredTag;
    /**
     * set desired tag for consumer would like to subscribe, a valid tag filter is:
     * smaller than 100 bytes in length and it is th combination of alphabet[a-zA-Z] and number[0-9]
     * @param desiredTag tag filter string
     * @return {@link NSQConfig}
     */
    public NSQConfig setConsumerDesiredTag(final DesiredTag desiredTag) {
        this.desiredTag = desiredTag;
        return this;
    }

    public DesiredTag getConsumerDesiredTag() {
        return desiredTag;
    }
    /**
     * @param consumerName the consumerName to set
     * @return {@link NSQConfig}
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
     * @return {@link NSQConfig}
     */
    public NSQConfig setOrdered(boolean ordered) {
        this.ordered = ordered;
        return this;
    }

    /**
     * Specify connection pool size for producer, or default value(30) applies.
     * @param connectionPoolSize
     *      connection pool size for producer connection pool.
     * @return current NSQConfig
     */
    public NSQConfig setConnectionPoolSize(int connectionPoolSize) {
        if(connectionPoolSize < 1) {
            throw new IllegalArgumentException("SDK does not accept connection pool size which smaller than 1.");
        }
        this.connectionSize = connectionPoolSize;
        return this;
    }

    /**
     * @return connection
     *      Connection size value for producer connection pool size
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
     * @return {@link NSQConfig}
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
        return heartbeatIntervalInMillisecond;
    }

    /**
     * @param heartbeatIntervalInMillisecond the heartbeatIntervalInMillisecond to set,
     *                                       which could not exceed max value allowed in NSQd, which is 60000 milliSec.
     * @return {@link NSQConfig}
     */
    public NSQConfig setHeartbeatIntervalInMillisecond(Integer heartbeatIntervalInMillisecond) {
        if(heartbeatIntervalInMillisecond > MAX_HEARTBEAT_INTERVAL_IN_MILLISEC)
            throw new IllegalArgumentException("heartbeat could not exceed max value " + MAX_HEARTBEAT_INTERVAL_IN_MILLISEC);
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
     * @return {@link NSQConfig}
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
     * Specify output_buffer_size (nsqd v0.2.21+) the size in bytes of the buffer nsqd will use when writing to this
     * client.
     *
     * @param outputBufferSize the outputBufferSize to set
     * @return {@link NSQConfig}
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
     * Specify output_buffer_timeout (nsqd v0.2.21+) the timeout after which any data that nsqd has buffered will be
     * flushed to this client.
     *
     * @param outputBufferTimeoutInMillisecond the outputBufferTimeoutInMillisecond to set
     * @return {@link NSQConfig}
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
     * @return {@link NSQConfig}
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
     * @return {@link NSQConfig}
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
     * @param compression the compression to set, reference to {@link Compression} for options, default value is
     *                    NO_COMPRESSION
     * @return {@link NSQConfig}
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
     * @param deflateLevel the expect deflateLevel to set, SDK will finally apply deflate level answer from nsqd
     * @return {@link NSQConfig}
     */
    public NSQConfig setDeflateLevel(Integer deflateLevel) {
        this.deflateLevel = deflateLevel;
        return this;
    }

    /**
     * @return the sample rate
     */
    public Integer getSampleRate() {
        return sampleRate;
    }

    /**
     * sample_rate (nsqd v0.2.25+) deliver a percentage of all messages received to this connection.
     * Valid range: 0 <= sample_rate <= 99 (0 disables sampling)
     * Defaults to 0
     * @param sampleRate the sample rate to set
     * @return {@link NSQConfig}
     */
    public NSQConfig setSampleRate(Integer sampleRate) {
        if(sampleRate < 0 || sampleRate > 99) {
            throw new IllegalArgumentException("Invalid sample rate, specify a value 0 <= sample_rate <= 99 (0 disables sampling)");
        }
        this.sampleRate = sampleRate;
        return this;
    }

    /**
     * @return the expected rdy count
     */
    public int getRdy() {
        return rdy;
    }

    /**
     * @param rdy the rdy to set , it is ready to receive the pushing message
     *            count
     * @return {@link NSQConfig}
     */
    public NSQConfig setRdy(int rdy) {
        if (rdy <= 0) {
            this.rdy = 1;
            throw new IllegalArgumentException("expect rdy smaller than 1 not allowed.");
        }
        this.rdy = rdy;
        return this;
    }

    public static int getListLookupIntervalInSecond() {
        return listLookupIntervalInSecond;
    }

    public static void setListLookupIntervalInSecond(int newListLookupIntervalInSecond) {
        if(newListLookupIntervalInSecond < 60) {
            logger.warn("ListLookupInterval accepts value which is not smaller than 60Sec.");
        }
        listLookupIntervalInSecond = newListLookupIntervalInSecond;
    }

    public static int getQueryTimeout4TopicSeedInMillisecond() {
        return queryTimeout4TopicSeedInMillisecond;
    }

    /**
     *timeout for seed lookup address subscribe.
     * @param queryTimeout4TopicSeedInMillisecond
     */
    public static void setQueryTimeout4TopicSeedInMillisecond(int queryTimeout4TopicSeedInMillisecond) {
        NSQConfig.queryTimeout4TopicSeedInMillisecond = queryTimeout4TopicSeedInMillisecond;
    }

    /**
     * @return the queryTimeoutInMillisecond
     */
    public int getQueryTimeoutInMillisecond() {
        return queryTimeoutInMillisecond;
    }

    /**
     * @param queryTimeoutInMillisecond the queryTimeoutInMillisecond to set
     * @return {@link NSQConfig}
     */
    public NSQConfig setQueryTimeoutInMillisecond(int queryTimeoutInMillisecond) {
        this.queryTimeoutInMillisecond = queryTimeoutInMillisecond;
        return this;
    }

    private static Logger getLogger() {
        return logger;
    }

    public static String[] getConfigAccessURLs() {
        return configAccessURLs;
    }

    public static String getConfigAccessEnv() {
        return configAccessEnv;
    }

    /**
     * return IDENTIFY json in String for current NSQConfig
     * @return identify json string
     */
    public String identify(boolean topicExt) {
        final StringBuffer buffer = new StringBuffer(300);
        buffer.append("{\"client_id\":\"" + clientId + "\", ");
        buffer.append("\"hostname\":\"" + hostname + "\", ");
        buffer.append("\"feature_negotiation\": true, ");
        if (outputBufferSize != null) {
            buffer.append("\"output_buffer_size\":" + outputBufferSize + ", ");
        }
        if (outputBufferTimeoutInMillisecond != null) {
            buffer.append("\"output_buffer_timeout\":" + outputBufferTimeoutInMillisecond + ", ");
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
                buffer.append("\"deflate_level\":" + deflateLevel + ", ");
            }
        }
        if (sampleRate != null) {
            buffer.append("\"sample_rate\":" + sampleRate + ",");
        }
        if (getHeartbeatIntervalInMillisecond() != null) {
            buffer.append("\"heartbeat_interval\":" + String.valueOf(getHeartbeatIntervalInMillisecond()) + ", ");
        }
        buffer.append("\"msg_timeout\":" + String.valueOf(msgTimeoutInMillisecond) + ",");
        DesiredTag tag = getConsumerDesiredTag();
        if (null != tag) {
            buffer.append("\"desired_tag\":\"" + tag.getTagName() + "\",");
        }
        buffer.append("\"extend_support\":" + topicExt + ",");
        buffer.append("\"user_agent\": \"" + userAgent + "\"}");
        return buffer.toString();
    }

    /**
     * reset global config access variables
     */
    public static void resetConfigAccessConfigs() {
        NSQConfig.configAccessEnv = null;
        NSQConfig.configAccessURLs = null;
        logger.info("Global config access configs cleared.");
    }

    /**
     * specify time elapse before a requeued message sent from NSQd. Next consuming timeout in current NSQConfig could
     * be override by {@link NSQMessage#setNextConsumingInSecond(Integer)}
     * @param timeout timeout in seconds
     * @return {@link NSQConfig} this NSQConfig
     */
    public NSQConfig setNextConsumingInSeconds(int timeout) {
        if (timeout < NSQConfig._MIN_NEXT_CONSUMING_IN_SECOND) {
            throw new IllegalArgumentException(
                    "Next consuming in second is illegal. It is too small. " + NSQConfig._MIN_NEXT_CONSUMING_IN_SECOND);
        }
        if (timeout > NSQConfig._MAX_NEXT_CONSUMING_IN_SECOND) {
            throw new IllegalArgumentException(
                    "Next consuming in second is illegal. It is too big. " + NSQConfig._MAX_NEXT_CONSUMING_IN_SECOND);
        }
        this.nextConsumingInSecond = timeout;
        return this;
    }

    /**
     * get time elapse for a requeued message in current NSQConfig.
     * @return expected interval in seconds before a requeue message pushed to client again. As
     */
    public int getNextConsumingInSecond() {
        return this.nextConsumingInSecond;
    }

    public void setConnWaitTimeoutForProducerInMilliSec(long timeout) {
        this.maxConnWait = timeout;
    }

    public long getConnWaitTimeoutForProducerInMilliSec() {
        return this.maxConnWait;
    }

    /**
     * Specify min number of idle connection number per topic in producer's connection pool
     * @param minIdleNum min idle connection number per topic for message producer
     */
    public void setMinIdleConnectionForProducer(int minIdleNum) {
        if(minIdleNum > 0)
            this.minIdleConn = minIdleNum;
    }

    public int getMinIdleConnectionForProducer() {
        return this.minIdleConn;
    }

    public long getProducerConnectionEvictIntervalInMillSec() {
        return this.producerConnectionEvictIntervalMillSec;
    }

    public NSQConfig setProducerConnectionEvictIntervalInMillSec(long producerConnectionEvictIntervalMillSec) {
        this.producerConnectionEvictIntervalMillSec = producerConnectionEvictIntervalMillSec;
        return this;
    }

    public NSQConfig setPublishRetry(int retry) {
        if (retry > PUB_MAX_RETRY) {
            throw new IllegalArgumentException("Max publish retry " + PUB_MAX_RETRY + " allowed");
        }
        this.pubRetry = retry;
        return this;
    }

    public int getPublishRetry() {
        return this.pubRetry;
    }

    /**
     * @return netty nio group pool size in current NSQConfig
     */
    public int getNettyPoolSize() {
        return nettyPoolSize;
    }

    /**
     * Specify netty work group thread pool size for nsqd frames receive & delivery process.
     * @param newPoolSize new pool size for netty nio group to set
     * @return {@link NSQConfig}
     */
    public NSQConfig setNettyPoolSize(int newPoolSize) {
        if (newPoolSize < 1) {
            throw new IllegalArgumentException("Thread pool size smaller than 1 is not accepted.");
        }
        this.nettyPoolSize = newPoolSize;
        return this;
    }

    public NSQConfig setConsumerWorkerPoolSize(int newPoolSize) {
        if (newPoolSize < 1) {
            throw new IllegalArgumentException("Consumer worker pool size smaller than 1 is not accepted.");
        }
        this.consumerWorkerPoolSize = newPoolSize;
        return this;
    }

    public int getConsumerWorkerPoolSize() {
        return this.consumerWorkerPoolSize;
    }

    private enum ConsumePolicy {
        SKIP
    }

    private Map<ConsumePolicy, Map<String, Object>> consumePolcyMap = new ConcurrentHashMap<>();

    /**
     * Set extension key/value map for consumer to skip, when json extension header in one message
     * contains Any subset of extensionKV, consumer will skip(ACK directly without passing to message handler)
     * this message.
     * extension
     * @param extensionKV
     * @return {@link NSQConfig} current NSQConfig
     */
    NSQConfig setMessageSkipExtensionKVMap(final Map<String, Object> extensionKV) {
        if(null == extensionKV || extensionKV.size() == 0)
            return this;
        this.consumePolcyMap.put(ConsumePolicy.SKIP, Collections.unmodifiableMap(extensionKV));
        return this;
    }

    /**
     * Set extension key for consumer to skip, when json extension header in one message
     * contains specified key, consumer will skip(ACK directly without passing to message handler)
     * this message.
     * extension
     * @param key
     * @return {@link NSQConfig} current NSQConfig
     */
    public NSQConfig setMessageSkipExtensionKey(final String key) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, "*");
        return this.setMessageSkipExtensionKVMap(map);
    }

    /**
     * Get extension key/value map for consumer to skip.
     * @return extension key/value map for consumer to skip, which is {@link UnmodifiableMap}.
     */
    public Map<String, Object> getMessageSkipExtensionKVMap() {
        return this.consumePolcyMap.get(ConsumePolicy.SKIP);
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

    /*
     *deprecated functions which are not used in 2.4
     */
    /**
     * Deprecated in SDK 2.4. this function makes no effect
     * set max requeue times threshold for one message.
     * @param times
     * @return {@link NSQConfig}
     */
    @Deprecated
    public NSQConfig setMaxRequeueTimes(int times) {
        logger.info("[NSQConfig.setMaxRequeueTimes] makes no effect");
        return this;
    }

    /**
     * Deprecated in SDK 2.4. this function ALWAYS answers -1
     * @return threshold of requeue time for one message.
     */
    @Deprecated
    @JsonIgnore
    public int getMaxRequeueTimes() {
        return -1;
    }

    /**
     * switch to enable/disable send a message which reaches max requeue times to the tail of message queue and ACK
     * it after success, default flag value is {@link Boolean#FALSE}, prior to current release is always {@link Boolean#TRUE}.
     * @param flag {@link Boolean#FALSE} to disable, otherwise enable
     * @return {@link NSQConfig}
     *
     * This is function is deprecated, invoke of this function makes no effect
     */
    @Deprecated
    public NSQConfig setSendAndACKAfterMaxRequeue(boolean flag) {
        logger.info("[NSQConfig.setSendAnsACKAfterMaxRequeue] makes no effect");
        return this;
    }

    /**
     * This function is deprecated, invoke of this function makes no effect
     * @return false
     */
    @Deprecated
    @JsonIgnore
    public boolean getSendAndACKAfterMaxRequeue() {
        return false;
    }

    /**
     * Set producer retry interval base when exception raised in publish.
     * producer retry 6 times when exception which SDK does not know happen.
     * producer sleeps  1 << (currentRetry - 1) * retryIntervalBase milliseconds in each interval.
     *
     * @param retryIntervalBase retry interval base in milliseconds, needs to be larger than 0. If pass in value is 0,
     *                          it means producer does not sleep when failure happens.
     * @return {@link NSQConfig} this NSQConfig
     *
     * Function is deprecated as there is no wait interval between two retries.
     */
    @Deprecated
    public NSQConfig setProducerRetryIntervalBaseInMilliSeconds(int retryIntervalBase) {
        if(retryIntervalBase >=0 ) {
            this.producerRetryIntervalBase = retryIntervalBase;
            logger.info("producer retry interval base set to {}.", retryIntervalBase);
        }
        return this;
    }

    /**
     * return producer retry interval base in milliseconds of current NSQConfig.
     * @return producerRetryIntervalBase
     *
     */
    @Deprecated
    @JsonIgnore
    public int getProducerRetryIntervalBaseInMilliSeconds() {
        return this.producerRetryIntervalBase;
    }


    /**
     * @return threadPoolSize4IO in current NSQConfig
     */
    @Deprecated
    public int getThreadPoolSize4IO() {
        return threadPoolSize4IO;
    }

    /**
     * Specify max thread size for consumer client business process.
     * @param threadPoolSize4IO the threadPoolSize4IO to set
     * @return {@link NSQConfig}
     */
    @Deprecated
    public NSQConfig setThreadPoolSize4IO(int threadPoolSize4IO) {
        if (threadPoolSize4IO < 1) {
            throw new IllegalArgumentException("Thread pool size smaller than 1 is not accepted.");
        }
        this.threadPoolSize4IO = threadPoolSize4IO;
        return this;
    }
}
