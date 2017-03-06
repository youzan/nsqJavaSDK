package com.youzan.nsq.client.entity;

import com.youzan.nsq.client.Version;
import com.youzan.util.HostUtil;
import com.youzan.util.NotThreadSafe;
import com.youzan.util.SystemUtil;
import io.netty.handler.ssl.SslContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * It is used for Producer or Consumer, and not both two.
 *
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
@NotThreadSafe
public class NSQConfig implements java.io.Serializable, Cloneable {

    private static final long serialVersionUID = 6624842850216901700L;
    private static final Logger logger = LoggerFactory.getLogger(NSQConfig.class);

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
    private int connectionSize = 30;
    private final String clientId;
    private final String hostname;
    private boolean featureNegotiation;
    private boolean slowStart = true;
    private boolean userSpecifiedLookupd = false;
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
     * query timeout for topic based seed lookup address check
     */
    private static int queryTimeout4TopicSeedInMillisecond = 500;

    /**
     * interval between two list lookup operation for all seed lookups
     */
    private static int listLookupIntervalInSecond = 120;

    /**
     * interval base for producer retry interval
     */
    private int producerRetryIntervalBase = 100;

    /**
     * the timeout after which any data that NSQd has buffered will be flushed
     * to this client
     */
    private Integer outputBufferTimeoutInMillisecond = null;

    /**
     * max requeue times setting for consumer, if message read attempts exceeds that value, consumer will:
     * 1. log that message;
     * 2. publish that message back to NSQ;
     * 3. ACK origin message.
     */
    private int maxRequeueTimes = 30;

    /*-
     *                             All of Timeout
     * =========================================================================
     */

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

    public boolean getUserSpecifiedLookupAddress() {
        return this.userSpecifiedLookupd;
    }

    /**
     * Switch on/off (default is OFF) config access agent to remote for NSQ Seed lookup discovery.
     * Once it is set to {@link Boolean#TRUE}, {@link NSQConfig#setLookupAddresses(String)} need to be invoked to pass
     * in seed lookup address.
     * @param userLookupd
     *      {@link Boolean#TRUE} to override with user specified seed lookup address, {@link Boolean#FALSE} to turn off
     *      user specified lookup address.
     * @return current NSQConfig
     */
    public NSQConfig setUserSpecifiedLookupAddress(boolean userLookupd) {
        this.userSpecifiedLookupd = userLookupd;
        return this;
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
     * @return lookupd addresses specified via {@link NSQConfig#setLookupAddresses(String)}
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
     * @return threadPoolSize4IO in current NSQConfig
     */
    public int getThreadPoolSize4IO() {
        return threadPoolSize4IO;
    }

    /**
     * Specify max thread size for consumer client business process.
     * @param threadPoolSize4IO the threadPoolSize4IO to set
     * @return {@link NSQConfig}
     */
    public NSQConfig setThreadPoolSize4IO(int threadPoolSize4IO) {
        if (threadPoolSize4IO < 1) {
            logger.warn("Thread pool size smaller than 1 is not accepted.");
        }
        this.threadPoolSize4IO = threadPoolSize4IO;
        return this;
    }

    /**
     * Specify connection pool size for producer, or default value(10) applies.
     * @param connectionPoolSize
     *      connection pool size for producer connection pool.
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
        final int max = 50 * 1000;
        if (heartbeatIntervalInMillisecond == null) {
            return Math.min(Integer.valueOf(getMsgTimeoutInMillisecond() / 3), max);
        }
        return Math.min(heartbeatIntervalInMillisecond, max);
    }

    /**
     * @param heartbeatIntervalInMillisecond the heartbeatIntervalInMillisecond to set
     * @return {@link NSQConfig}
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
     * Specify output_buffer_size (nsqd v0.2.21+) the size in bytes of the buffer nsqd will use when writing to this
     * client.
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
     * @param compression the compression to set
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
     * @param deflateLevel the deflateLevel to set
     * @return {@link NSQConfig}
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
     * @return {@link NSQConfig}
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
     * @return {@link NSQConfig}
     */
    public NSQConfig setRdy(int rdy) {
        if (rdy <= 0) {
            this.rdy = 1;
            throw new IllegalArgumentException("Are you kidding me? The rdy should be positive.");
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

    public boolean isConsumerSlowStart() {
        return this.slowStart;
    }

    /**
     * Turn on/off consumer slow start with {@link Boolean#TRUE} or {@link Boolean#FALSE}
     * @param allowSlowStart switch to turn on/off consumer slow start
     * @return {@link NSQConfig} this NSQConfig
     */
    public NSQConfig setConsumerSlowStart(boolean allowSlowStart) {
        this.slowStart = allowSlowStart;
        return this;
    }

    /**
     * Set producer retry interval base when exception raised in publish.
     * producer retry 6 times when exception which SDK does not know happen.
     * producer sleeps  1 << (currentRetry - 1) * retryIntervalBase milliseconds in each interval.
     *
     * @param retryIntervalBase retry interval base in milliseconds, needs to be larger than 0. If pass in value is 0,
     *                          it means producer does not sleep when failure happens.
     * @return {@link NSQConfig} this NSQConfig
     */
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
     */
    public int getProducerRetryIntervalBaseInMilliSeconds() {
        return this.producerRetryIntervalBase;
    }

    public NSQConfig setMaxRequeueTimes(int times) {
       this.maxRequeueTimes = times;
       return this;
    }

    public int getMaxRequeueTimes() {
        return this.maxRequeueTimes;
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
