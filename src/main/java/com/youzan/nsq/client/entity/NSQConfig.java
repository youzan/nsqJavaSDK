package com.youzan.nsq.client.entity;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.Version;
import com.youzan.util.HostUtil;
import com.youzan.util.IPUtil;
import com.youzan.util.NotThreadSafe;
import com.youzan.util.SystemUtil;

import io.netty.handler.ssl.SslContext;

/**
 * One config to One cluster with specific topic. <br>
 * It is used for Producer or Consumer, and not both two.
 * 
 * 
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 *
 * 
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
    private String lookupAddresses;

    @Deprecated
    private String topic;
    /**
     * In NSQ, it is a channel.
     */
    private String consumerName;
    /**
     * The set of messages is ordered in one specified partition
     */
    private boolean ordered = true;
    /**
     * <pre>
     * Set the thread_pool_size for IO running.
     * It is also used for Netty.
     * In recommended, the size is (CPUs - 1) and bind CPU affinity.
     * </pre>
     */
    private int threadPoolSize4IO = Runtime.getRuntime().availableProcessors() - 1;
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
    private int connectTimeoutInMillisecond = 50;
    /**
     * Perform one interactive action between request and response underlying
     * Netty handling TCP
     */
    private int queryTimeoutInMillisecond = 2000;
    /**
     * Perform one action during specified timeout
     */
    @Deprecated
    private int timeoutInSecond = 1;
    /**
     * the timeout after which any data that nsqd has buffered will be flushed
     * to this client
     */
    private Integer outputBufferTimeoutInMillisecond = null;
    /*-
     *                             All of Timeout
     * =========================================================================
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

    public NSQConfig() {
        try {
            hostname = HostUtil.getLocalIP();
            // JDK8, string contact is OK.
            clientId = "IP:" + IPUtil.ipv4(hostname) + ", PID:" + SystemUtil.getPID() + ", ID:"
                    + (id.getAndIncrement());
        } catch (Exception e) {
            throw new RuntimeException("System cann't get the IPv4!", e);
        }
    }

    /**
     * One lookup cluster
     * 
     * @return the lookupAddresses
     */
    public String getLookupAddresses() {
        return lookupAddresses;
    }

    /**
     * @param lookupAddresses
     *            the lookupAddresses to set
     */
    public void setLookupAddresses(String lookupAddresses) {
        this.lookupAddresses = lookupAddresses;
    }

    /**
     * @return the connectTimeoutInMillisecond
     */
    public int getConnectTimeoutInMillisecond() {
        return connectTimeoutInMillisecond;
    }

    /**
     * @param connectTimeoutInMillisecond
     *            the connectTimeoutInMillisecond to set
     */
    public void setConnectTimeoutInMillisecond(int connectTimeoutInMillisecond) {
        this.connectTimeoutInMillisecond = connectTimeoutInMillisecond;
    }

    /**
     * @return the timeoutInSecond
     */
    @Deprecated
    public int getTimeoutInSecond() {
        return timeoutInSecond;
    }

    /**
     * @param timeoutInSecond
     *            the timeoutInSecond to set
     */
    @Deprecated
    public void setTimeoutInSecond(int timeoutInSecond) {
        this.timeoutInSecond = timeoutInSecond;
    }

    /**
     * @return the serialversionuid
     */
    public static long getSerialversionuid() {
        return serialVersionUID;
    }

    /**
     * @return the topic
     */
    @Deprecated
    public String getTopic() {
        return topic;
    }

    /**
     * @param topic
     *            the topic to set
     */
    @Deprecated
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * @return the consumerName
     */
    public String getConsumerName() {
        return consumerName;
    }

    /**
     * @param consumerName
     *            the consumerName to set
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
     * @param ordered
     *            the ordered to set
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
     * @param threadPoolSize4IO
     *            the threadPoolSize4IO to set
     */
    public void setThreadPoolSize4IO(int threadPoolSize4IO) {
        this.threadPoolSize4IO = threadPoolSize4IO;
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
     * @param msgTimeoutInMillisecond
     *            the msgTimeoutInMillisecond to set
     */
    public void setMsgTimeoutInMillisecond(int msgTimeoutInMillisecond) {
        this.msgTimeoutInMillisecond = msgTimeoutInMillisecond;
    }

    /**
     * @return the heartbeatIntervalInMillisecond
     */
    public Integer getHeartbeatIntervalInMillisecond() {
        if (heartbeatIntervalInMillisecond == null) {
            return Integer.valueOf(getMsgTimeoutInMillisecond() / 3);
        }
        return heartbeatIntervalInMillisecond;
    }

    /**
     * @param heartbeatIntervalInMillisecond
     *            the heartbeatIntervalInMillisecond to set
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
     * @param featureNegotiation
     *            the featureNegotiation to set
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
     * @param outputBufferSize
     *            the outputBufferSize to set
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
     * @param outputBufferTimeoutInMillisecond
     *            the outputBufferTimeoutInMillisecond to set
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
     * @param havingMonitoring
     *            the havingMonitoring to set
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
     * @param tlsV1
     *            the tlsV1 to set
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
     * @param compression
     *            the compression to set
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
     * @param deflateLevel
     *            the deflateLevel to set
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
     * @param sampleRate
     *            the sampleRate to set
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
     * @param rdy
     *            the rdy to set , it is ready to receive the pushing message
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
     * @param queryTimeoutInMillisecond
     *            the queryTimeoutInMillisecond to set
     */
    public void setQueryTimeoutInMillisecond(int queryTimeoutInMillisecond) {
        this.queryTimeoutInMillisecond = queryTimeoutInMillisecond;
    }

    private static Logger getLogger() {
        return logger;
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
