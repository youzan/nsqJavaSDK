package com.youzan.nsq.client.remoting.connector;

import java.net.SocketException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.remoting.NetworkUtils;
import com.youzan.nsq.client.remoting.NetworkUtils.StackType;

import io.netty.handler.ssl.SslContext;

/**
 * @author caohaihong since 2015年10月28日 下午7:22:39
 */
public class NSQConfig {
    private static final Logger log = LoggerFactory.getLogger(NSQConfig.class);
    public static enum Compression {
        NO_COMPRESSION, DEFLATE, SNAPPY
    }

    private String clientId;
    private String hostname;
    private boolean featureNegotiation = true;
    private Integer heartbeatInterval = null;
    private Integer outputBufferSize = null;
    private Integer outputBufferTimeout = null;
    private boolean tlsV1 = false;
    private Compression compression = Compression.NO_COMPRESSION;
    private Integer deflateLevel = null;
    private Integer sampleRate = null;
    private String userAgent = null;
    private Integer msgTimeout = null;
    private SslContext sslContext = null;

    public NSQConfig() {
        try {
            userAgent = "JavaClient_1.3.3";
            clientId = NetworkUtils.getFirstNonLoopbackAddress(StackType.IPv4).getHostAddress();
            hostname = clientId;
        } catch (SocketException e) {
            log.warn("NSQCnfog: get local ip goes wrong at:{}", e);
        }
    }
    
    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }
    
    public String getHostname() {
        return hostname;
    }
    
    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public boolean isFeatureNegotiation() {
        return featureNegotiation;
    }

    public void setFeatureNegotiation(final boolean featureNegotiation) {
        this.featureNegotiation = featureNegotiation;
    }

    public Integer getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(final Integer heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public Integer getOutputBufferSize() {
        return outputBufferSize;
    }

    public void setOutputBufferSize(final Integer outputBufferSize) {
        this.outputBufferSize = outputBufferSize;
    }

    public Integer getOutputBufferTimeout() {
        return outputBufferTimeout;
    }

    public void setOutputBufferTimeout(final Integer outputBufferTimeout) {
        this.outputBufferTimeout = outputBufferTimeout;
    }

    public boolean isTlsV1() {
        return tlsV1;
    }

    public Compression getCompression() {
        return compression;
    }

    public void setCompression(final Compression compression) {
        this.compression = compression;
    }

    public Integer getDeflateLevel() {
        return deflateLevel;
    }

    public void setDeflateLevel(final Integer deflateLevel) {
        this.deflateLevel = deflateLevel;
    }

    public Integer getSampleRate() {
        return sampleRate;
    }

    public void setSampleRate(final Integer sampleRate) {
        this.sampleRate = sampleRate;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(final String userAgent) {
        this.userAgent = userAgent;
    }

    public Integer getMsgTimeout() {
        return msgTimeout;
    }

    public void setMsgTimeout(final Integer msgTimeout) {
        this.msgTimeout = msgTimeout;
    }

    public SslContext getSslContext() {
        return sslContext;
    }

    public void setSslContext(SslContext sslContext) {
        tlsV1 = true;
        this.sslContext = sslContext;
    }

    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("{\"feature_negotiation\": true, ");
        if (getClientId() != null) {
            buffer.append("\"client_id\":\"" + clientId + "\", ");
        }
        if (getHostname() != null) {
            buffer.append("\"hostname\":\"" + hostname + "\", ");
        } 
        if (getHeartbeatInterval() != null) {
            buffer.append("\"heartbeat_interval\":" + getHeartbeatInterval().toString() + ", ");
        }
        if (getOutputBufferSize() != null) {
            buffer.append("\"output_buffer_size\":" + getOutputBufferSize().toString() + ", ");
        }
        if (getOutputBufferTimeout() != null) {
            buffer.append("\"output_buffer_timeout\":" + getOutputBufferTimeout().toString() + ", ");
        }
        if (isTlsV1()) {
            buffer.append("\"tls_v1\":" + isTlsV1() + ", ");
        }
        if (getCompression() == Compression.SNAPPY) {
            buffer.append("\"snappy\": true, ");
        }
        if (getCompression() == Compression.DEFLATE) {
            buffer.append("\"deflate\": true, ");
        }
        if (getDeflateLevel() != null) {
            buffer.append("\"deflate_level\":" + getDeflateLevel().toString() + ", ");
        }
        if (getSampleRate() != null) {
            buffer.append("\"sample_rate\":" + getSampleRate().toString() + ", ");
        }
        if (getMsgTimeout() != null) {
            buffer.append("\"msg_timeout\":" + getMsgTimeout().toString() + ", ");
        }
        buffer.append("\"user_agent\": \"" + userAgent + "\"}");

        return buffer.toString();
    }
}
