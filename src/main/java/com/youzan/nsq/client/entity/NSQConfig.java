package com.youzan.nsq.client.entity;

import java.util.List;

public class NSQConfig implements java.io.Serializable {

    private static final long serialVersionUID = 6624842850216901700L;

    public static final byte[] MAGIC_PROTOCOL_VERSION = "  V2".getBytes();
    /**
     * the sorted Lookupd addresses
     */
    private List<String> lookupAddresses;
    private String topic;
    /**
     * In NSQ, it is a channel.
     */
    private String consumerName;
    /**
     * The set of messages is ordered in one specific partition
     */
    private boolean ordered = true;
    private int connectionPoolSize;
    private int clientId;
    private String host;
    private boolean featureNegotiation;
    private Integer heartbeatInterval;
    private String userAgent;

    /**
     * @return the lookupAddresses
     */
    public List<String> getLookupAddresses() {
        return lookupAddresses;
    }

    /**
     * @param lookupAddresses
     *            the lookupAddresses to set
     */
    public void setLookupAddresses(List<String> lookupAddresses) {
        this.lookupAddresses = lookupAddresses;
    }

    /**
     * @return the serialversionuid
     */
    public static long getSerialversionuid() {
        return serialVersionUID;
    }

    /**
     * @return the magicProtocolVersion
     */
    public static byte[] getMagicProtocolVersion() {
        return MAGIC_PROTOCOL_VERSION;
    }

    /**
     * @return the topic
     */
    public String getTopic() {
        return topic;
    }

    /**
     * @param topic
     *            the topic to set
     */
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
     * @return the connectionPoolSize
     */
    public int getConnectionPoolSize() {
        return connectionPoolSize;
    }

    /**
     * @param connectionPoolSize
     *            the connectionPoolSize to set
     */
    public void setConnectionPoolSize(int connectionPoolSize) {
        this.connectionPoolSize = connectionPoolSize;
    }

    /**
     * @return the clientId
     */
    public int getClientId() {
        return clientId;
    }

    /**
     * @param clientId
     *            the clientId to set
     */
    public void setClientId(int clientId) {
        this.clientId = clientId;
    }

    /**
     * @return the host
     */
    public String getHost() {
        return host;
    }

    /**
     * @param host
     *            the host to set
     */
    public void setHost(String host) {
        this.host = host;
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
     * @return the heartbeatInterval
     */
    public Integer getHeartbeatInterval() {
        return heartbeatInterval;
    }

    /**
     * @param heartbeatInterval
     *            the heartbeatInterval to set
     */
    public void setHeartbeatInterval(Integer heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    /**
     * @return the userAgent
     */
    public String getUserAgent() {
        return userAgent;
    }

    /**
     * @param userAgent
     *            the userAgent to set
     */
    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

}
