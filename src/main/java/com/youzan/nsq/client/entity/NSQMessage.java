package com.youzan.nsq.client.entity;

public class NSQMessage {
    // *************************************************************************
    // NSQ the message format
    // *************************************************************************
    private long timestamp; // 8 bytes : nanosecond timestamp
    private int attempts; // 2 bytes
    private byte[] id; // 16 bytes : (hex string encoded in ASCII)
    private byte[] message; // n bytes

    // *************************************************************************
    // For client , human message
    // *************************************************************************
    private String datetime; // yyyy-MMdd hh:mm:ss

    /**
     * 8 bytes : nanosecond timestamp
     * 
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * @param timestamp
     *            the timestamp to set
     */
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * @return the id
     */
    public byte[] getId() {
        return id;
    }

    /**
     * @param id
     *            the id to set
     */
    public void setId(byte[] id) {
        this.id = id;
    }

    /**
     * @return the attempts
     */
    public int getAttempts() {
        return attempts;
    }

    /**
     * @param attempts
     *            the attempts to set
     */
    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    /**
     * @return the message
     */
    public byte[] getMessage() {
        return message;
    }

    /**
     * @param message
     *            the message to set
     */
    public void setMessage(byte[] message) {
        this.message = message;
    }

}
