package com.youzan.nsq.client.entity;

public class NSQMessage {

    /**
     * all the parameters is the NSQ message format!
     * 
     * @param timestamp
     * @param attempts
     * @param messageID
     * @param messageBody
     */
    public NSQMessage(byte[] timestamp, byte[] attempts, byte[] messageID, byte[] messageBody) {
    }

    // *************************************************************************
    // For client , human message
    // *************************************************************************
    private String datetime; // yyyy-MMdd hh:mm:ss

}
