package com.youzan.nsq.client.entity;

import java.util.List;

public class NSQConfig {

    private List<String> addresses;
    private String topic;
    private String consumerName;
    private int workersSize;
    private int clientId;
    private String host;
    private boolean featureNegotiation;
    private Integer heartbeatInterval;
    private String userAgent;

}
