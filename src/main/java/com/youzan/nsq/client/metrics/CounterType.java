package com.youzan.nsq.client.metrics;

/**
 * Created by lin on 18/8/14.
 */
public enum CounterType {
    PUB("pub"),
    RECV("recv"),
    SUCCESS("success"),
    SKIP("skip"),
    FIN("fin"),
    REQ("req"),
    TOUCH("touch"),
    TIMEOUT("timeout"),
    EXCEPTION("exception");

    private String typeName;

    CounterType(String typeName) {
        this.typeName = typeName;
    }

    public String getTypeName() {
        return this.typeName;
    }
}