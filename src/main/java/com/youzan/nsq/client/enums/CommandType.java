package com.youzan.nsq.client.enums;

public enum CommandType {
    SUBSCRIBE("SUB"), //
    PUBLISH("PUB"), //
    READY("RDY"), //
    FINISH("FIN"), //
    REQUEUE("REQ"), //
    IDENTIFY("IDENTIFY"), //
    CLOSE("CLS"), //
    NOP("NOP");

    private final String code;

    private CommandType(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
