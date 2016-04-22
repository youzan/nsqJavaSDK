package com.youzan.nsq.client.enums;

public enum ProtocolVersion {
    V2("V2");
    private String version;

    private ProtocolVersion(String version) {
        this.version = version;
    }

    public String getCode() {
        return this.version;
    }
}
