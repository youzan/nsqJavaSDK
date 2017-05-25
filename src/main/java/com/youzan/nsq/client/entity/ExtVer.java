package com.youzan.nsq.client.entity;

import java.nio.ByteBuffer;

/**
 * Created by lin on 17/5/25.
 */
public enum ExtVer {
    Ver0x1(1),
    Ver0x2(2);

    private final int ver;

    ExtVer(int ver) {
        this.ver = ver;
    }

    public int getVersion() {
        return this.ver;
    }

    public static ExtVer getExtVersion(byte[] extVerByte) {
        int ver = ByteBuffer.wrap(extVerByte).get() & 0xFF;
        switch (ver) {
            case    2:
                return Ver0x2;
            default:
                return Ver0x1;
        }
    }
}
