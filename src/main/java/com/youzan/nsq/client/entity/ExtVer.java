package com.youzan.nsq.client.entity;

import java.nio.ByteBuffer;

/**
 * Created by lin on 17/5/25.
 */
public enum ExtVer {
    Ver0x0(0),
    //tag ext
    Ver0x2(2),
    //json header ext
    Ver0x4(4);

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
            case    4:
                return Ver0x4;
            case    2:
                return Ver0x2;
            default:
                return Ver0x0;
        }
    }
}
