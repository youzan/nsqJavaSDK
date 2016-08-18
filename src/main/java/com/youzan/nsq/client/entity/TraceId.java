package com.youzan.nsq.client.entity;

import com.youzan.nsq.client.core.command.NSQCommand;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by lin on 16/8/31.
 */
public class TraceId {
    private static final int TRACE_ID_LMT = 8;

    private final byte[] id;

    public TraceId(String id){
        this.id = String2Bytes(id);
    }

    public TraceId(byte[] id){
        this.id = id;
    }

    static byte[] String2Bytes(String val){
        byte[] valByte = val.getBytes(NSQCommand.DEFAULT_CHARSET);
        return Arrays.copyOfRange(valByte, 0, 8);
    }

    public byte[] getIdValue(){
        return this.id;
    }
}
