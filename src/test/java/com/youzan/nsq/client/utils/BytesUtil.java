package com.youzan.nsq.client.utils;

/**
 * Created by leen on 6/21/17.
 */
public class BytesUtil {

    public static void writeInt(byte[] bytes, int offset, int value) {
        bytes[offset] = (byte) (value & 0xFF);
        bytes[offset + 1] = (byte) ((value >> 8) & 0xFF);
        bytes[offset + 2] = (byte) ((value >> 16) & 0xFF);
        bytes[offset + 3] = (byte) ((value >> 24) & 0xFF);
    }

    public static int readInt(byte[] bytes, int offset) {
        int value = bytes[offset] & 0xFF;
        value |=  bytes[offset + 1] << 8 & 0xFF00;
        value |=  bytes[offset + 2] << 16 & 0xFF0000;
        value |=  bytes[offset + 3] << 24 & 0xFF000000;
        return value;
    }

}
