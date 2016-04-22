package com.youzan.nsq.client.frames;

import java.util.HashMap;
import java.util.Map;

public enum FrameType {
    RESPONSE(0), ERROR(1), MESSAGE(2);
    private static Map<Integer, FrameType> mappings;

    static {
        mappings = new HashMap<Integer, FrameType>();
        for (FrameType t : FrameType.values()) {
            mappings.put(t.getCode(), t);
        }
    }

    private int code;

    private FrameType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static FrameType fromCode(int code) {
        return mappings.get(code);
    }
}
