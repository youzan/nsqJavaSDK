package com.youzan.nsq.client.remoting.listener;

import java.util.EventObject;

/**
 * Created by pepper on 14/12/15.
 */
public class NSQEvent extends EventObject {
	private static final long serialVersionUID = 1111998555336264327L;
	private byte[] data;
    private String type;

    public static final String STARTED = "Started";
    public static final String READ = "Read";

    public NSQEvent(String type , byte[] data) {
        super(data);
        this.type = type;
        this.data = data;
    }

    public String getMessage() {
        return new String(data);
    }
    
    public byte[] getData() {
    	return data;
    }

    public String getType() {
        return type;
    }
}
