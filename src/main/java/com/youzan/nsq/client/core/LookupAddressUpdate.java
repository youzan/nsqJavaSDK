package com.youzan.nsq.client.core;

import com.youzan.nsq.client.entity.NSQConfig;

import java.sql.Timestamp;

/**
 * Created by lin on 16/9/23.
 */
public class LookupAddressUpdate {
    private NSQConfig config;
    private Timestamp lastUpdateTimestamp = new Timestamp(0L);

    public LookupAddressUpdate(final NSQConfig config){
        this.config = config;
    }

    public String[] getNewLookupAddress(){
        Timestamp tmpTimestamp = new Timestamp(this.lastUpdateTimestamp.getTime());
        String[] newLookups = this.config.getLookupAddresses(tmpTimestamp);
        if(tmpTimestamp.after(this.lastUpdateTimestamp)){
            //update
            this.lastUpdateTimestamp = tmpTimestamp;
        }
        return newLookups;
    }
}
