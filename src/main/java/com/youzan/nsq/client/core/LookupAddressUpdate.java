package com.youzan.nsq.client.core;

import com.youzan.nsq.client.entity.NSQConfig;

/**
 * Created by lin on 16/9/23.
 */
public class LookupAddressUpdate {
    private NSQConfig config;
    private Long lastUpdateTimestamp = 0l;

    public LookupAddressUpdate(final NSQConfig config){
        this.config = config;
    }

    public String[] getLookupAddress(){
        Long tmp = new Long(this.lastUpdateTimestamp);
        String[] newLookups = this.config.getLookupAddresses(tmp);
        if(tmp > this.lastUpdateTimestamp){
            //update
            this.lastUpdateTimestamp = tmp;
        }
        return newLookups;
    }
}
