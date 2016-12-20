package com.youzan.nsq.client.entity.lookup;

/**
 * Created by lin on 16/12/5.
 */
public class DefaultSeedLookupd {
    private volatile String address;

    public DefaultSeedLookupd(String seedLookupd) {
        this.address = seedLookupd;
    }

    public String getAddress() {
        return this.address;
    }
}
