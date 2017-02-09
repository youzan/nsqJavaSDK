package com.youzan.nsq.client.entity.lookup;

/**
 *
 * Created by lin on 16/12/5.
 */
public abstract class AbstractLookupdAddress {
    protected String address;
    private String clusterId;

    public AbstractLookupdAddress(String clusterId, String address){
        this.clusterId = clusterId;
        this.address = address;
    }

    public String getAddress() {
        return this.address;
    }

    public String getClusterId() {
        return this.clusterId;
    }

    public boolean isValid(){
        return null != this.clusterId && null != address;
    }

}
