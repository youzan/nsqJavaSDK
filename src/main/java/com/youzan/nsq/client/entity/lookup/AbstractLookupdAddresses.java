package com.youzan.nsq.client.entity.lookup;

import java.util.List;

/**
 * Lookupd addresses gathers from previous clusters and current clusters, addresses in previous or current part comes
 * from different clusters, That is the difference between {@link AbstractLookupdAddress}
 * Created by lin on 17/2/9.
 */
public class AbstractLookupdAddresses {
    protected List<String> addresses;
    private List<String> clusterIds;

    public AbstractLookupdAddresses(List<String> clusterIds, List<String> addresses){
        this.clusterIds = clusterIds;
        this.addresses = addresses;
    }

    public List<String> getAddresses() {
        return this.addresses;
    }

    public List<String> getClusterIds() {
        return this.clusterIds;
    }

    public boolean isValid(){
        return null != this.clusterIds && null != addresses && this.clusterIds.size() > 0 && this.addresses.size() > 0;
    }


}
