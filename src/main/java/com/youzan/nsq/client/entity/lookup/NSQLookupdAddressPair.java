package com.youzan.nsq.client.entity.lookup;

import com.youzan.nsq.client.entity.IPartitionsSelector;
import com.youzan.nsq.client.entity.MigrationPartitionsSelector;
import com.youzan.nsq.client.entity.Partitions;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQLookupException;

/**
 * Created by lin on 16/12/13.
 */
public class NSQLookupdAddressPair extends NSQLookupdAddress {
    NSQLookupdAddress preLookupdAddress;
    NSQLookupdAddress curLookupdAddress;

    public NSQLookupdAddressPair(String preClusterId, String preLookupdAddress, String curClusterId, String currentLookupAddress, int currentFactor) {
        super(curClusterId, currentLookupAddress);
        this.preLookupdAddress = new NSQLookupdAddress(preClusterId, preLookupdAddress);
        this.curLookupdAddress = new NSQLookupdAddress(curClusterId, currentLookupAddress);
        this.prefactor = 100 - currentFactor;
    }

    @Override
    public IPartitionsSelector lookup(final Topic topic, boolean writable) throws NSQLookupException {
        if (null == topic || null == topic.getTopicText() || topic.getTopicText().isEmpty()) {
            throw new NSQLookupException("Your input topic is blank!");
        }

        //previous cluster first
        try{
            String lookupPre = this.preLookupdAddress.getAddress();
            Partitions aPartitionsPre = lookup(lookupPre, topic, writable);

            String lookupCur = this.curLookupdAddress.getAddress();
            Partitions aPartitionsCur = lookup(lookupCur, topic, writable);

            IPartitionsSelector mps = null;
            if(null != aPartitionsCur && null != aPartitionsPre)
                mps = new MigrationPartitionsSelector(aPartitionsPre, aPartitionsCur, this.prefactor);

            return mps; // maybe it is empty
        } catch (Exception e) {
            final String tip = "SDK can't get the right lookup info. " + this.getAddress();
            throw new NSQLookupException(tip, e);
        }
    }
}
