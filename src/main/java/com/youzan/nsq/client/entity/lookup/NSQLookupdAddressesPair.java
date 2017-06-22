package com.youzan.nsq.client.entity.lookup;

import com.youzan.nsq.client.entity.IPartitionsSelector;
import com.youzan.nsq.client.entity.MigrationPartitionsSelector;
import com.youzan.nsq.client.entity.Partitions;
import com.youzan.nsq.client.exception.NSQLookupException;
import com.youzan.nsq.client.exception.NSQProducerNotFoundException;
import com.youzan.nsq.client.exception.NSQTopicNotFoundException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lin on 16/12/13.
 */
public class NSQLookupdAddressesPair extends NSQLookupdAddresses {
    NSQLookupdAddresses preLookupdAddress;
    NSQLookupdAddresses curLookupdAddress;

    public NSQLookupdAddressesPair(List<String> preClusterIds, List<String> preLookupdAddresses, List<String> curClusterIds, List<String> currentLookupAddresses, int currentFactor) {
        //not used here, just for initialization of constructor
        super(curClusterIds, currentLookupAddresses);
        this.preLookupdAddress = new NSQLookupdAddresses(preClusterIds, preLookupdAddresses);
        this.curLookupdAddress = new NSQLookupdAddresses(curClusterIds, currentLookupAddresses);
        this.prefactor = 100 - currentFactor;
    }

    @Override
    public IPartitionsSelector lookup(final String topic, boolean writable) throws NSQLookupException, NSQProducerNotFoundException, NSQTopicNotFoundException {
        if (null == topic || topic.isEmpty()) {
            throw new NSQLookupException("Your input topic is blank!");
        }

        //previous cluster first
        try{
            List<String> lookupPres = this.preLookupdAddress.getAddresses();
            List<Partitions> partitionsPres = new ArrayList<>();
            for(String aLookupPre : lookupPres) {
                Partitions aPartitionsPre = lookup(aLookupPre, topic, writable);
                if (null != aPartitionsPre)
                    partitionsPres.add(aPartitionsPre);
            }

            List<String> lookupCur = this.curLookupdAddress.getAddresses();
            List<Partitions> partitionsCurs = new ArrayList<>();
            for(String aLookupCur : lookupCur) {
                Partitions aPartitionsCur = lookup(aLookupCur, topic, writable);
                if(null != aPartitionsCur)
                    partitionsCurs.add(aPartitionsCur);
            }

            IPartitionsSelector mps = null;
            if(partitionsCurs.size() > 0 && partitionsPres.size() > 0)
                mps = new MigrationPartitionsSelector(partitionsPres, partitionsCurs, this.prefactor);

            return mps; // maybe it is empty
        } catch (IOException e) {
            final String tip = "SDK can't get the right lookup info from seed lookupd address. ";
            throw new NSQLookupException(tip, e);
        }
    }
}
