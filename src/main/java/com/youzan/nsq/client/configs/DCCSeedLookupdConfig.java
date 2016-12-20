package com.youzan.nsq.client.configs;

import com.youzan.nsq.client.core.LookupAddressUpdate;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.entity.lookup.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.SoftReference;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * APP: %s.nsq.lookupd.addr
 * <p>
 * Key: producer(consumer)
 * <p>
 * type:    combination
 * <p>
 * [
 * <p>
 * "##_default":"",
 * <p>
 * "##_default_global":"global_lookupd",
 * <p>
 * "##_default_sync":"sync_lookupd",
 * <p>
 * "topic1":"migration_control_config1",
 * <p>
 * "topic2":"migration_control_config2",
 * <p>
 * ]
 * <p>
 * Created by lin on 16/12/5.
 */
public class DCCSeedLookupdConfig extends AbstractSeedLookupdConfig {
    private static final Logger logger = LoggerFactory.getLogger(DCCSeedLookupdConfig.class);

    private static final String DEFAULT_PREFIX = "##_default";
    private volatile int INDEX = 0;
    private ReentrantReadWriteLock defaultLock = new ReentrantReadWriteLock();
    private ConcurrentHashMap<String, DefaultSeedLookupd> defaultcnfMap = new ConcurrentHashMap<>();
    private final String categorization;

    public DCCSeedLookupdConfig(String categorization) {
        super();
        this.categorization = categorization;
    }

    /**
     * Update nsq cluster info associated migration control config.
     *
     * @param clusterInfo
     * @param seedLookup
     */
    @Deprecated
    public void putDefaultCluster(String clusterInfo, String seedLookup) {
        if (null == clusterInfo || null == seedLookup || !clusterInfo.startsWith(DEFAULT_PREFIX))
            return;
        try {
            defaultLock.writeLock().lock();
            DefaultSeedLookupd aDefaultSeedLookup = new DefaultSeedLookupd(seedLookup);
            defaultcnfMap.put(clusterInfo, aDefaultSeedLookup);
        } finally {
            defaultLock.writeLock().unlock();
        }
    }

    /**
     * Get nsq cluster info associated migration control config
     *
     * @param clusterInfo
     * @return
     */
    @Deprecated
    public String getDefaultCluster(String clusterInfo) {
        if (null == clusterInfo || !clusterInfo.startsWith(DEFAULT_PREFIX))
            return null;
        try {
            defaultLock.readLock().lock();
            DefaultSeedLookupd aDefaultSeedLookup = defaultcnfMap.get(clusterInfo);
            if (null != aDefaultSeedLookup)
                return aDefaultSeedLookup.getAddress();
        } finally {
            defaultLock.readLock().unlock();
        }
        return null;
    }

    @Override
    public List<SoftReference<SeedLookupdAddress>> getSeedLookupAddress(String categorization, final Topic topic) {
        AbstractControlConfig ctrlCnf = getTopicCtrlCnf(LookupAddressUpdate.formatCategorizationTopic(categorization, topic.getTopicText()));
        if (null == ctrlCnf)
            return null;
        return ctrlCnf.getSeeds();
    }

    /**
     * punch one lookup address from one Reference to {@link SeedLookupdAddress}
     *
     * @param topic
     * @return NSQ lookup address if there is any in cache, otherwise null.
     */
    @Override
    public NSQLookupdAddress punchLookupdAddress(String categorization, final Topic topic) {
        AbstractControlConfig ctrlCnf = getTopicCtrlCnf(LookupAddressUpdate.formatCategorizationTopic(categorization, topic.getTopicText()));
        if (null == ctrlCnf)
            return null;
        List<SoftReference<SeedLookupdAddress>> curRefs = ctrlCnf.getCurrentReferences();
        List<SoftReference<SeedLookupdAddress>> preRefs = ctrlCnf.getPreviousReferences();
        String curLookupd = null, preLookupd = null;
        SeedLookupdAddress aCurSeed = null, aPreSeed = null;
        if (curRefs.size() > 0) {
            try {
                aCurSeed = curRefs.get((INDEX++ & Integer.MAX_VALUE) % curRefs.size()).get();
                curLookupd = aCurSeed.punchLookupdAddressStr();
            }catch(NullPointerException npe){
                //swallow it
            }
        }
        if (preRefs.size() > 0) {
            try {
                aPreSeed = preRefs.get((INDEX++ & Integer.MAX_VALUE) % preRefs.size()).get();
                preLookupd = aPreSeed.punchLookupdAddressStr();
            }catch(NullPointerException npe){
                //swallow it
            }
        }

        //create one NSQLookupdAddress instance
        NSQLookupdAddress aLookupAddress = null;
        if (null != preLookupd && null != curLookupd)
            aLookupAddress = NSQLookupdAddress.create(aPreSeed.getClusterId(), preLookupd, aCurSeed.getClusterId(), curLookupd, ctrlCnf.getGradation().getPercentage().getFactor());
        else if (null != curLookupd)
            aLookupAddress = NSQLookupdAddress.create(aCurSeed.getClusterId(), curLookupd);
        else {
            logger.warn("No lookup address found for passin topic {}, categorization {}.", topic.getTopicText(), categorization);
        }
        return aLookupAddress;
    }
}
