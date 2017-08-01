package com.youzan.nsq.client.configs;

import com.youzan.nsq.client.core.LookupAddressUpdate;
import com.youzan.nsq.client.entity.lookup.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.SoftReference;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
     * Not used in 2.3
     * @param clusterInfo cluster info
     * @param seedLookup  seed lookupd address of default cluster
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
     * Get nsq cluster info associated migration control config.
     * Not used in 2.3
     * @param clusterInfo default clusterinfo
     * @return seed lookupd address for default cluster
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
    public List<SoftReference<SeedLookupdAddress>> getSeedLookupAddress(String categorization, final String topic) {
        AbstractControlConfig ctrlCnf = getTopicCtrlCnf(LookupAddressUpdate.formatCategorizationTopic(categorization, topic));
        if (null == ctrlCnf)
            return null;
        return ctrlCnf.getSeeds();
    }

    /**
     * punch one lookup address from one Reference to {@link SeedLookupdAddress}
     * @param categorization categorization string topic fall into.
     * @param topic topic for lookup.
     * @param force force seedlookup address to lookup if {@link Boolean#TRUE},
     *              otherwise punch lookup address from cache.
     * @return NSQ lookup address if there is any in cache, otherwise null.
     */
    @Override
    public NSQLookupdAddresses punchLookupdAddress(String categorization, final String topic, boolean force) {
        AbstractControlConfig ctrlCnf = getTopicCtrlCnf(LookupAddressUpdate.formatCategorizationTopic(categorization, topic));
        if (null == ctrlCnf)
            return null;
        List<SoftReference<SeedLookupdAddress>> curRefs = ctrlCnf.getCurrentReferences();
        List<SoftReference<SeedLookupdAddress>> preRefs = ctrlCnf.getPreviousReferences();

        //use a set here to category seed lookups belong to different cluster by hostname
        Set<String> clusterIds = new HashSet<>();
        List<String> curLookupds = new ArrayList<>();
        List<String> curClusterIds = new ArrayList<>();
        if (null != curRefs && curRefs.size() > 0) {
            for(SoftReference<SeedLookupdAddress> seedRef : curRefs){
                try {
                    SeedLookupdAddress aCurSeed = seedRef.get();
                    String clusterId = aCurSeed.getClusterId();
                    //clusterIds set is used to make sure that there is only ONE lookupd address from one NSQd cluster
                    if(!clusterIds.contains(clusterId)){
                        //punch ONE lookupd address from one cluster seed lookupd address
                        String curLookupd = aCurSeed.punchLookupdAddressStr(force);
                        if(null != curLookupd) {
                            curLookupds.add(curLookupd);
                            curClusterIds.add(clusterId);
                            clusterIds.add(clusterId);
                        }
                    }
                }catch(NullPointerException npe) {
                    //swallow it
                }
            }
        }

        clusterIds.clear();
        List<String> preLookupds = new ArrayList<>();
        List<String> preClusterIds = new ArrayList<>();
        if (null != preRefs && preRefs.size() > 0) {
            for(SoftReference<SeedLookupdAddress> seedRef : preRefs){
                SeedLookupdAddress aPreSeed = seedRef.get();
                String clusterId = aPreSeed.getClusterId();
                String address = aPreSeed.getAddress();
                try {
                    URL url = new URL(address);
                    String host = url.getHost();
                    if(!clusterIds.contains(host)){
                        String preLookupd = aPreSeed.punchLookupdAddressStr(force);
                        if(null != preLookupd) {
                            preLookupds.add(preLookupd);
                            preClusterIds.add(clusterId);
                            clusterIds.add(host);
                        }
                    }
                }catch(NullPointerException npe){
                    //swallow it
                } catch (MalformedURLException e) {
                    logger.error("Seed lookupd address {} is NOT valid URL form. Skip and check with config access remote.", address);
                }
            }
        }

        //create one NSQLookupdAddress instance
        NSQLookupdAddresses aLookupAddress = null;
        if (preLookupds.size() > 0 && curLookupds.size() > 0)
            aLookupAddress = NSQLookupdAddresses.create(preClusterIds, preLookupds, curClusterIds, curLookupds, ctrlCnf.getGradation().getPercentage().getFactor());
        else if (curLookupds.size() > 0)
            aLookupAddress = NSQLookupdAddresses.create(curClusterIds, curLookupds);
        else {
            logger.warn("No lookup address found for passin topic {}, categorization {}.", topic, categorization);
        }
        return aLookupAddress;
    }
}
