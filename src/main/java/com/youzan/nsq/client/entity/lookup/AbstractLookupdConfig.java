package com.youzan.nsq.client.entity.lookup;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by lin on 16/12/9.
 */
public abstract class AbstractLookupdConfig {

    private ReentrantReadWriteLock topicLock = new ReentrantReadWriteLock();
    private ConcurrentHashMap<String, AbstractControlConfig> topicnfMap = new ConcurrentHashMap();

    public AbstractLookupdConfig(){
    }

    /**
     * Update topic associated migration control config.
     * @param categorizationTopic categorization tailed with topic name
     * @param cc new control config
     */
    public void putTopicCtrlCnf(String categorizationTopic, final AbstractControlConfig cc) {
        if(null == categorizationTopic || null == cc)
            return;
        try{
            topicLock.writeLock().lock();
            AbstractControlConfig oldCtrlConf = topicnfMap.put(categorizationTopic, cc);
            //if old control conf is not null, need to remove reference from SeedLookupAddress map
            if(null != oldCtrlConf) {
                oldCtrlConf.clean();
            }
        }finally {
            topicLock.writeLock().unlock();
        }
    }

    /**
     * get topic associated migration control config
     * @param categorizationTopic categorization tailed with topic name
     * @return control config
     */
    public AbstractControlConfig getTopicCtrlCnf(String categorizationTopic) {
        if(null == categorizationTopic)
            return null;
        try{
            topicLock.readLock().lock();
            if(topicnfMap.containsKey(categorizationTopic))
                return topicnfMap.get(categorizationTopic);
        }finally {
            topicLock.readLock().unlock();
        }
        return null;
    }

    public void clean() {
        try{
            topicLock.writeLock().lock();
            for(AbstractControlConfig ctrlCnf : topicnfMap.values()) {
                ctrlCnf.clean();
            }
        }finally {
            topicLock.writeLock().unlock();
        }
    }

    public boolean containsControlConfig() {
        return !this.topicnfMap.isEmpty();
    }
}
