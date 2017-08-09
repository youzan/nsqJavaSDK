package com.youzan.nsq.client.core;

import com.youzan.nsq.client.IConfigAccessSubscriber;
import com.youzan.nsq.client.configs.*;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.Role;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.entity.lookup.AbstractControlConfig;
import com.youzan.nsq.client.entity.lookup.AbstractSeedLookupdConfig;
import com.youzan.nsq.client.entity.lookup.NSQLookupdAddresses;
import com.youzan.nsq.client.entity.lookup.SeedLookupdAddress;
import com.youzan.nsq.client.exception.NSQConfigAccessException;
import com.youzan.nsq.client.exception.NSQLookupException;
import com.youzan.nsq.client.exception.NSQSeedLookupConfigNotFoundException;
import com.youzan.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LookupAddressUpdate
 * Created by lin on 16/9/23.
 */
public class LookupAddressUpdate implements IConfigAccessSubscriber<AbstractSeedLookupdConfig>{
    private final static Logger logger = LoggerFactory.getLogger(LookupAddressUpdate.class);

    private static final String DEFAULT_CONTROL_CONFIG = "{\"previous\":[],\"current\":[%s],\"gradation\":{}}";
    private final ExecutorService subscribeExec = Executors.newCachedThreadPool(new NamedThreadFactory("LookupAddressUpdate-subscribe-pool", Thread.MAX_PRIORITY));
    private volatile boolean startListLookup = false;
    private static final int LISTLOOKUP_INIT_DELAY = 0;
    private static final String CATE_TOPIC_FORMAT = "%s:%s";
    private final ScheduledExecutorService listLookupExec = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("LookupAddressUpdate-listlookup-single", Thread.MAX_PRIORITY));
    private volatile boolean touched = false;
    private final CountDownLatch latch = new CountDownLatch(1);
    //categorization 2 seed lookup config mapping
    private final Set<String> categorizationsInUsed;
    private final ConcurrentHashMap<String, AbstractSeedLookupdConfig> cat2SeedLookupCnfMap;
    private final AtomicLong clientCnt = new AtomicLong(0);

    class LookupAddressUpdateHandler implements ConfigAccessAgent.IConfigAccessCallback<TreeMap<String, String>> {
        private String categorization;
        private CountDownLatch latch;

        public LookupAddressUpdateHandler(String categorization, CountDownLatch latch) {
            this.categorization = categorization;
            this.latch = latch;
        }

        @Override
        public void process(TreeMap<String, String> newItems) {
            long start = System.currentTimeMillis();
            try {
                updateLookupAddresses(newItems);
                tryListLookupRightNow();
            }finally {
                latch.countDown();
                logger.info("New config update ends in {} millSec", System.currentTimeMillis() - start);
            }
        }

        @Override
        public void fallback(TreeMap<String, String> itemsInCache, Object... objs) {
            long start = System.currentTimeMillis();
            try {
                updateLookupAddresses(itemsInCache);
                tryListLookupRightNow();
            }finally {
                latch.countDown();
                logger.info("New config fallback ends in {} millSec", System.currentTimeMillis() - start);
            }
        }

        private void updateLookupAddresses(final SortedMap<String, String> newLookupAddress){
            if(null == newLookupAddress || newLookupAddress.size() == 0) {
                logger.info("Subscribe returns no result.");
                return;
            }

            AbstractSeedLookupdConfig aSeedLookUpConfig;
            boolean seedLookupCnfExisted = cat2SeedLookupCnfMap.containsKey(categorization);
            if(!seedLookupCnfExisted)
                aSeedLookUpConfig = AbstractSeedLookupdConfig.create(categorization);
            else
                aSeedLookUpConfig = cat2SeedLookupCnfMap.get(categorization);

            for(String topicKey : newLookupAddress.keySet()) {
                //skip default cluster seed lookup info
                if(topicKey.startsWith("##_default"))
                    continue;
                String controlCnfStr = newLookupAddress.get(topicKey);
                AbstractControlConfig ctrlCnf = AbstractControlConfig.create(controlCnfStr);
                aSeedLookUpConfig.putTopicCtrlCnf(formatCategorizationTopic(categorization, topicKey), ctrlCnf);
                logger.info("Control config updated for topic: {} in categorization: {}", topicKey, categorization);
            }

            if(!aSeedLookUpConfig.containsControlConfig()) {
                logger.warn("No control config is picked from updated config, categorization: {}.", this.categorization);
                return;
            }
            if(!seedLookupCnfExisted)
                updateCat2SeedLookupCnfMap(categorization, aSeedLookUpConfig);
        }

        public int hashCode() {
            return this.categorization.hashCode();
        }
    }

    private static final Object LOCK = new Object();
    private static LookupAddressUpdate _INSTANCE = null;

    public LookupAddressUpdate(){
        categorizationsInUsed = new HashSet<>();
        cat2SeedLookupCnfMap = new ConcurrentHashMap<>();
    }

    public static LookupAddressUpdate getInstance(){
        return getInstance(false);
    }

    public static LookupAddressUpdate getInstance(boolean touch){
        if(null == _INSTANCE){
            synchronized (LOCK){
                if(null == _INSTANCE){
                    //init domain and keys
                    _INSTANCE = new LookupAddressUpdate();
                    logger.info("LookupAddressUpdate instance initialized.");
                }
            }
        }
        if(touch){
            _INSTANCE.touched();
        }
        return _INSTANCE;
    }

    public static void setInstance(final LookupAddressUpdate lau) {
        synchronized (LOCK){
           _INSTANCE = lau;
        }
    }

    public static String formatCategorizationTopic(String categorization, final String topicName) {
        if(null == categorization || null == topicName)
            throw new IllegalArgumentException("Neither categorization nor topic is null.");
        return String.format(CATE_TOPIC_FORMAT, categorization, topicName);
    }

    private String generateSeedLookupdsJsonStr(final String[] seedLookups) {
        if(null == seedLookups || seedLookups.length == 0){
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for(String aSeed : seedLookups) {
            sb.append("\"").append(aSeed).append("\"").append(",");
        }
        return sb.substring(0, sb.length()-1);
    }

    private boolean isTouched() {
        return this.touched;
    }

    private void touch() {
        this.touched = true;
    }

    /**
     * function to set up user specified seed lookup address
     * @param lookupLocalID lookupLocalId used to specify lookup addresses of client's own.
     * @param seedLookups seed lookup addresses specified from user API {@link NSQConfig#setLookupAddresses(String)}.
     */
    public void setUpDefaultSeedLookupConfig(int lookupLocalID, final String[] seedLookups){
        if(null == seedLookups || seedLookups.length == 0) {
            logger.warn("Pass in seed lookup address should not be null.");
            return;
        }

        int id = lookupLocalID;
        String categorization = TopicRuleCategory.TOPIC_CATEGORIZATION_USER_SPECIFIED + '_' + id;
        AbstractSeedLookupdConfig aSeedLookUpConfig = AbstractSeedLookupdConfig.create(categorization);
        if(checkinCategorization(categorization)) {
            String defaultSeedsStr = generateSeedLookupdsJsonStr(seedLookups);
            String defaultSeedLookupCtrlCnfStr = String.format(DEFAULT_CONTROL_CONFIG, defaultSeedsStr);
            AbstractControlConfig ctrlCnf = AbstractControlConfig.create(defaultSeedLookupCtrlCnfStr);
            if(null != ctrlCnf) {
                aSeedLookUpConfig.putTopicCtrlCnf(formatCategorizationTopic(categorization, Topic.TOPIC_DEFAULT.getTopicText()), ctrlCnf);
                updateCat2SeedLookupCnfMap(categorization, aSeedLookUpConfig);
            }
        }
    }

    public void removeDefaultSeedLookupConfig(int lookupLocalID) {
        String categorization = TopicRuleCategory.TOPIC_CATEGORIZATION_USER_SPECIFIED + '_' + lookupLocalID;
        cat2SeedLookupCnfMap.remove(categorization);
    }

    @Override
    public AbstractSeedLookupdConfig subscribe(ConfigAccessAgent subscribeTo, final AbstractConfigAccessDomain domain, final AbstractConfigAccessKey<Role>[] keys, final ConfigAccessAgent.IConfigAccessCallback callback) throws NSQConfigAccessException {
        if(null == keys || keys.length == 0) {
            logger.warn("Only one key allowed");
            return null;
        }

        AbstractConfigAccessKey<Role> roleKey = keys[0];
        final String topic = (String) domain.getInnerDomain();
        TopicRuleCategory topicRoleCat = TopicRuleCategory.getInstance(roleKey.getInnerKey());
        String categorization = topicRoleCat.category(topic);

        logger.info("LookupAddressUpdate Instance subscribe to {}, for {}.", subscribeTo, roleKey.toKey());
        SortedMap<String, String> firstSeedLookupAddress = subscribeTo.handleSubscribe(domain, keys, callback);

        //handle first subscribe
        if(null == firstSeedLookupAddress || firstSeedLookupAddress.size() == 0) {
            checkoutCategorization(categorization);
            throw new NSQConfigAccessException("Subscribe to " + subscribeTo + " returns no result. ConfigAccessAgent metas:" + subscribeTo.metadata());
        }

        //count down latch need counted down here
        callback.process(firstSeedLookupAddress);

        //as there is no function invoke current function for return value, just return null no
        return null;
    }

    public LookupAddressUpdateHandler createCallbackHandler(String categorization, CountDownLatch latch) {
        if(logger.isDebugEnabled())
            logger.debug("call back handler created for Categorization: {}.", categorization);
        return new LookupAddressUpdateHandler(categorization, latch);
    }

    private boolean checkinCategorization(String categorization){
        if (!cat2SeedLookupCnfMap.containsKey(categorization)) {
            //then need to update cat2SeedLookupCnfMap
            synchronized (categorizationsInUsed) {
                if (!categorizationsInUsed.contains(categorization)) {
                    return categorizationsInUsed.add(categorization);
                }
            }
        }
        return false;
    }

    /**
     * Checkout categorization from categorization to seed lookup mapping, it is a two-phase checkout.
     * @param categorization categorization string
     * @return {@link Boolean#TRUE} if categorization exists and checked out. otherwise {@link Boolean#FALSE}
     */
    private boolean checkoutCategorization(String categorization){
        //then need to update cat2SeedLookupCnfMap
        synchronized (categorizationsInUsed) {
            if (categorizationsInUsed.contains(categorization)) {
                boolean removedInUsedSet = categorizationsInUsed.remove(categorization);
                if(cat2SeedLookupCnfMap.containsKey(categorization)){
                    return null != cat2SeedLookupCnfMap.remove(categorization);
                }else{
                    return removedInUsedSet;
                }
            }
            return false;
        }
    }

    private void updateCat2SeedLookupCnfMap(String categorization, AbstractSeedLookupdConfig newCnf) {
        AbstractSeedLookupdConfig oldCnf = cat2SeedLookupCnfMap.put(categorization, newCnf);
        if(null != oldCnf) {
            //there should be no be any old could enter this line
            logger.error("Old SeedLookupdConfig should not exist for categorization {}.", categorization);
        }
    }

    /**
     * Subscribe topics to configAccess
     * @param category category pass in topics belong to.
     * @param topics topics to subscribe
     */
    public void subscribe(final TopicRuleCategory category, String... topics) {
        if(null == topics || topics.length == 0)
            return;
        List<String> topics4Subscribe = new ArrayList<>();
        for(final String topic : topics) {
            final String categorization = category.category(topic);
            if (checkinCategorization(categorization))
                topics4Subscribe.add(topic);
        }
        final CountDownLatch latch = new CountDownLatch(topics4Subscribe.size());
        for(final String topic: topics4Subscribe){
            subscribeExec.submit(new Runnable(){
                public void run() {
                //return a cached value or subscribe new and return cached value
                //try subscribing and check what the returning value is
                String categorization = category.category(topic);
                try {
                    //subscribes categorization to config access and returns
                    subscribe(ConfigAccessAgent.getInstance(), DCCMigrationConfigAccessDomain.getInstance(topic), new AbstractConfigAccessKey[]{DCCMigrationConfigAccessKey.getInstance(category.getRole())}, createCallbackHandler(categorization, latch));
                }catch(NSQConfigAccessException caE){
                    logger.error("Error in checking seed lookup address config for categorization {}, topic {}.", categorization, topic, caE);
                }catch(Exception exp) {
                    logger.error("Unexpected error in checking seed lookup address config for categorization {}, topic {}.", categorization, topic, exp);
                    checkoutCategorization(categorization);
                }finally {
                }
                }
            });
        }

        boolean timeout;
        if(topics4Subscribe.size() > 0)
            try {
            long start = System.currentTimeMillis();
                timeout = latch.await(NSQConfig.getQueryTimeout4TopicSeedInMillisecond() * topics4Subscribe.size(), TimeUnit.MILLISECONDS);
                if(!timeout) {
                    logger.error("Timeout waiting for querying {} topic seed lookup address. Current query timeout for topic seed lookup is {}.", topics.length, NSQConfig.getQueryTimeout4TopicSeedInMillisecond());
                    //try checking latch counter to see if there is still some successfully subscribed categorization
                    if(latch.getCount() < topics4Subscribe.size()) {
                        logger.warn("Topics subscribe returns partially response.");
                    }
                }
                if(logger.isDebugEnabled()) {
                    logger.debug("Tooks {} millSec to wait for subscribe response from config remote.", System.currentTimeMillis() - start);
                }
            } catch (InterruptedException e) {
                logger.error("Thread interrupted waiting for seed lookup address update.");
            }
    }

    private volatile long listlookupLastRun;
    /**
     * invoke listlookup API for ALL cached {@link SeedLookupdAddress}
     * leave client(consumer&producer) to kick off listlookup
     */
    void keepListLookup() {
        if(!startListLookup) {
            synchronized (listLookupExec) {
                if(!startListLookup) {
                    listLookupExec.scheduleWithFixedDelay(new Runnable() {
                        public void run() {
                            while(!isTouched() && 0 == SeedLookupdAddress.seedLookupMapSize()){
                                logger.info("Backoff list lookup request as there is no seed lookupd address found.");
                                try {
                                    Thread.sleep(500);
                                } catch (InterruptedException e) {
                                    logger.error("Interrupted while waiting for new seed lookupd address.");
                                }
                            }
                            int success;
                            try {
                                lockListlookupProcess();
                                success = SeedLookupdAddress.listAllLookup();
                            }finally {
                                unlockListlookupProcess();
                            }
                            listlookupLastRun = System.currentTimeMillis();
                            if(!isTouched()) {
                                if(success > 0) {
                                    logger.info("New lookupd addresses are ready.");
                                    touch();
                                    latch.countDown();
                                } else {
                                    logger.error("No lookupd address found after first list lookup request, make sure valid seed lookup address(es) offered.");
                                }
                            }

                        }
                    }, LISTLOOKUP_INIT_DELAY, NSQConfig.getListLookupIntervalInSecond(), TimeUnit.SECONDS);
                    startListLookup = true;
                    logger.info("ListLookup process starts.");
                }
            }
        }
    }

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private void lockListlookupProcess() {
        lock.writeLock().lock();
    }

    private boolean tryLockListlookupProcess() {
        return lock.writeLock().tryLock();
    }

    private void unlockListlookupProcess() {
        lock.writeLock().unlock();
    }

    private void tryListLookupRightNow() {
        if(listlookupLastRun + NSQConfig.getListLookupIntervalInSecond()*1000 - System.currentTimeMillis() < 5000) {
            logger.info("listlookup routine will start soon, tryListlookupRightNow exits.");
            return;
        }

        if(tryLockListlookupProcess())
            try {
                logger.info("List lookup start refresh...");
                SeedLookupdAddress.listAllLookup();
                logger.info("List lookup refresh ends.");
            } finally {
                unlockListlookupProcess();
            }
        //fail to lock, that is fine.
    }

    /**
     * Get one lookup address with given {@link Topic} and {@link TopicRuleCategory} (has role info)
     * @param topic topic
     * @param category category to decide role of NSQ client
     * @param localLookupd {@link Boolean#TRUE} if user specified lookupd address is applied, which will override seed
     *                                         lookupd from config access remote, otherwise {@link Boolean#FALSE}.
     * @param force {@link Boolean#TRUE} to force seed lookup address to update it lookupd address, before return
     *                                  a lookupd address, otherwise {@link Boolean#FALSE}.
     * @param lookupLocalID int value to form lookup categorization key for client
     * @return {@link NSQLookupdAddresses} lookupd address.
     * @throws NSQLookupException {@link NSQLookupException} exception during lookup process.
     * @throws NSQSeedLookupConfigNotFoundException raised when seed lookup control config in remote not found.
     */
    public NSQLookupdAddresses getLookup(final String topic, TopicRuleCategory category, boolean localLookupd, boolean force, int lookupLocalID) throws NSQLookupException, NSQSeedLookupConfigNotFoundException {
        int retry = 3;
        String categorization = null;
        AbstractSeedLookupdConfig aSeedLookupCnf = null;
        String topicInner = null;
        while(null == aSeedLookupCnf && retry-- > 0) {
            if (localLookupd && lookupLocalID > 0) {
                categorization = TopicRuleCategory.TOPIC_CATEGORIZATION_USER_SPECIFIED + '_' + lookupLocalID;
                aSeedLookupCnf = cat2SeedLookupCnfMap.get(categorization);
                topicInner = Topic.TOPIC_DEFAULT.getTopicText();
            } else {
                categorization = category.category(topic);
                this.subscribe(category, topic);
                aSeedLookupCnf = cat2SeedLookupCnfMap.get(categorization);
                topicInner = topic;
            }
            if(null == aSeedLookupCnf)
                //backoff a while
                try {
                    Thread.sleep(NSQConfig.getQueryTimeout4TopicSeedInMillisecond());
                } catch (InterruptedException e) {
                    logger.error("Interrupted while waiting for another lookup process for categorization {}", categorization);
                }
        }

        if (null == aSeedLookupCnf)
            throw new NSQSeedLookupConfigNotFoundException("Seed Lookup config not found for Topic: " + topic + " Categorization: " + categorization);

        //await for list lookup address to signal first success
        if(!this.touched) {
            try {
                if(!latch.await(30, TimeUnit.SECONDS)) {
                    throw new NSQLookupException("Timeout for first seed lookup address to list lookup.");
                }
            }catch (InterruptedException e) {
                logger.warn("Thread interrupted waiting for new lookup address incoming.");
            }
        }

        return aSeedLookupCnf.punchLookupdAddress(categorization, topicInner, force);
    }

    private long touched(){
        return this.clientCnt.incrementAndGet();
    }

    public long closed(){
        long cnt = this.clientCnt.decrementAndGet();
        if(cnt == 0) {
            synchronized (clientCnt){
                if(clientCnt.get() == 0) {
                    this.close();
                }
            }
        }
        return cnt;
    }

    private void close() {
        synchronized (LOCK){
            this.listLookupExec.shutdownNow();
            for(AbstractSeedLookupdConfig seedCnf:this.cat2SeedLookupCnfMap.values())
                seedCnf.clean();
            this.cat2SeedLookupCnfMap.clear();
            this.categorizationsInUsed.clear();
            _INSTANCE = null;
        }
        logger.info("LookupAddressUpdate instance closed.");
    }
}
