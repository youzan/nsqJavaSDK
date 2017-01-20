package com.youzan.nsq.client.core;

import com.youzan.nsq.client.IConfigAccessSubscriber;
import com.youzan.nsq.client.configs.*;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.Role;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.entity.lookup.AbstractControlConfig;
import com.youzan.nsq.client.entity.lookup.AbstractSeedLookupdConfig;
import com.youzan.nsq.client.entity.lookup.NSQLookupdAddress;
import com.youzan.nsq.client.entity.lookup.SeedLookupdAddress;
import com.youzan.nsq.client.exception.NSQConfigAccessException;
import com.youzan.nsq.client.exception.NSQLookupException;
import com.youzan.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

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

    //categorization 2 seed lookup config mapping
    private final Set<String> categorizationsInUsed;
    private final ConcurrentHashMap<String, AbstractSeedLookupdConfig> cat2SeedLookupCnfMap;
    private final AtomicLong clientCnt = new AtomicLong(0);

    class LookupAddressUpdateHandler implements ConfigAccessAgent.IConfigAccessCallback<TreeMap<String, String>> {
        private String categorization;

        public LookupAddressUpdateHandler(String categorization) {
            this.categorization = categorization;
        }

        @Override
        public void process(TreeMap<String, String> newItems) {
            updateLookupAddresses(newItems);
        }

        @Override
        public void fallback(TreeMap<String, String> itemsInCache, Object... objs) {
            updateLookupAddresses(itemsInCache);
        }

        private void updateLookupAddresses(final SortedMap<String, String> newLookupAddress){
            if(null == newLookupAddress || newLookupAddress.size() == 0) {
                logger.info("Subscribe returns no result.");
                return;
            }

            AbstractSeedLookupdConfig aSeedLookUpConfig = AbstractSeedLookupdConfig.create(categorization);
            for(String topicKey : newLookupAddress.keySet()) {
                //skip default cluster seed lookup info
                if(topicKey.startsWith("##_default"))
                    continue;
                String controlCnfStr = newLookupAddress.get(topicKey);
                AbstractControlConfig ctrlCnf = AbstractControlConfig.create(controlCnfStr);
                aSeedLookUpConfig.putTopicCtrlCnf(formatCategorizationTopic(categorization, topicKey), ctrlCnf);
                if(logger.isDebugEnabled()){
                    logger.debug("Control config udpated for topic: {} in categorization: {}", topicKey, categorization);
                }
            }

            if(!aSeedLookUpConfig.containsControlConfig()) {
                logger.warn("No control config is picked from updated config, categorization: {}.", this.categorization);
                return;
            }
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
        if(null == _INSTANCE && touch){
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
            throw new RuntimeException("Neither categorization nor topic is null.");
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

    /**
     * function to set up user specified seed lookup address
     * @param seedLookups seed lookup addresses specified from user API {@link NSQConfig#setLookupAddresses(String)}.
     */
    public void setUpDefaultSeedLookupConfig(final String[] seedLookups){
        if(null == seedLookups || seedLookups.length == 0) {
            logger.warn("Pass in seed lookup address should not be null.");
            return;
        }

        AbstractSeedLookupdConfig aSeedLookUpConfig = AbstractSeedLookupdConfig.create(TopicRuleCategory.TOPIC_CATEGORIZATION_USER_SPECIFIED);
        if(checkinCategorization(TopicRuleCategory.TOPIC_CATEGORIZATION_USER_SPECIFIED)) {
            String defaultSeedsStr = generateSeedLookupdsJsonStr(seedLookups);
            String defaultSeedLookupCtrlCnfStr = String.format(DEFAULT_CONTROL_CONFIG, defaultSeedsStr);
            AbstractControlConfig ctrlCnf = AbstractControlConfig.create(defaultSeedLookupCtrlCnfStr);
            if(null != ctrlCnf) {
                aSeedLookUpConfig.putTopicCtrlCnf(formatCategorizationTopic(TopicRuleCategory.TOPIC_CATEGORIZATION_USER_SPECIFIED, Topic.TOPIC_DEFAULT.getTopicText()), ctrlCnf);
                updateCat2SeedLookupCnfMap(TopicRuleCategory.TOPIC_CATEGORIZATION_USER_SPECIFIED, aSeedLookUpConfig);
            }
        }
    }

    @Override
    public AbstractSeedLookupdConfig subscribe(ConfigAccessAgent subscribeTo, final AbstractConfigAccessDomain domain, final AbstractConfigAccessKey<Role>[] keys, final ConfigAccessAgent.IConfigAccessCallback callback) throws NSQConfigAccessException {
        if(null == keys || keys.length == 0) {
            logger.warn("Only one keys allowed");
            return null;
        }

        AbstractConfigAccessKey<Role> roleKey = keys[0];
        final Topic topic = (Topic) domain.getInnerDomain();
        TopicRuleCategory topicRoleCat = TopicRuleCategory.getInstance(roleKey.getInnerKey());
        String categorization = topicRoleCat.category(topic);

        logger.info("LookupAddressUpdate Instance subscribe to {}.", subscribeTo);
        SortedMap<String, String> firstSeedLookupAddress = subscribeTo.handleSubscribe(domain, keys, callback);

        //handle first subscribe
        if(null == firstSeedLookupAddress || firstSeedLookupAddress.size() == 0) {
            checkoutCategorization(categorization);
            throw new NSQConfigAccessException("Subscribe to " + subscribeTo + " returns no result. ConfigAccessAgent metas:" + subscribeTo.metadata());
        }

        AbstractSeedLookupdConfig aSeedLookUpConfig = AbstractSeedLookupdConfig.create(categorization);
        for(String topicKey : firstSeedLookupAddress.keySet()) {
            //skip default cluster seed lookup info
            if(topicKey.startsWith("##_default"))
                continue;
            String controlCnfStr = firstSeedLookupAddress.get(topicKey);
            AbstractControlConfig ctrlCnf = AbstractControlConfig.create(controlCnfStr);
            aSeedLookUpConfig.putTopicCtrlCnf(formatCategorizationTopic(categorization, topicKey), ctrlCnf);
        }

        if(!aSeedLookUpConfig.containsControlConfig()) {
            logger.warn("No control config is picked from updated config, categorization: {}. Null result returns.", categorization);
            return null;
        }
        updateCat2SeedLookupCnfMap(categorization, aSeedLookUpConfig);
        //update categorization 2 topics categorization

        return aSeedLookUpConfig;
    }

    public LookupAddressUpdateHandler createCallbackHandler(String categorization) {
        if(logger.isDebugEnabled())
            logger.debug("call back handler created for Categorization: {}.", categorization);
        return new LookupAddressUpdateHandler(categorization);
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
        if(null != oldCnf)
            oldCnf.clean();
    }

    /**
     * Subscribe topics to configAccess
     * @param category category pass in topics belong to.
     * @param topics topics to subscribe
     */
    public void subscribe(final TopicRuleCategory category, Topic... topics) {
        if(null == topics || topics.length == 0)
            return;
        List<Topic> topics4Subscribe = new ArrayList<>();
        for(final Topic topic : topics) {
            final String categorization = category.category(topic);
            if (checkinCategorization(categorization))
                topics4Subscribe.add(topic);
        }
        final CountDownLatch latch = new CountDownLatch(topics4Subscribe.size());
        for(final Topic topic: topics4Subscribe){
            subscribeExec.submit(new Runnable(){
                public void run() {
                //return a cached value or subscribe new and return cached value
                //try subscribing and check what the returning value is
                String categorization = category.category(topic);
                try {
                    //subscribes categorization to config access and returns
                    subscribe(ConfigAccessAgent.getInstance(), DCCMigrationConfigAccessDomain.getInstance(topic), new AbstractConfigAccessKey[]{DCCMigrationConfigAccessKey.getInstance(category.getRole())}, createCallbackHandler(categorization));
                }catch(NSQConfigAccessException caE){
                    logger.error("Error in checking seed lookup address config for categorization {}, topic {}.", categorization, topic.getTopicText(), caE);
                }catch(Exception exp) {
                    logger.error("Unexpected error in checking seed lookup address config for categorization {}, topic {}.", categorization, topic.getTopicText(), exp);
                    checkoutCategorization(categorization);
                }finally {
                    latch.countDown();
                }
                }
            });
        }

        boolean timeout;
        try {
            timeout = latch.await(NSQConfig.getQueryTimeout4TopicSeedInMillisecond() * topics4Subscribe.size(), TimeUnit.MILLISECONDS);
            if(!timeout) {
                logger.error("Timeout waiting for querying {} topic seed lookup address. Current query timeout for topic seed lookup is {}.", topics.length, NSQConfig.getQueryTimeout4TopicSeedInMillisecond());
                //try checking latch counter to see if there is still some successfully subscribed categorization
                if(latch.getCount() < topics4Subscribe.size()) {
                    logger.warn("Topics subscribe returns partially response.");
                }
            }
        }catch (InterruptedException e) {
            logger.error("Thread interrupted waiting for seed lookup address update.");
        }
    }

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
                            SeedLookupdAddress.listAllLookup();
                        }
                    }, LISTLOOKUP_INIT_DELAY, NSQConfig.getListLookupIntervalInSecond(), TimeUnit.SECONDS);
                    startListLookup = true;
                    logger.info("ListLookup process starts.");
                }
            }
        }
    }

    /**
     * get one lookup address with given {@link Topic} and {@link TopicRuleCategory} (has role info)
     * @param topic topic
     * @param category category to decide role of NSQ client
     * @return {@link NSQLookupdAddress} lookupd address
     */
    public NSQLookupdAddress getLookup(final Topic topic, TopicRuleCategory category, boolean localLookupd) throws NSQLookupException {
        if(localLookupd) {
            AbstractSeedLookupdConfig aSeedLookupCnf = cat2SeedLookupCnfMap.get(TopicRuleCategory.TOPIC_CATEGORIZATION_USER_SPECIFIED);
            if(null == aSeedLookupCnf)
                throw new NSQLookupException("Local Seed Lookup config not found for Topic: " + topic.getTopicText() + " Categorization: " + TopicRuleCategory.TOPIC_CATEGORIZATION_USER_SPECIFIED);
            return aSeedLookupCnf.punchLookupdAddress(TopicRuleCategory.TOPIC_CATEGORIZATION_USER_SPECIFIED, Topic.TOPIC_DEFAULT);
        }

        String categorization = category.category(topic);
        this.subscribe(category, topic);
        AbstractSeedLookupdConfig aSeedLookupCnf =  cat2SeedLookupCnfMap.get(categorization);
        if(null == aSeedLookupCnf)
            throw new NSQLookupException("Seed Lookup config not found for Topic: " + topic.getTopicText() + " Categorization: " + categorization);
        return aSeedLookupCnf.punchLookupdAddress(categorization, topic);
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
        this.listLookupExec.shutdownNow();
        for(AbstractSeedLookupdConfig seedCnf:this.cat2SeedLookupCnfMap.values())
            seedCnf.clean();
        this.cat2SeedLookupCnfMap.clear();
        this.categorizationsInUsed.clear();
        logger.info("LookupAddressUpdate instance closed.");
    }
}
