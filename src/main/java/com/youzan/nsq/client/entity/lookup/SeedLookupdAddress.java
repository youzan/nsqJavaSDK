package com.youzan.nsq.client.entity.lookup;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Sets;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.util.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * SeedLookupdAddress
 * Created by lin on 16/12/5.
 */
public class SeedLookupdAddress extends AbstractLookupdAddress {
    private static ConcurrentHashMap<String, SeedLookupdAddress> seedLookupMap = new ConcurrentHashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(SeedLookupdAddress.class);

    private static final String HTTP_PRO_HEAD = "http://";
    private static Map<String, Long> LISTLOOKUP_LASTUPDATED = new ConcurrentHashMap<>();
    private volatile int INDEX = 0;

    private String clusterId;
    private AtomicLong refCounter = new AtomicLong(0);
    //lookupAddresses fetched from listlookup API
    private ReentrantReadWriteLock lookupAddressLock = new ReentrantReadWriteLock();
    private List<LookupdAddress> lookupAddressesRefs;
    private Set<String> lookupAddressInUse;

    protected SeedLookupdAddress(String address) {
        super(address, address);
        //TODO: do we really need cluster info
        this.clusterId = address;
        lookupAddressesRefs = new ArrayList<>();
        lookupAddressInUse = new HashSet<>();
    }

    public String getClusterId() {
        return this.clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    /**
     * Fetch lookup addresses of current {@link SeedLookupdAddress}. Update references to lookupaddresses
     */
    private void listLookup(boolean force) throws IOException {
        int lookupAddrRefSize = 0;
        if(!force) {
            try {
                this.lookupAddressLock.readLock().lock();
                lookupAddrRefSize = this.lookupAddressesRefs.size();
                if (lookupAddrRefSize > 0)
                    return;
                else {
                    logger.info("lookup address ref not exist, force listlookup needed");
                }
            } finally {
                this.lookupAddressLock.readLock().unlock();
            }
        }

        String seed = this.getAddress();
        long current_inMills = System.currentTimeMillis();
        if(!force && lookupAddrRefSize > 0) {
            long lastUpdated_inMills = LISTLOOKUP_LASTUPDATED.get(seed);
            if (current_inMills - lastUpdated_inMills < NSQConfig.getListLookupIntervalInSecond() * 1000) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Seed lookupd address: {} has list lookup in last {} sec.", seed, NSQConfig.getListLookupIntervalInSecond());
                }
                return;
            }
        }

        if (!seed.startsWith(HTTP_PRO_HEAD))
            seed = HTTP_PRO_HEAD + seed;
        final String url = String.format("%s/listlookup", seed);
        if (logger.isDebugEnabled())
            logger.debug("Begin to get the new lookup servers. From URL: {}", url);
        JsonNode tmpRootNode = null;
        final Set<String> lookups = new HashSet<>();
        URL lookupUrl;
        try {
            lookupUrl = new URL(url);
            tmpRootNode = IOUtil.readFromUrl(lookupUrl);
            final JsonNode nodes = tmpRootNode.get("lookupdnodes");
            if (null == nodes) {
                logger.error("NSQ listlookup responses without any lookup servers for seed lookup address: {}.", seed);
                return;
            }
            for (JsonNode node : nodes) {
                final String host = node.get("NodeIP").asText();
                final int port = node.get("HttpPort").asInt();
                final String address = host + ":" + port;
                lookups.add(address);
            }
        } catch (ConnectException ce) {
            _handleConnectionTimeout(seed, ce);
        } catch (FileNotFoundException e) {
            //add seed lookup directlly as lookup address
            //TODO: choose a clusterId
            lookups.add(seed);
            logger.info("You run with lower server version, current seed lookup address will be added directly as lookup address");
        }

        this.updateLookupdAddresses(lookups);

        //update last update timestamp
        if(LISTLOOKUP_LASTUPDATED.containsKey(seed))
            LISTLOOKUP_LASTUPDATED.put(seed, current_inMills);
        if (logger.isDebugEnabled())
            logger.debug("Recently have got the lookup servers: {} from seed lookup: {}", lookups, seed);
    }

    private void _handleConnectionTimeout(String lookup, ConnectException ce) throws IOException {
        String ip = "EMPTY", address = "EMPTY";
        try {
            InetAddress addr = InetAddress.getLocalHost();
            ip = addr.getHostAddress();//ip where sdk resides
            address = addr.getHostName();//address where sdk resides
        } catch (Exception e) {
            logger.error("Could not fetch ip or address form local client, should not occur.", e);
        }
        logger.warn("Fail to connect to NSQ lookup. SDK Client, ip:{} address:{}. Remote lookup:{}. Will kick off another try in another round.", ip, address, lookup);
        logger.warn("Nested connection exception stacktrace:", ce);
    }

    private long updateRefCounter(long delta) {
        if (delta > 0) {
            return this.refCounter.incrementAndGet();
        } else if (delta < 0) {
            return this.refCounter.decrementAndGet();
        }
        return this.refCounter.get();
    }

    /**
     * add one {@link LookupdAddress} to current {@link SeedLookupdAddress} object, reference to passin lookup address
     * updated in function;
     * @deprecated use {@link SeedLookupdAddress#updateLookupdAddresses(Set)} instead
     * @param lookupd
     */
    @Deprecated
    void addLookupdAddress(final LookupdAddress lookupd) {
        try {
            this.lookupAddressLock.writeLock().lock();
            if (!lookupAddressInUse.contains(lookupd.getAddress())) {
                this.lookupAddressesRefs.add(lookupd);
                lookupd.addReference();
                lookupAddressInUse.add(lookupd.getAddress());
            }
        } finally {
            this.lookupAddressLock.writeLock().unlock();
        }
    }

    /**
     * add passin lookupd addresses string to current {@link SeedLookupdAddress} object.
     * @deprecated use {@link SeedLookupdAddress#updateLookupdAddresses(Set)} instead*
     * @param lookupds
     */
    @Deprecated
    private void addLookupdAddresses(String... lookupds) {
        try {
            lookupAddressLock.writeLock().lock();
            for (String aLookupStr : lookupds) {
                if (!lookupAddressInUse.contains(aLookupStr)) {
                    LookupdAddress aLookup = LookupdAddress.create(this.getClusterId(), aLookupStr);
                    this.lookupAddressesRefs.add(aLookup);
                    aLookup.addReference();
                    lookupAddressInUse.add(aLookupStr);
                }
            }
        } finally {
            lookupAddressLock.writeLock().unlock();
        }
    }

    /**
     * update lookup address found based on current seed lookup address, new lookup address added and retires old ones.
     * @param lookupds
     */
    void updateLookupdAddresses(final Set<String> lookupds) {
        if(null == lookupds || lookupds.size() <= 0) {
            logger.warn("Referenced lookup addresses for update should not be empty, seed lookup address {}", this.getAddress());
            return;
        }
        try {
            lookupAddressLock.writeLock().lock();
            //remove retired lookup address
            Sets.SetView<String> lookupdsNew = Sets.difference(lookupds, this.lookupAddressInUse);
            Sets.SetView<String> lookupdsRetire = Sets.difference(this.lookupAddressInUse, lookupds);
            List<LookupdAddress> lookupdAddressesRemove = new LinkedList<>();
            //retied lookup addresses
            if(null != lookupdsRetire && lookupdsRetire.size() > 0) {
                for (int idx = 0; idx < this.lookupAddressesRefs.size(); idx++) {
                    LookupdAddress lookupdAddr = this.lookupAddressesRefs.get(idx);
                    if (lookupdsRetire.contains(lookupdAddr.getAddress())) {
                        lookupdAddr.removeReference();
                        lookupdAddressesRemove.add(lookupdAddr);
                        this.lookupAddressInUse.remove(lookupdAddr.getAddress());
                    }
                }
                if (lookupdAddressesRemove.size() > 0)
                    this.lookupAddressesRefs.removeAll(lookupdAddressesRemove);
                logger.info("remove {} from seed lookupd clusterId: {} address: {}", lookupdAddressesRemove, this.getClusterId(), this.getAddress());
            }
            //add new lookup addresses
            if(null != lookupdsNew && lookupdsNew.size() > 0) {
                for (String aLookupStr : lookupdsNew) {
                    LookupdAddress aLookup = LookupdAddress.create(this.getClusterId(), aLookupStr);
                    this.lookupAddressesRefs.add(aLookup);
                    aLookup.addReference();
                    lookupAddressInUse.add(aLookupStr);
                }
                logger.info("add {} to seed lookupd clusterId: {} address: {}", lookupdsNew, this.getClusterId(), this.getAddress());
            }
        } finally {
            lookupAddressLock.writeLock().unlock();
        }
    }

    /**
     * remove all {@link LookupdAddress} under current SeedLookup address
     */
    private void removeAllLookupdAddress() {
        try {
            this.lookupAddressLock.writeLock().lock();
            for (LookupdAddress aLookupd : this.lookupAddressesRefs) {
                if (null != aLookupd) {
                    aLookupd.removeReference();
                }
            }
            this.lookupAddressInUse.clear();
        } finally {
            this.lookupAddressLock.writeLock().unlock();
        }

    }

    /**
     * create SeedLookupdAddress for pass in address
     * @param address seed lookupd address
     * @return  {@link SeedLookupdAddress}
     */
    public static SeedLookupdAddress create(String address) {
        SeedLookupdAddress aSeed = new SeedLookupdAddress(address);
        if (!seedLookupMap.containsKey(address)) {
            synchronized (seedLookupMap) {
                if (!seedLookupMap.containsKey(address)) {
                    seedLookupMap.put(address, aSeed);
                    LISTLOOKUP_LASTUPDATED.put(address, 0L);
                }
            }
        } else {
            aSeed = seedLookupMap.get(address);
        }

        return aSeed;
    }

    /**
     * Add one reference count to current {@link SeedLookupdAddress} object
     *
     * @param   aSeed seed lookupd address
     * @return  reference count after addReference
     */
    static long addReference(SeedLookupdAddress aSeed) {
        if (seedLookupMap.containsValue(aSeed)) {
            synchronized (aSeed) {
                if (seedLookupMap.containsValue(aSeed)) {
                    return aSeed.updateRefCounter(1);
                }
            }
        }
        return -1;
    }

    /**
     * Decrease one reference count to current {@link SeedLookupdAddress}, if updated reference count is smaller than 1,
     * current SeedLookupdAddress object will be removed.
     *
     * @param   aSeed seed lookupd address
     * @return  reference after removeReference.
     */
    static long removeReference(SeedLookupdAddress aSeed) {
        if (seedLookupMap.containsValue(aSeed)) {
            synchronized (aSeed) {
                if (seedLookupMap.containsValue(aSeed)) {
                    long cnt = aSeed.updateRefCounter(-1);
                    if (cnt <= 0) {
                        logger.info("remove seed lookup address {} as NO reference from control config.", aSeed.getAddress());
                        seedLookupMap.remove(aSeed.getAddress());
                        LISTLOOKUP_LASTUPDATED.remove(aSeed.getAddress());
                        aSeed.clean();
                        return 0;
                    }
                }
            }
        }
        return -1;
    }

    /**
     * punch out one lookup address, from current seed lookupd address.
     * @param force {@link Boolean#TRUE} to force list lookup addresses, otherwise {@link Boolean#FALSE}
     * @return lookupd address
     */
    public String punchLookupdAddressStr(boolean force) {
        try {
            this.listLookup(force);
        } catch (IOException e) {
            logger.error("Fail to get lookup address for seed lookup address: {}. Start to punch one cached lookupd address, if there is any.", this.getAddress());
        }

        LookupdAddress lookupdAddress = null;
        try {
            this.lookupAddressLock.readLock().lock();
            if (this.lookupAddressesRefs.size() > 0)
                lookupdAddress = this.lookupAddressesRefs.get((INDEX++ & Integer.MAX_VALUE) % this.lookupAddressesRefs.size());
        } finally {
            this.lookupAddressLock.readLock().unlock();
        }
        if (null != lookupdAddress)
            return lookupdAddress.getAddress();
        return null;
    }

    long getReferenceCount() {
        return this.refCounter.get();
    }

    public static int seedLookupMapSize() {
        return seedLookupMap.size();
    }

    void clean() {
        this.removeAllLookupdAddress();
    }

    private static AtomicInteger LIST_ALL_ERR_CNT = new AtomicInteger(0);
    private static final int THRESHOLD_LIST_LOOKUP_CNT = 3;

    public static int listAllLookup() {
        long start = 0L;
        int success = 0;
        if(logger.isDebugEnabled()) {
            start = System.currentTimeMillis();
        }
        for (SeedLookupdAddress aSeed : seedLookupMap.values()) {
            try {
                aSeed.listLookup(true);
                success ++;
                LIST_ALL_ERR_CNT.set(0);
            } catch (IOException e) {
                int errCnt = LIST_ALL_ERR_CNT.addAndGet(1);
                if(errCnt > THRESHOLD_LIST_LOOKUP_CNT) {
                    logger.error("Fail to get lookup address for seed lookup address in continuous {} times. Lookupd address: {}.", errCnt, aSeed.getAddress());
                } else {
                    logger.warn("Fail to get lookup address for seed lookup address: {}.", aSeed.getAddress());
                }
            }
        }

        if(logger.isDebugEnabled()) {
            logger.debug("Time elapse: {} millsec for all seed lookup addresses to listLookup, size {}", System.currentTimeMillis() - start, seedLookupMap.size());
        }

        return success;
    }

    public static int getListLookupErrCnt() {
        return LIST_ALL_ERR_CNT.get();
    }

}
