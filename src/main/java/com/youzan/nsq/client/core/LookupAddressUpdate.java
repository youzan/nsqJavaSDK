package com.youzan.nsq.client.core;

import com.youzan.nsq.client.IConfigAccessSubscriber;
import com.youzan.nsq.client.configs.ConfigAccessAgent;
import com.youzan.nsq.client.entity.NSQConfig;
import org.omg.PortableInterceptor.LOCATION_FORWARD;
import org.omg.PortableInterceptor.ServerRequestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LookupAddressUpdate
 * Created by lin on 16/9/23.
 */
public class LookupAddressUpdate implements IConfigAccessSubscriber{
    private final static Logger logger = LoggerFactory.getLogger(LookupAddressUpdate.class);

    //lookup address
    private volatile List<String> lookupAddresses = null;

    private final ConfigAccessAgent.IConfigAccessCallback lookupAddressUpdateHandler = new ConfigAccessAgent.IConfigAccessCallback<TreeMap<String, String>>() {
        @Override
        public void process(TreeMap newItems) {
           updateLookupAddresses(newItems);
        }

        @Override
        public void fallback(TreeMap itemsInCache, Object... objs) {
            //update lookup address, with cached configs;
            //first param is String[], second is Exception
            updateLookupAddresses(itemsInCache);
        }
    };

    private static final Object LOCK = new Object();
    private static LookupAddressUpdate _INSTANCE = null;

    private static final String NSQ_LOOKUP_KEY_PRO = "nsq.key.lookupd.addr";
    //default value for app value
    private static final String DEFAULT_NSQ_LOOKUP_KEY = "lookupd.addr";

    public LookupAddressUpdate(){
    }

    /**
     * return lookup addresses from config access agent, note that returned lookup addresses could be null
     * if there is no any update from config access remote.
     * @return
     */
    public List<String> getLookupAddresses(){
        return lookupAddresses;
    }

    public static LookupAddressUpdate getInstance(){
        if(null == _INSTANCE){
            synchronized (LOCK){
                if(null == _INSTANCE){
                    //init domain and keys
                    _INSTANCE = new LookupAddressUpdate();
                    //subscribe lookup config request to ConfigAccessAgent
                    try {
                        //subscribe to config access agent
                        _INSTANCE.subscribe(ConfigAccessAgent.getInstance());
                    } catch (Exception e) {
                        logger.error("Fail to initialize LookupAddressUpdate.");
                        _INSTANCE = null;
                        throw e;
                    }
                }
            }
        }
        return _INSTANCE;
    }

    @Override
    public String getDomain() {
        String domain = ConfigAccessAgent.getProperty(NSQ_APP_VAL);
        return null == domain ? DEFAULT_NSQ_APP_VAL : domain;
    }

    @Override
    public String[] getKeys() {
        String[] keys = new String[1];
        String key = ConfigAccessAgent.getProperty(NSQ_LOOKUP_KEY_PRO);
        keys[0] = null == key ? DEFAULT_NSQ_LOOKUP_KEY : key;
        return keys;
    }

    @Override
    public ConfigAccessAgent.IConfigAccessCallback getCallback() {
        return this.lookupAddressUpdateHandler;
    }

    @Override
    public void subscribe(ConfigAccessAgent subscribeTo) {
        logger.info("LookupAddressUpdate Instance subscribe to {}.", subscribeTo);
        SortedMap<String, String> firstLookupAddress = subscribeTo.handleSubscribe(getDomain(), getKeys(), getCallback());
        if(null == firstLookupAddress || firstLookupAddress.size() == 0)
            logger.info("Subscribe to {} returns no result.", subscribeTo);
        updateLookupAddresses(firstLookupAddress);
    }

    private void updateLookupAddresses(final SortedMap<String, String> newLookupAddress){
        if(null == newLookupAddress || newLookupAddress.size() == 0) {
            return;
        }
        //sort
        lookupAddresses = Arrays.asList(newLookupAddress.values().toArray(new String[0]));
    }
}
