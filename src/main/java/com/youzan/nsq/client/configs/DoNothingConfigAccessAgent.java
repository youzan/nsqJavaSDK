package com.youzan.nsq.client.configs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.SortedMap;

/**
 * Created by lin on 16/10/26.
 */
public class DoNothingConfigAccessAgent extends ConfigAccessAgent{
    private static final Logger logger = LoggerFactory.getLogger(DoNothingConfigAccessAgent.class);

    public DoNothingConfigAccessAgent(){
        logger.info("Do nothing config access agent initialize.");
    }

    @Override
    public SortedMap<String, String> handleSubscribe(String domain, String[] keys, IConfigAccessCallback callback) {
        return null;
    }

    @Override
    public void kickoff() {
        logger.info("Do nothing config access agent kicksoff.");
    }

    @Override
    public void close() {
        logger.info("Do nothing config access agent close.");
    }
}
