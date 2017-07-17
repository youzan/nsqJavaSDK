package com.youzan.nsq.client;

/**
 * Created by lin on 17/6/29.
 */
public interface IConsumeInfo {
    /**
     * load factor of consumer, (total size in queue)/(active messages in queue)
     * @return load factor
     */
    float getLoadFactor();

    /**
     * rdy count per connection
     * @return rdy count per connection in config
     */
    int getRdyPerConnection();

    boolean isConsumptionEstimateElapseTimeout();
}
