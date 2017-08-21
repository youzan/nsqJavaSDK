package com.youzan.nsq.client.entity;

/**
 * Created by lin on 17/7/31.
 */
public interface IRdyUpdatePolicy {

    /**
     * indicate whether rdy should increase.
     * @param scheduleLoad  scheduleLoad for {@link com.youzan.nsq.client.Consumer} which has current policy applied.
     *                      scheduleLoad = (float)(message for processing in queue) / (message being processing)
     *
     * @param mayTimeout    indicate whether message processing may timeout, given the message waiting for process
     *                      in queue and the average process time elapse:
     *                      (message consumption rate) * (message # in queue) >= NSQConfig.getMsgTiemoutInMilliSe
     * @param maxRdyPerCon  max rdy per connection allowed
     *
     * @param extraRdy      extra Rdy could be allocated for current topic's connections
     *
     * @return true indicates rdy should increase, otherwise false
     */
    boolean rdyShouldIncrease(String topic, float scheduleLoad, boolean mayTimeout, int maxRdyPerCon, int extraRdy);

    /**
     * indicate whether rdy should increase.
     * @param scheduleLoad  scheduleLoad for {@link com.youzan.nsq.client.Consumer} which has current policy applied.
     *                      scheduleLoad = (float)(message for processing in queue) / (message being processing)
     *
     * @param mayTimeout    indicate whether message processing may timeout, given the message waiting for process
     *                      in queue and the average process time elapse:
     *                      (message consumption rate) * (message # in queue) >= NSQConfig#getMsgTiemoutInMilliSe
     * @param maxRdyPerCon  max rdy per connection allowed
     *
     * @param extraRdy      extra Rdy could be allocated for current topic's connections
     *
     * @return true indicates rdy should decline, otherwise false
     */
    boolean rdyShouldDecline(String topic, float scheduleLoad, boolean mayTimeout, int maxRdyPerCon, int extraRdy);
}
