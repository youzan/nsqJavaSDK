package com.youzan.nsq.client.entity;

/**
 * Created by lin on 17/7/31.
 */
public class DefaultRdyUpdatePolicy implements IRdyUpdatePolicy {
    public final static float THRESDHOLD = 1.5f;
    public final static float WATER_HIGH = 1.75f;

    @Override
    public boolean rdyShouldIncrease(String topic, float scheduleLoad, boolean mayTimeout, int maxRdyPerCon, int extraRdy) {
        return !mayTimeout && scheduleLoad <= THRESDHOLD;
    }

    @Override
    public boolean rdyShouldDecline(String topic, float scheduleLoad, boolean mayTimeout, int maxRdyPerCon, int extraRdy) {
        return scheduleLoad >= WATER_HIGH && mayTimeout;
    }
}
