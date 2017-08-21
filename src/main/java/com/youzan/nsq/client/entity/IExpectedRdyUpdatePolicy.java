package com.youzan.nsq.client.entity;

/**
 * Created by lin on 17/8/15.
 */
public interface IExpectedRdyUpdatePolicy {
    /**
     * calculate new current rdy after INCREASING, given current rdy and expected rdy
     * @param currentRdy    current rdy in connection
     * @param expectedRdy   expected connection rdy
     * @return  new current rdy
     */
    int expectedRdyIncrease(int currentRdy, int expectedRdy);

    /**
     * calculate new expected rdy after DECLINING, given current rdy and expected rdy
     * @param currentRdy    current rdy in connection
     * @param expectedRdy   expected connection rdy
     * @return  new current rdy after decline
     */
    int expectedRdyDecline(int currentRdy, int expectedRdy);
}
