package com.youzan.nsq.client.core;

/**
 * Created by lin on 17/6/27.
 */
public interface IRdyCallback {
    void onUpdated(int newRdy, int lastRdy);
}
