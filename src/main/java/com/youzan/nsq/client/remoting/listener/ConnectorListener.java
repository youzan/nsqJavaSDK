package com.youzan.nsq.client.remoting.listener;

import java.util.EventListener;

/**
 * Created by pepper on 14/12/15.
 */
public interface ConnectorListener extends EventListener {

    public void handleEvent(NSQEvent event) throws Exception;

}
