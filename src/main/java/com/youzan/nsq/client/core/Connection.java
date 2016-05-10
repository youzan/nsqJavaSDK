package com.youzan.nsq.client.core;

import java.io.Closeable;

import com.youzan.nsq.client.core.command.NSQCommand;
import com.youzan.nsq.client.entity.NSQConfig;

/**
 * This is for  consumer to broker .  One message can  be  transported
 * simultaneously.
 */
interface Connection extends Closeable {

    void beat();

    /**
     * synchronize
     * 
     * @param cmd
     */
    void sendCommand(NSQCommand cmd);

    void connect(NSQConfig config);
}
