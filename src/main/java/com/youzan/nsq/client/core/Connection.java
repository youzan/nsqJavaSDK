package com.youzan.nsq.client.core;

import java.io.Closeable;

import com.youzan.nsq.client.core.command.NSQCommand;

/**
 * This is for  consumer to broker .  One message can  be  transported simultaneously.
 */
public interface Connection extends Closeable {

    void beat();

    /**
     * synchronize
     * @param cmd
     */
    void sendCommand(NSQCommand cmd);

}