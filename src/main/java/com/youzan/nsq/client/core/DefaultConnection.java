package com.youzan.nsq.client.core;

import java.io.IOException;

import com.youzan.nsq.client.core.command.NSQCommand;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;

import io.netty.channel.Channel;

public class DefaultConnection implements Connection {

    private long lastMesaage;
    private Address address;
    private Channel channel;

    @Override
    public void beat() {
        // TODO - implement DefaultConnection.beat
        throw new UnsupportedOperationException();
    }

    /**
     * synchronize
     * 
     * @param cmd
     */
    @Override
    public void sendCommand(NSQCommand cmd) {
        // TODO - implement DefaultConnection.sendCommand
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void connect(NSQConfig config) {
    }

}
