package com.youzan.nsq.client;

import java.io.Closeable;
import java.util.List;

import io.netty.util.AttributeKey;

public interface Producer extends Client, Closeable {

    public static final AttributeKey<Client> CONFIG_STATE = AttributeKey.valueOf("ClientConfig.State");

    Producer start();

    void pub(String topic, byte[] message);

    void pubMulti(String topic, List<byte[]> messages);

}
