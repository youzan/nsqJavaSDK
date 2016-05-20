package com.youzan.nsq.client;

import java.io.Closeable;
import java.util.List;

public interface Producer extends Client, Closeable {

    Producer start();

    void publish(String topic, byte[] message);

    void publishMulti(String topic, List<byte[]> messages);

}
