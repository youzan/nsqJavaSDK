package com.youzan.nsq.client.core;

import com.youzan.nsq.client.network.frame.NSQFrame;

interface ConsumerWorker {
    void incoming(NSQFrame frame);
}
