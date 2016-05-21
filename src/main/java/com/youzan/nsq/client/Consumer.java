package com.youzan.nsq.client;

import java.io.Closeable;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public interface Consumer extends Client, Closeable {

    Consumer start();

}
