package com.youzan.nsq.client;

import java.io.Closeable;

/**
 * It have a exposure to the client. <br />
 * We use one connection to Because of the synchronized protocol and avoid the
 * complex handler when errors occur.
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public interface Consumer extends Client, Closeable {

    Consumer start();

}
