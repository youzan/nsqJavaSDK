/**
 * 
 */
package com.youzan.nsq.client;

import com.youzan.nsq.client.entity.NSQConfig;

import io.netty.util.AttributeKey;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public interface Client {
    public static final AttributeKey<Client> STATE = AttributeKey.valueOf("ClientConfig.State");

    NSQConfig getConfig();
}
