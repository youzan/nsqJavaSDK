package com.youzan.nsq.client.entity;

/**
 * Created by lin on 17/5/25.
 */
public class NoExtContent implements IExtContent {
    @Override
    public ExtVer version() {
        return ExtVer.Ver0x0;
    }
}
