package com.youzan.nsq.client;

/**
 * Created by lin on 16/10/31.
 */
public interface MessageMetadata {
    /**
     * output message metada in {@link String}
     * @return
     */
    String toMetadataStr();
}
