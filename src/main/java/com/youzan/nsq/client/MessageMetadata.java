package com.youzan.nsq.client;

/**
 * Created by lin on 16/10/31.
 */
public interface MessageMetadata {
    /**
     * output message meta data in {@link String}
     * @return meta data in {@link String}
     */
    String toMetadataStr();
}
