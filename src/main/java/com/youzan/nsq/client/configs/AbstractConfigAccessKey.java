package com.youzan.nsq.client.configs;

/**
 * Abstract class for config access key.
 * Created by lin on 16/12/9.
 */
public abstract class AbstractConfigAccessKey<T> {
    T innerKey;

    public AbstractConfigAccessKey(T key) {
        this.innerKey = key;
    }

    public T getInnerKey(){
        return innerKey;
    }

    abstract public String toKey();
}
