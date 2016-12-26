package com.youzan.nsq.client.configs;

/**
 * Created by lin on 16/12/9.
 */
public abstract class AbstractConfigAccessKey<T> {
    protected T innerKey;

    public AbstractConfigAccessKey(T key) {
        this.innerKey = key;
    }

    public T getInnerKey(){
        return innerKey;
    }

    abstract public String toKey();
}
