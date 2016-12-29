package com.youzan.nsq.client.configs;

/**
 * Abstract class of config access domain.
 * Created by lin on 16/12/9.
 */
public abstract class AbstractConfigAccessDomain<T> {
    T innerDomain;

    public AbstractConfigAccessDomain(T domain) {
        this.innerDomain = domain;
    }

    public T getInnerDomain() {
        return this.innerDomain;
    }

    abstract public String toDomain();
}
