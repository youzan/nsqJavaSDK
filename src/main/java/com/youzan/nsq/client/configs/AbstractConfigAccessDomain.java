package com.youzan.nsq.client.configs;

/**
 * Created by lin on 16/12/9.
 */
public abstract class AbstractConfigAccessDomain<T> {
    protected T innerDomain;

    public AbstractConfigAccessDomain(T domain) {
        this.innerDomain = domain;
    }

    public T getInnerDomain() {
        return this.innerDomain;
    }

    abstract public String toDomain();
}
