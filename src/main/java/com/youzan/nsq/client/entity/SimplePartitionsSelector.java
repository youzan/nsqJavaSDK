package com.youzan.nsq.client.entity;

/**
 * Created by lin on 16/12/19.
 */
public class SimplePartitionsSelector implements IPartitionsSelector{
    final private Partitions pras;

    public SimplePartitionsSelector(final Partitions curPras) {
        this.pras = curPras;
    }

    @Override
    public Partitions choosePartitions() {
        return this.pras;
    }

    @Override
    public Partitions[] dumpAllPartitions() {
        return new Partitions[]{this.pras};
    }
}
