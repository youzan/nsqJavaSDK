package com.youzan.nsq.client.entity;

import java.util.List;

/**
 * Created by lin on 16/12/19.
 */
public class SimplePartitionsSelector implements IPartitionsSelector{
    final private List<Partitions> pras;

    public SimplePartitionsSelector(final List<Partitions> curPras) {
        this.pras = curPras;
    }

    @Override
    public Partitions[] choosePartitions() {
        return this.pras.toArray(new Partitions[0]);
    }

    @Override
    public Partitions[] dumpAllPartitions() {
        return this.pras.toArray(new Partitions[0]);
    }
}
