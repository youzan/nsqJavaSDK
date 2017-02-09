package com.youzan.nsq.client.entity;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by lin on 16/12/19.
 */
public class MigrationPartitionsSelector implements IPartitionsSelector {
    private static final Logger logger = LoggerFactory.getLogger(MigrationPartitionsSelector.class);
    final private static Random _ran = new Random();
    final private List<Partitions> prePars;
    final private AtomicLong preCnt = new AtomicLong(1);

    final private List<Partitions> curPars;
    final private AtomicLong curCnt = new AtomicLong(1);
    final private double preFactor;

    public MigrationPartitionsSelector(final List<Partitions> prePars, final List<Partitions> curPars, double preFactor) {
        this.prePars = prePars;
        this.curPars = curPars;
        this.preFactor = preFactor;
    }

    @Override
    public Partitions[] choosePartitions() {
        double seed = _ran.nextFloat() * 100;
        if (seed < this.preFactor) {
            if(logger.isDebugEnabled())
                logger.debug("Previous partitions chosen.");
            this.preCnt.incrementAndGet();
            return this.prePars.toArray(new Partitions[0]);
        } else {
            if(logger.isDebugEnabled())
                logger.debug("Current partitions chosen.");
            this.curCnt.incrementAndGet();
            return this.curPars.toArray(new Partitions[0]);
        }
    }

    @Override
    public Partitions[] dumpAllPartitions() {
        Partitions[] returnPars = new Partitions[this.curPars.size() + this.prePars.size()];
        Partitions[] curParsArr = this.curPars.toArray(new Partitions[0]);
        Partitions[] preParsArr = this.prePars.toArray(new Partitions[0]);
        System.arraycopy(curParsArr, 0, returnPars, 0, this.curPars.size());
        System.arraycopy(preParsArr, 0, returnPars, this.curPars.size(), this.prePars.size());
        return returnPars;
    }
}
