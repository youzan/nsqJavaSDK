package com.youzan.nsq.client.entity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by lin on 17/5/4.
 */
public class PerfTune {
    private static final Logger log = LoggerFactory.getLogger(PerfTune.class);
    private static Object INSTANCE_LOCK = new Object();
    private static PerfTune _INSTANCE;

    private Properties props = new Properties();

    private PerfTune() {
        String perfTunePath = System.getProperty("nsq.client.perfTune.path");
        if(null == perfTunePath) {
            log.info("Load performance tuning properties from inner properties");
            perfTunePath = "perf.properties";
        } else {
            log.info("Load performance tuning properties from system property %s", perfTunePath);
        }
        InputStream is = getClass().getClassLoader().getResourceAsStream(perfTunePath);

        try {
            props.load(is);
            log.info("Performance tuning properties loaded.");
        } catch (IOException e) {
            log.error("Fail to initialize performance tune properties.", e);
        }
    }

    private long getnsqConnLimit = -1L;
    public long getNSQConnElapseLimit() {
        if(getnsqConnLimit < 0)
            getnsqConnLimit =  Long.parseLong(props.getProperty("nsq.client.conn.get"));
        return getnsqConnLimit;
    }

    private long sendmsgLimit = -1L;
    public long getSendMSGLimit() {
        if(sendmsgLimit < 0)
            sendmsgLimit =  Long.parseLong(props.getProperty("nsq.client.msg.send"));
        return sendmsgLimit;
    }

    private long returnnsqConnLimit = -1L;
    public long getNSQConnReturnLimit() {
        if(returnnsqConnLimit < 0)
            returnnsqConnLimit =  Long.parseLong(props.getProperty("nsq.client.conn.return"));
        return returnnsqConnLimit;
    }

    private long borrownsqConnLimit = -1L;
    public long getNSQConnBorrowLimit() {
        if(borrownsqConnLimit < 0)
            borrownsqConnLimit = Long.parseLong(props.getProperty("nsq.client.pool.conn.borrow"));
        return borrownsqConnLimit;
    }

    private void preload() {
        this.getNSQConnBorrowLimit();
        this.getNSQConnReturnLimit();
        this.getNSQConnElapseLimit();
        this.getSendMSGLimit();
    }

    public static PerfTune getInstance() {
        if(null == _INSTANCE) {
            synchronized (INSTANCE_LOCK) {
                if(null == _INSTANCE) {
                    _INSTANCE = new PerfTune();
                    _INSTANCE.preload();
                }
            }
        }
        return _INSTANCE;
    }
}
