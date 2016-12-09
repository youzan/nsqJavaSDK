package com.youzan.nsq.client.core;

import com.youzan.nsq.client.core.command.Rdy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by lin on 16/12/8.
 */
public class RdySpectrum {
    private static final Logger logger = LoggerFactory.getLogger(RdySpectrum.class);

    public static int increase(final NSQConnection conn, int currentRdy, int targetRdy) {
       if(targetRdy > currentRdy && currentRdy > 0) {
           conn.command(new Rdy(++currentRdy));
           logger.info("Rdy increase 1 to {}, for connection to {}", currentRdy, conn.getAddress().toString());
       }
       return currentRdy;
    }

    public static int decrease(final NSQConnection conn, int currentRdy, int targetRdy) {
        if(currentRdy > targetRdy && targetRdy > 0) {
            conn.command(new Rdy(--currentRdy));
            logger.info("Rdy decrease 1 to {}, for connection to {}", currentRdy, conn.getAddress().toString());
        }
        return currentRdy;
    }
}
