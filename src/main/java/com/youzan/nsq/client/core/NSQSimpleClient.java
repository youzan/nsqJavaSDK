/**
 * 
 */
package com.youzan.nsq.client.core;

import java.util.Random;
import java.util.SortedSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.core.command.Nop;
import com.youzan.nsq.client.core.command.Rdy;
import com.youzan.nsq.client.core.lookup.NSQLookupService;
import com.youzan.nsq.client.core.lookup.NSQLookupServiceImpl;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.Response;
import com.youzan.nsq.client.network.frame.ErrorFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.nsq.client.network.frame.ResponseFrame;
import com.youzan.util.ConcurrentSortedSet;
import com.youzan.util.NamedThreadFactory;

/**
 * The intersection between {@code Producer} and {@code Consumer}.
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class NSQSimpleClient implements Client {
    private static final Logger logger = LoggerFactory.getLogger(NSQSimpleClient.class);

    private final String topic;;
    private final NSQLookupService lookup;
    private volatile NSQLookupService migratingLookup = null;
    /**
     * NSQd Servers
     */
    private final ConcurrentSortedSet<Address> dataNodes = new ConcurrentSortedSet<>();
    private final ScheduledExecutorService scheduler = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory(this.getClass().getName(), Thread.MAX_PRIORITY));

    public NSQSimpleClient(final String lookupAddresses, final String topic) {
        this.topic = topic;
        this.lookup = new NSQLookupServiceImpl(lookupAddresses);
        keepDataNodes();
    }

    public void start() {
        newDataNodes();
    }

    private void newDataNodes() {
        final int retries = 2;
        int c = 0;
        while (c++ < retries) {
            try {
                final SortedSet<Address> nodes = this.lookup.lookup(this.topic);
                if (nodes != null && !nodes.isEmpty()) {
                    this.dataNodes.swap(nodes);
                    break;
                }
            } catch (Exception e) {
                logger.error("Exception", e);
            }
            sleep(1000 * c);
        }
    }

    /**
     * 
     */
    private void keepDataNodes() {
        final Random random = new Random(10000);
        final int delay = random.nextInt(120) + 120; // seconds
        scheduler.scheduleWithFixedDelay(() -> {
            newDataNodes();
        }, delay, 1 * 60, TimeUnit.SECONDS);
    }

    @Override
    public void incoming(final NSQFrame frame, final NSQConnection conn) {
        switch (frame.getType()) {
            case RESPONSE_FRAME: {
                final String resp = frame.getMessage();
                if (Response._HEARTBEAT_.getContent().equals(resp)) {
                    conn.command(Nop.getInstance());
                    return;
                } else {
                    conn.addResponseFrame((ResponseFrame) frame);
                }
                break;
            }
            case ERROR_FRAME: {
                // TODO Error Callback?
                conn.addErrorFrame((ErrorFrame) frame);
                break;
            }
            default: {
                logger.error("Invalid Frame Type.");
                break;
            }
        }
        return;
    }

    @Override
    public void backoff(NSQConnection conn) {
        conn.command(new Rdy(0));
    }

    @Override
    public ConcurrentSortedSet<Address> getDataNodes() {
        return dataNodes;
    }

    /**
     * @param millisecond
     */
    private void sleep(final int millisecond) {
        try {
            Thread.sleep(millisecond);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("System is too busy! Please check it!", e);
        }
    }

}
