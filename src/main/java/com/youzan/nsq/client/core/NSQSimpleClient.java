/**
 * 
 */
package com.youzan.nsq.client.core;

import java.util.SortedSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.core.command.Nop;
import com.youzan.nsq.client.core.command.Rdy;
import com.youzan.nsq.client.core.lookup.LookupService;
import com.youzan.nsq.client.core.lookup.LookupServiceImpl;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.Response;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.exception.NSQLookupException;
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
    private final LookupService lookup;
    private volatile LookupService migratingLookup = null;
    /**
     * NSQd Servers
     */
    private final ConcurrentSortedSet<Address> dataNodes = new ConcurrentSortedSet<>();
    private final ScheduledExecutorService scheduler = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory(this.getClass().getName(), Thread.MAX_PRIORITY));

    public NSQSimpleClient(final String lookupAddresses, final String topic) {
        this.topic = topic;
        this.lookup = new LookupServiceImpl(lookupAddresses);
        keepDataNodes();
    }

    @Override
    public void start() {
        try {
            newDataNodes();
        } catch (Exception e) {
            logger.error("Exception", e);
        }
    }

    private void keepDataNodes() {
        final int delay = _r.nextInt(120) + 60; // seconds
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                newDataNodes();
            } catch (Exception e) {
                logger.error("Exception", e);
            }
        }, delay, _INTERVAL_IN_SECOND, TimeUnit.SECONDS);
    }

    private void newDataNodes() throws NSQLookupException {
        final SortedSet<Address> nodes = this.lookup.lookup(this.topic);
        if (nodes != null) {
            this.dataNodes.swap(nodes);
        }
    }

    @Override
    public void incoming(final NSQFrame frame, final NSQConnection conn) throws NSQException {
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
                final ErrorFrame err = (ErrorFrame) frame;
                conn.addErrorFrame(err);
            }
            default: {
                logger.error("Invalid Frame Type. Frame: {}", frame);
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

    @Override
    public void clearDataNode(Address address) {
        if (address == null) {
            return;
        }
        dataNodes.remove(address);
    }

}
