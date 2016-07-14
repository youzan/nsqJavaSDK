/**
 * 
 */
package com.youzan.nsq.client.core;

import java.io.Closeable;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
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

import io.netty.channel.ChannelFuture;

/**
 * The intersection between {@code Producer} and {@code Consumer}.
 * 
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public class NSQSimpleClient implements Client, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(NSQSimpleClient.class);

    private final ConcurrentHashMap<String, ConcurrentSortedSet<Address>> topic_2_dataNodes = new ConcurrentHashMap<>();
    private final LookupService lookup;
    private volatile LookupService migratingLookup = null;

    private final ScheduledExecutorService scheduler = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory(this.getClass().getName(), Thread.MAX_PRIORITY));

    public NSQSimpleClient(final String lookupAddresses) {
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
        final int delay = _r.nextInt(60) + 45; // seconds
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    newDataNodes();
                } catch (Exception e) {
                    logger.error("Exception", e);
                }
            }
        }, delay, _INTERVAL_IN_SECOND, TimeUnit.SECONDS);
    }

    private void newDataNodes() throws NSQLookupException {
        final Set<String> topics = this.lookup.getAllTopics();
        if (topics == null) {
            throw new NSQLookupException("I cann't get all the topics.");
        }

        for (String topic : topics) {
            final SortedSet<Address> nodes = this.lookup.lookup(topic);
            if (nodes != null) {
                if (this.topic_2_dataNodes.containsKey(topic)) {
                    this.topic_2_dataNodes.get(topic).swap(nodes);
                } else {
                    final ConcurrentSortedSet<Address> target = new ConcurrentSortedSet<Address>();
                    this.topic_2_dataNodes.putIfAbsent(topic, target);
                }
                logger.debug("Now get the current topic: {} , and new data-nodes(NSQd) are {}", topic, nodes);
            }
        }
        topics.clear();
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
                try {
                    conn.addErrorFrame(err);
                } catch (Exception e) {
                    logger.error("Address: {}, Exception: {}", conn.getAddress(), e);
                }
            }
            default: {
                logger.error("Invalid frame-type from {} , frame: {}", conn.getAddress(), frame);
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
    public ConcurrentSortedSet<Address> getDataNodes(String topic) {
        return topic_2_dataNodes.get(topic);
    }

    @Override
    public void clearDataNode(Address address) {
        if (address == null) {
            return;
        }

        if (topic_2_dataNodes.containsKey(address)) {
            topic_2_dataNodes.remove(address);
            final ConcurrentSortedSet<Address> nodes = topic_2_dataNodes.get(address);
            nodes.clear();
        }
    }

    @Override
    public boolean validateHeartbeat(NSQConnection conn) {
        final ChannelFuture future = conn.command(Nop.getInstance());
        if (future.awaitUninterruptibly(500, TimeUnit.MILLISECONDS)) {
            return future.isSuccess();
        }
        return false;
    }

    public void close() {
        topic_2_dataNodes.clear();
        scheduler.shutdownNow();
    }

    void sleep(final long millisecond) {
        logger.debug("Sleep {} millisecond.", millisecond);
        try {
            Thread.sleep(millisecond);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Your machine is too busy! Please check it!");
        }
    }
}
