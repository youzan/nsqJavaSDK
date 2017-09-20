/**
 *
 */
package com.youzan.nsq.client.core;

import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.network.frame.NSQFrame;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Random;
import java.util.Set;

/**
 * NSQ business processing.
 *
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public interface Client extends Closeable {

    Logger logger = LoggerFactory.getLogger(Client.class);

    AttributeKey<Client> STATE = AttributeKey.valueOf("Client.State");
    //ordered attribute not specified in nsq connection for producer.
    AttributeKey<Boolean> ORDERED = AttributeKey.valueOf("Ordered");

    Random _r = new Random(10000);

    //interval of updating topic 2 partition selector
    int _INTERVAL_IN_SECOND = 20;

    void start() throws NSQException;

    /**
     * Receive the frame of NSQ.
     *
     * @param frame NSQFrame to be handled
     * @param conn  NSQConnection
     * @throws NSQException Client code should be catch
     */
    void incoming(final NSQFrame frame, final NSQConnection conn) throws NSQException;

    /**
     * No messages will be sent to the client.
     *
     * @param conn NSQConnection
     */
    void backoff(final NSQConnection conn);

    boolean validateHeartbeat(final NSQConnection conn);

    Set<NSQConnection> clearDataNode(Address address);

    void close(final NSQConnection conn);
}
