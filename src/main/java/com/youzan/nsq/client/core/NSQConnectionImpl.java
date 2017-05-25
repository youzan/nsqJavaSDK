package com.youzan.nsq.client.core;

import com.youzan.nsq.client.core.command.Identify;
import com.youzan.nsq.client.core.command.Magic;
import com.youzan.nsq.client.core.command.NSQCommand;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.network.frame.ErrorFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.nsq.client.network.frame.ResponseFrame;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public class NSQConnectionImpl implements Serializable, NSQConnection, Comparable {
    private static final Logger logger = LoggerFactory.getLogger(NSQConnectionImpl.class);
    private static final long serialVersionUID = 7139923487863469738L;

    private final Object lock = new Object();
    private final int id; // primary key
    private final long queryTimeoutInMillisecond;

    private boolean started = false;
    private boolean closing = false;
    private boolean havingNegotiation = false;

    private final LinkedBlockingQueue<NSQCommand> requests = new LinkedBlockingQueue<>(1);
    private final LinkedBlockingQueue<NSQFrame> responses = new LinkedBlockingQueue<>(1);

    private final Address address;
    private final Channel channel;
    //topic for subscribe
    private Topic topic;
    private final NSQConfig config;

    //indicate if current should be extendable, if it is true, message received from nsqd should be extended.
    private final boolean isExtend;

    //start ready cnt for current count
    private volatile int currentRdy;

    private final AtomicLong latestInternalID = new AtomicLong(-1L);
    private final AtomicLong latestDiskQueueOffset = new AtomicLong(-1L);

    public NSQConnectionImpl(int id, Address address, Channel channel, NSQConfig config) {
        this.id = id;
        this.address = address;
        this.channel = channel;
        this.config = config;
        if(!this.config.isConsumerSlowStart())
            this.currentRdy = this.config.getRdy();
        else
            this.currentRdy = 1;
        this.queryTimeoutInMillisecond = config.getQueryTimeoutInMillisecond();
        if(address.isTopicExtend()) {
            isExtend = Boolean.TRUE;
        } else {
            isExtend = Boolean.FALSE;
        }
        if(logger.isDebugEnabled())
            logger.debug("extend marked as {} for connection to {}", this.isExtend, address);
    }

    @Override
    public boolean isExtend() {
        return this.isExtend;
    }

    @Override
    public boolean checkOrder(long internalID, long diskQueueOffset, final NSQMessage msg){
        if(!this.config.isOrdered())
            return true;
        if(internalID >= this.latestInternalID.get() && diskQueueOffset >= this.latestDiskQueueOffset.get()){
            this.latestInternalID.set(internalID);
            this.latestDiskQueueOffset.set(diskQueueOffset);
            return true;
        }else {
            logger.warn("InternalID or diskQueueOffset is(are) NOT latest in current connection.\n" +
                    "InternalID:{}, latestInternalID:{}. diskQueueOffset:{}, latestQueueOffset:{}.\n" +
                    "Message: {}.", internalID, diskQueueOffset, this.latestInternalID.get(), this.latestDiskQueueOffset.get(), msg.toMetadataStr());
            return false;
        }
    }

    /**
     * initialize NSQConnection to NSQd by sending Identify Command
     */
    @Override
    public void init() throws TimeoutException {
        assert address != null;
        assert config != null;
        synchronized (lock) {
            if (!havingNegotiation) {
                command(Magic.getInstance());
                final NSQCommand identify = new Identify(config);
                final NSQFrame response = commandAndGetResponse(identify);
                if (null == response) {
                    throw new IllegalStateException("Bad Identify Response! Close connection!");
                }
                // TODO judge OK?
                havingNegotiation = true;
            }
            started = true;
        }
        assert channel.isActive();
        assert isConnected();
        logger.debug("Having initiated {}", this);
    }

    @Override
    public void init(final Topic topic) throws TimeoutException {
        this.init();
        setTopic(topic);
    }

    @Override
    public ChannelFuture command(NSQCommand cmd) {
        if (cmd == null) {
            return null;
        }

        // Use Netty Pipeline
        return channel.writeAndFlush(cmd);
    }

    @Override
    public NSQFrame commandAndGetResponse(final NSQCommand command) throws TimeoutException {
        if (!channel.isActive()) {
            if (!closing) {
                throw new TimeoutException("The channel " + channel + " is closed. This is not closing.");
            } else {
                throw new TimeoutException("The channel " + channel + " is closed. This is closing.");
            }
        }
        final long start = System.currentTimeMillis();
        try {
            long timeout = queryTimeoutInMillisecond - (System.currentTimeMillis() - start);
            if (!requests.offer(command, timeout, TimeUnit.MILLISECONDS)) {
                throw new TimeoutException(
                        "The command is timeout. The command name is : " + command.getClass().getName());
            }

            responses.clear(); // clear
            // write data
            final ChannelFuture future = command(command);

            // wait to get the response
            timeout = queryTimeoutInMillisecond - (System.currentTimeMillis() - start);
            if (!future.await(timeout, TimeUnit.MILLISECONDS)) {
                throw new TimeoutException(
                        "The command is timeout. The command name is : " + command.getClass().getName());
            }
            timeout = queryTimeoutInMillisecond - (System.currentTimeMillis() - start);
            final NSQFrame frame = responses.poll(timeout, TimeUnit.MILLISECONDS);
            if (frame == null) {
                throw new TimeoutException(
                        "The command is timeout. The command name is : " + command.getClass().getName());
            }

            requests.poll(); // clear
            return frame;
        } catch (InterruptedException e) {
            close();
            Thread.currentThread().interrupt();
            logger.error("Thread was interrupted, probably shutting down! Close connection!", e);
        }
        return null;
    }

    @Override
    public void addResponseFrame(ResponseFrame frame) {
        if (!requests.isEmpty()) {
            try {
                responses.offer(frame, queryTimeoutInMillisecond * 2, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                close();
                Thread.currentThread().interrupt();
                logger.error("Thread was interrupted, probably shutting down!", e);
            }
        } else {
            logger.error("No request to send, but get a frame from the server.");
        }
    }

    @Override
    public void addErrorFrame(ErrorFrame frame) {
        try {
            responses.offer(frame, queryTimeoutInMillisecond, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Thread was interrupted, probably shutting down!", e);
        }
    }

    @Override
    public void setTopic(final Topic topic) {
        this.topic = topic;
    }

    @Override
    public Topic getTopic() {
        return this.topic;
    }

    @Override
    public boolean isConnected() {
        synchronized (lock) {
            return channel.isActive() && havingNegotiation;
        }
    }

    @Override
    public void close() {
        logger.info("Begin to close {}", this);
        synchronized (lock) {
            if (closing) {
                return;
            }
            closing = true;
            if (null != channel) {
                // It is very important!
                havingNegotiation = false;
                channel.attr(NSQConnection.STATE).remove();
                channel.attr(Client.STATE).remove();
                if(channel.hasAttr(Client.ORDERED))
                    channel.attr(Client.ORDERED).remove();
                if (channel.isActive()) {
                    channel.close();
                    channel.deregister();
                }
                if (!channel.isActive()) {
                    logger.info("Having closed {} OK!", this);
                }
            } else {
                logger.error("No channel has be set...");
            }
        }
    }

    public void setCurrentRdyCount(int newCount) {
        if(newCount <= 0 || this.currentRdy == newCount)
            return;
        this.currentRdy = newCount;
    }

    public int getCurrentRdyCount() {
        return this.currentRdy;
    }
    /**
     * @return the id , the primary key of the object
     */
    @Override
    public int getId() {
        return id;
    }

    /**
     * @return the address
     */
    @Override
    public Address getAddress() {
        return address;
    }

    @Override
    public NSQConfig getConfig() {
        return config;
    }


    @Override
    public int compareTo(Object o) {
        return getId() - ((NSQConnectionImpl) o).getId();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NSQConnectionImpl that = (NSQConnectionImpl) o;

        if (id != that.id) return false;
        if (queryTimeoutInMillisecond != that.queryTimeoutInMillisecond) return false;
        if (started != that.started) return false;
        if (closing != that.closing) return false;
        if (havingNegotiation != that.havingNegotiation) return false;
        if (lock != null ? !lock.equals(that.lock) : that.lock != null) return false;
        if (requests != null ? !requests.equals(that.requests) : that.requests != null) return false;
        if (responses != null ? !responses.equals(that.responses) : that.responses != null) return false;
        if (address != null ? !address.equals(that.address) : that.address != null) return false;
        if (channel != null ? !channel.equals(that.channel) : that.channel != null) return false;
        return config != null ? config.equals(that.config) : that.config == null;

    }

    @Override
    public int hashCode() {
        int result = lock != null ? lock.hashCode() : 0;
        result = 31 * result + id;
        result = 31 * result + (int) (queryTimeoutInMillisecond ^ (queryTimeoutInMillisecond >>> 32));
        result = 31 * result + (requests != null ? requests.hashCode() : 0);
        result = 31 * result + (responses != null ? responses.hashCode() : 0);
        result = 31 * result + (started ? 1 : 0);
        result = 31 * result + (closing ? 1 : 0);
        result = 31 * result + (havingNegotiation ? 1 : 0);
        result = 31 * result + (address != null ? address.hashCode() : 0);
        result = 31 * result + (channel != null ? channel.hashCode() : 0);
        result = 31 * result + (config != null ? config.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        // JDK8
        return "NSQConnectionImpl [id=" + id + ", havingNegotiation=" + havingNegotiation + ", closing=" + closing + ", address=" + address
                + ", channel=" + channel + ", config=" + config + ", queryTimeoutInMillisecond=" + queryTimeoutInMillisecond
                + "]";
    }


}
