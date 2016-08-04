/**
 *
 */
package com.youzan.nsq.client.core;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.core.command.Identify;
import com.youzan.nsq.client.core.command.Magic;
import com.youzan.nsq.client.core.command.NSQCommand;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.network.frame.ErrorFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.nsq.client.network.frame.ResponseFrame;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

/**
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public class NSQConnectionImpl implements NSQConnection {
    private static final Logger logger = LoggerFactory.getLogger(NSQConnectionImpl.class);

    private final int id; // primary key

    private final ReentrantLock lock = new ReentrantLock();
    private final LinkedBlockingQueue<NSQCommand> requests = new LinkedBlockingQueue<>(1);
    private final LinkedBlockingQueue<NSQFrame> responses = new LinkedBlockingQueue<>(1);

    private volatile boolean closing = false;
    private volatile boolean closed = false;
    private boolean havingNegotiation = false;

    private final Address address;
    private final Channel channel;
    private final NSQConfig config;
    private final long timeoutInMillisecond;

    public NSQConnectionImpl(int id, Address address, Channel channel, NSQConfig config) {
        this.id = id;
        this.address = address;
        this.channel = channel;
        this.config = config;

        this.timeoutInMillisecond = config.getQueryTimeoutInMillisecond();

    }

    @Override
    public void init() throws TimeoutException {
        assert address != null;
        assert config != null;

        if (!havingNegotiation) {
            command(Magic.getInstance());
            final NSQCommand ident = new Identify(config);
            final NSQFrame response = commandAndGetResponse(ident);
            if (null == response) {
                throw new IllegalStateException("Bad Identify Response! Close connection!");
            }
            havingNegotiation = true;
        }
        assert havingNegotiation;
        assert channel.isActive();
        assert isConnected();
        logger.info("Having initiated {}", this);
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
        lock.lock();
        try {
            final long start = System.currentTimeMillis();
            try {
                long timeout = timeoutInMillisecond - (0L);
                if (!requests.offer(command, timeout, TimeUnit.MILLISECONDS)) {
                    throw new TimeoutException(
                            "The command is timeout. The command name is : " + command.getClass().getName());
                }

                responses.clear(); // clear
                // write data
                final ChannelFuture future = command(command);

                // wait to get the response
                timeout = timeoutInMillisecond - (start - System.currentTimeMillis());
                if (!future.await(timeout, TimeUnit.MILLISECONDS)) {
                    throw new TimeoutException(
                            "The command is timeout. The command name is : " + command.getClass().getName());
                }
                timeout = timeoutInMillisecond - (start - System.currentTimeMillis());
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
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void addResponseFrame(ResponseFrame frame) {
        if (!requests.isEmpty()) {
            try {
                responses.offer(frame, timeoutInMillisecond * 2, TimeUnit.MILLISECONDS);
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
            responses.offer(frame, timeoutInMillisecond, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Thread was interrupted, probably shutting down!", e);
        }
    }

    @Override
    public boolean isConnected() {
        return channel.isActive() && havingNegotiation;
    }

    @Override
    public void close() {
        if (closing && closed) {
            return;
        }
        if (closing == false) {
            closing = true;
            if (null != channel) {
                // It is very important!
                channel.attr(NSQConnection.STATE).remove();
                channel.attr(Client.STATE).remove();
                channel.close();
                havingNegotiation = false;
                logger.info("Having closed {} OK!", this);
            } else {
                logger.error("No channel be setted?");
            }
        }
        closed = true;
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
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((address == null) ? 0 : address.hashCode());
        result = prime * result + ((channel == null) ? 0 : channel.hashCode());
        result = prime * result + (closed ? 1231 : 1237);
        result = prime * result + (closing ? 1231 : 1237);
        result = prime * result + ((config == null) ? 0 : config.hashCode());
        result = prime * result + (havingNegotiation ? 1231 : 1237);
        result = prime * result + id;
        result = prime * result + ((requests == null) ? 0 : requests.hashCode());
        result = prime * result + ((responses == null) ? 0 : responses.hashCode());
        result = prime * result + (int) (timeoutInMillisecond ^ (timeoutInMillisecond >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        NSQConnectionImpl other = (NSQConnectionImpl) obj;
        if (address == null) {
            if (other.address != null) {
                return false;
            }
        } else if (!address.equals(other.address)) {
            return false;
        }
        if (channel == null) {
            if (other.channel != null) {
                return false;
            }
        } else if (!channel.equals(other.channel)) {
            return false;
        }
        if (closed != other.closed) {
            return false;
        }
        if (closing != other.closing) {
            return false;
        }
        if (config == null) {
            if (other.config != null) {
                return false;
            }
        } else if (!config.equals(other.config)) {
            return false;
        }
        if (havingNegotiation != other.havingNegotiation) {
            return false;
        }
        if (id != other.id) {
            return false;
        }
        if (requests == null) {
            if (other.requests != null) {
                return false;
            }
        } else if (!requests.equals(other.requests)) {
            return false;
        }
        if (responses == null) {
            if (other.responses != null) {
                return false;
            }
        } else if (!responses.equals(other.responses)) {
            return false;
        }
        if (timeoutInMillisecond != other.timeoutInMillisecond) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        // JDK8
        return "NSQConnectionImpl [id=" + id + ", havingNegotiation=" + havingNegotiation + ", address=" + address
                + ", channel=" + channel + ", config=" + config + ", timeoutInMillisecond=" + timeoutInMillisecond
                + "]";
    }

}
