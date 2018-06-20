package com.youzan.nsq.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.youzan.nsq.client.core.command.*;
import com.youzan.nsq.client.entity.*;
import com.youzan.nsq.client.exception.NSQNoConnectionException;
import com.youzan.nsq.client.network.frame.ErrorFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.nsq.client.network.frame.ResponseFrame;
import com.youzan.util.NamedThreadFactory;
import com.youzan.util.SystemUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public class NSQConnectionImpl implements Serializable, NSQConnection, Comparable {
    private final ListeningScheduledExecutorService resumeExec = MoreExecutors.listeningDecorator(
            Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("rdy-resume", Thread.NORM_PRIORITY)));

    public static final int INIT_RDY = 1;

    private static final Logger logger = LoggerFactory.getLogger(NSQConnectionImpl.class);
    private static final Logger PERF_LOG = LoggerFactory.getLogger(NSQConnectionImpl.class.getName() + ".perf");

    private static final long serialVersionUID = 7139923487863469738L;

    private final ReentrantReadWriteLock conLock = new ReentrantReadWriteLock();
    private final Long id; // primary key
    private final long queryTimeoutInMillisecond;

    private AtomicBoolean closing = new AtomicBoolean(Boolean.FALSE);
    private AtomicBoolean identitySent = new AtomicBoolean(Boolean.FALSE);
    private AtomicBoolean subSent = new AtomicBoolean(Boolean.FALSE);
    private AtomicBoolean backoff = new AtomicBoolean(Boolean.FALSE);

    protected final LinkedBlockingQueue<NSQCommand> requests = new LinkedBlockingQueue<>(1);
    protected final LinkedBlockingQueue<NSQFrame> responses = new LinkedBlockingQueue<>(1);

    private final Address address;
    protected final Channel channel;
    //topic for subscribe
    private Topic topic;
    private final NSQConfig config;

    //indicate if current should be extensible, if it is true, message received from nsqd should be extended.
    private final boolean isExtend;

    //start ready cnt for current count
    private AtomicInteger currentRdy = new AtomicInteger(0);
    private AtomicInteger lastRdy = new AtomicInteger(0);
    private AtomicInteger expectedRdy = new AtomicInteger(0);

    private final AtomicLong latestInternalID = new AtomicLong(-1L);
    private final AtomicLong latestDiskQueueOffset = new AtomicLong(-1L);

    //msg_timeout
    private int msgTimeout = 0;
    //max_msg_timeout
    private int maxMsgTimeout = 0;

    private volatile long lastMsgTouched;
    private volatile long lastMsgConsumptionFailed;

    public NSQConnectionImpl(long id, Address address, Channel channel, NSQConfig config) {
        this.id = id;
        this.address = address;
        this.channel = channel;
        this.config = config;
        this.expectedRdy.set(NSQConfig.DEFAULT_RDY);
        this.queryTimeoutInMillisecond = config.getQueryTimeoutInMillisecond();
        if (address.isTopicExtend()) {
            isExtend = Boolean.TRUE;
        } else {
            isExtend = Boolean.FALSE;
        }
    }

    public NSQConnectionImpl(long id, Address address, Channel channel, NSQConfig config, int computedRdyCeiling) {
        this.id = id;
        this.address = address;
        this.channel = channel;
        this.config = config;
        this.expectedRdy.set(computedRdyCeiling);
        this.queryTimeoutInMillisecond = config.getQueryTimeoutInMillisecond();
        if (address.isTopicExtend()) {
            isExtend = Boolean.TRUE;
        } else {
            isExtend = Boolean.FALSE;
        }
    }

    @Override
    public boolean isExtend() {
        return this.isExtend;
    }

    @Override
    public boolean checkOrder(long internalID, long diskQueueOffset, final NSQMessage msg) {
        if (!this.config.isOrdered())
            return true;
        if (internalID >= this.latestInternalID.get() && diskQueueOffset >= this.latestDiskQueueOffset.get()) {
            this.latestInternalID.set(internalID);
            this.latestDiskQueueOffset.set(diskQueueOffset);
            return true;
        } else {
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
    public void init() throws TimeoutException, IOException, ExecutionException {
        _init();
    }

    @Override
    public void init(final Topic topic) throws TimeoutException, IOException, ExecutionException {
        this._init();
        Topic topicCon = Topic.newInstacne(topic, true);
        setTopic(topicCon);
    }

    private void _init() throws TimeoutException, IOException, ExecutionException {
        assert address != null;
        assert config != null;
        if (identitySent.compareAndSet(Boolean.FALSE, Boolean.TRUE)) {
            command(Magic.getInstance());
            final NSQCommand identify = new Identify(config, isExtend());
            NSQFrame response = null;
            try {
                response = _commandAndGetResposne(null, identify);
            } catch (InterruptedException e) {
                logger.error("Identity fail to {}", this.address);
                Thread.currentThread().interrupt();
            }
            if (null == response) {
                throw new IllegalStateException("Bad Identify Response! Close connection!");
            }
            JsonNode respIdentify = SystemUtil.getObjectMapper().readTree(response.getData());
            JsonNode msgTimeoutNode = respIdentify.get(Identify.MSG_TIMEOUT);
            if (null != msgTimeoutNode) {
                this.msgTimeout = msgTimeoutNode.asInt();
            }
            JsonNode maxMsgTimeoutNode = respIdentify.get(Identify.MAX_MSG_TIMEOUT);
            if (null != maxMsgTimeoutNode) {
                this.maxMsgTimeout = maxMsgTimeoutNode.asInt();
            }
        }
        assert channel.isActive();
        if (logger.isDebugEnabled())
            logger.debug("Having initialized {}", this);
    }

    @Override
    public ChannelFuture command(NSQCommand cmd) {
        if (cmd == null) {
            return null;
        }

        // Use Netty Pipeline
        return channel.writeAndFlush(cmd);
    }

    private NSQFrame _commandAndGetResposne(final Context cxt, final NSQCommand command) throws TimeoutException, InterruptedException, ExecutionException {
        if (!requests.offer(command, queryTimeoutInMillisecond, TimeUnit.MILLISECONDS)) {
            throw new TimeoutException(
                    "The command timeout in " + queryTimeoutInMillisecond + " milliSec. The command name is : " + command.getClass().getName());
        }
        responses.clear(); // clear
        command(command);
        final NSQFrame frame = responses.poll(queryTimeoutInMillisecond, TimeUnit.MILLISECONDS);
        if (frame == null) {
            throw new TimeoutException(
                    "The command timeout receiving response frame in " + queryTimeoutInMillisecond + " milliSec. The command name is : " + command.getClass().getName());
        }
        requests.poll(); // clear
        return frame;
    }

    @Override
    public NSQFrame commandAndGetResponse(Context cxt, final NSQCommand command) throws TimeoutException, NSQNoConnectionException, ExecutionException {
        try {
            long isActiveStart = System.currentTimeMillis();
            if (!this._isConnected()) {
                throw new NSQNoConnectionException(String.format("%s is not connected， command %s quit.", this, command));
            }
            if (cxt != null && PERF_LOG.isDebugEnabled())
                PERF_LOG.debug("{}: tooks {} ms to check active", cxt.getTraceID(), System.currentTimeMillis() - isActiveStart);

            return _commandAndGetResposne(cxt, command);
        } catch (InterruptedException e) {
            logger.error("Thread was interrupted, probably shutting down! Close connection!", e);
            close();
            throw new NSQNoConnectionException(String.format("%s is interrupted， command %s quit.", this, command));
        }
    }

    @Override
    public void addResponseFrame(ResponseFrame frame) {
        if (!requests.isEmpty()) {
            try {
                responses.offer(frame, queryTimeoutInMillisecond * 2, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Thread was interrupted, probably shutting down!", e);
            }
        } else {
            logger.error("No request to send, but get a frame from the server. {}", frame);
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

    protected void setTopic(final Topic topic) {
        this.topic = topic;
    }

    @Override
    public Topic getTopic() {
        return this.topic;
    }

    @Override
    public boolean isConnected() {
        return this._isConnected();
    }

    private boolean _isConnected() {
        return !closing.get() && channel.isActive();
    }

    @Override
    public boolean isIdentitySent() {
        return _isConnected() && identitySent.get();
    }

    @Override
    public boolean isSubSent() {
        return _isConnected() && this.subSent.get();
    }

    @Override
    public boolean subSent() {
        return this.subSent.compareAndSet(Boolean.FALSE, Boolean.TRUE);
    }

    /**
     * clear underneath resources of {@Link NSQConnection}
     */
    @Override
    public void close() {
        logger.info("Begin to clear {}", this);
        //extra lock to proof from more than none thread waiting for close
        if (closing.compareAndSet(false, true)) {
            if (this.isSubSent())
                this.onClose();
            _clear();
        }
        this.resumeExec.shutdown();
        logger.info("End clear {}", this);
    }

    private void _clear() {
        if (null != channel) {
            channel.attr(NSQConnection.STATE).remove();
            channel.attr(Client.STATE).remove();
            if (channel.isActive()) {
                channel.close();
                channel.deregister();
            }
            if (!channel.isActive()) {
                logger.info("Having cleared {} OK!", this);
            }
        } else {
            logger.error("No channel has be set...");
        }
    }

    /**
     * disconnection current NSQConnection from nsqd, including
     * 1. backoff
     * 2. Send CLS command
     * 3. clear resources underneath
     */
    public void disconnect(final ConnectionManager conMgr) {
        logger.info("Disconnect from nsqd {} ...", this.address);
        //1. backoff
        conMgr.backoffConn(this, null);
        //2. send CLS
        this.close();
        logger.info("nsqd {} disconnect", this.address);
    }

    @Override
    public void onRdy(final int rdy, final IRdyCallback callback) {
        if (!this.isConnected()) {
            logger.info("Connection is closed. Resume quit. {}", this);
            int currentRdy = getCurrentRdyCount();
            callback.onUpdated(currentRdy, currentRdy);
            return;
        }

        if (backoff.get()) {
            logger.info("Connection is already backed off. {}", this);
            int currentRdy = getCurrentRdyCount();
            callback.onUpdated(currentRdy, currentRdy);
            return;
        }

        command(new Rdy(rdy)).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) throws Exception {
                if (channelFuture.isSuccess()) {
                    int lastRdy = getCurrentRdyCount();
                    setCurrentRdyCount(rdy);
                    callback.onUpdated(rdy, lastRdy);
                } else {
                    logger.warn("Fail to update Rdy for connection {}", this);
                }
            }
        });
    }

    public boolean isBackoff() {
        return this.backoff.get();
    }

    @Override
    public void onClose() {
        if (!this.channel.isActive() || !this.closing.get()) {
            logger.info("Connection is not connected nor NSQConnection.close  is not invoked. onClose quit. {}", this);
            return;
        } else {
            this._onClose();
            logger.info("[{}] onClose sent", this);
        }
    }

    private void _onClose() {
        //closing signal is updated here
        try {
            this._commandAndGetResposne(null, Close.getInstance());
        } catch (TimeoutException e) {
            logger.warn("Timeout receiving response for Close command.");
        } catch (InterruptedException e) {
            logger.error("Interrupted waiting for response from CLS.");
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.warn("fail to send CLS.", e);
        }
    }

    @Override
    public void onResume(final IRdyCallback callback) {
        if (!this.isConnected()) {
            logger.info("Connection is closed. Resume quit. {}", this);
            int currentRdy = getCurrentRdyCount();
            callback.onUpdated(currentRdy, currentRdy);
            return;
        }
        if (backoff.compareAndSet(true, false)) {
            final int rdy = INIT_RDY;
            command(new Rdy(rdy)).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    if (channelFuture.isSuccess()) {
                        int lastRdy = getCurrentRdyCount();
                        assert lastRdy == 0;
                        setCurrentRdyCount(rdy);
                        callback.onUpdated(rdy, lastRdy);
                    } else {
                        logger.warn("Fail to resume consumption for connection {}", this);
                    }
                }
            });
        } else {
            logger.info("Connection is not backed off. {}", this);
            int currentRdy = getCurrentRdyCount();
            callback.onUpdated(currentRdy, currentRdy);
        }
    }

    @Override
    public void onBackoff(final IRdyCallback backoffCallback, final long resumeDelayInSecond, final IRdyCallback resumeCallback) {
        if (!this.isConnected()) {
            logger.info("Connection is closed. Back off quit. {}", this);
            if(null != backoffCallback)
                backoffCallback.onUpdated(0, 0);
            return;
        }
        if (backoff.compareAndSet(false, true)) {
            //update last rdy
            command(Rdy.BACK_OFF).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    if (channelFuture.isSuccess()) {
                        int lastRdy = getCurrentRdyCount();
                        setCurrentRdyCount(0);
                        if(null != backoffCallback)
                            backoffCallback.onUpdated(0, lastRdy);
                        //resume with delay
                        resumeExec.schedule(new Runnable() {
                            @Override
                            public void run() {
                                onResume(resumeCallback);
                            }
                        }, resumeDelayInSecond, TimeUnit.SECONDS);
                    } else {
                        logger.warn("Fail to backoff consumption for connection {}", this);
                    }
                }
            });
        } else {
            logger.info("Connection is already backed off. {}", this);
            //notify callback with new rdy and old rdy
            if(null != backoffCallback)
                backoffCallback.onUpdated(0, 0);
        }
    }

    @Override
    public void onBackoff(final IRdyCallback callback) {
        if (!this.isConnected()) {
            logger.info("Connection is closed. Back off quit. {}", this);
            if(null != callback)
                callback.onUpdated(0, 0);
            return;
        }
        if (backoff.compareAndSet(false, true)) {
            //update last rdy
            command(Rdy.BACK_OFF).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    if (channelFuture.isSuccess()) {
                        int lastRdy = getCurrentRdyCount();
                        setCurrentRdyCount(0);
                        if(null != callback)
                            callback.onUpdated(0, lastRdy);
                    } else {
                        logger.warn("Fail to backoff consumption for connection {}", this);
                    }
                }
            });
        } else {
            logger.info("Connection is already backed off. {}", this);
            //notify callback with new rdy and old rdy
            if(null != callback)
                callback.onUpdated(0, 0);
        }
    }

    public synchronized void setCurrentRdyCount(int newCount) {
        if (newCount < 0 || this.currentRdy.get() == newCount) {
            return;
        }
        this.lastRdy.set(this.currentRdy.get());
        this.currentRdy.set(newCount);
    }

    public boolean declineExpectedRdy() {
        int currentExpRdy = this.expectedRdy.get();
        int newExpRdy = this.config.getExpectedRdyUpdatePolicy().expectedRdyDecline(currentExpRdy,
                this.config.getRdy());
        return this.expectedRdy.compareAndSet(currentExpRdy, newExpRdy);
    }

    public boolean increaseExpectedRdy(int rdyCeiling) {
        int currentExpRdy = this.expectedRdy.get();
        rdyCeiling = this.config.isRdyOverride() ? this.config.getRdy() : rdyCeiling;
        int newExpRdy = this.config.getExpectedRdyUpdatePolicy().expectedRdyIncrease(currentExpRdy,
                rdyCeiling);
        return this.expectedRdy.compareAndSet(currentExpRdy, newExpRdy);
    }

    public int getExpectedRdy() {
        return this.expectedRdy.get();
    }

    public void setExpectedRdy(int expectedRdy) {
        int originalExpectedRdy = this.expectedRdy.get();
        if (originalExpectedRdy != expectedRdy && this.expectedRdy.compareAndSet(originalExpectedRdy, expectedRdy))
            logger.info("Expected rdy set to {} from {}, connection: {}", expectedRdy, originalExpectedRdy, this);
    }

    public int getCurrentRdyCount() {
        return this.currentRdy.get();
    }

    /**
     * @return the id , the primary key of the object
     */
    @Override
    public long getId() {
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
        long res = getId() - ((NSQConnectionImpl) o).getId();
        if (res > 0l) {
            return 1;
        } else if (res < 0l) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NSQConnectionImpl that = (NSQConnectionImpl) o;

        if (id != that.id) return false;
        return address != null ? address.equals(that.address) : that.address == null;
    }

    @Override
    public int hashCode() {
        int result = 0;
        result = 31 * result + id.hashCode();
        result = 31 * result + (closing.get() ? 1 : 0);
        result = 31 * result + (identitySent.get() ? 1 : 0);
        result = 31 * result + (address != null ? address.hashCode() : 0);
        return result;
    }

    @Override
    public void setMessageTouched(long timeStamp) {
        this.lastMsgTouched = timeStamp;
    }

    @Override
    public long lastMessageTouched() {
        return this.lastMsgTouched;
    }

    @Override
    public void setMessageConsumptionFailed(long timeStamp) {
        this.lastMsgConsumptionFailed = timeStamp;
    }

    @Override
    public long lastMessageConsumptionFailed() {
        return this.lastMsgConsumptionFailed;
    }

    @Override
    public String toString() {
        // JDK8
        return "NSQConnectionImpl [id=" + id + ", identitySent=" + identitySent.get() + ", closing=" + closing + ", address=" + address
                + ", channel=" + channel + ", config=" + config + ", queryTimeoutInMillisecond=" + queryTimeoutInMillisecond
                + "]";
    }

    @Override
    public ChannelFuture finish(final NSQMessage msg) {
        if (this.isConnected()) {
            return command(new Finish(msg.getMessageID()));
        } else {
            logger.info("Connection for message {} is closed. Finish exits.", msg);
            return null;
        }
    }

    @Override
    public ChannelFuture requeue(final NSQMessage msg, int defaultDelayInSecond) {
        int delayInSecond = defaultDelayInSecond;
        if(delayInSecond < 0) {
            delayInSecond = msg.getNextConsumingInSecond();
        }
        if (this.isConnected()) {
            return command(new ReQueue(msg.getMessageID(), delayInSecond));
        } else {
            logger.info("Connection for message {} is closed. Requeue exits.", msg);
            return null;
        }
    }
}
