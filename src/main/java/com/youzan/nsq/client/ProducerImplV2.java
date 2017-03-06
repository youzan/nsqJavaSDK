package com.youzan.nsq.client;

import com.youzan.nsq.client.core.LookupAddressUpdate;
import com.youzan.nsq.client.core.NSQConnection;
import com.youzan.nsq.client.core.NSQSimpleClient;
import com.youzan.nsq.client.core.command.Pub;
import com.youzan.nsq.client.core.pool.producer.KeyedPooledConnectionFactory;
import com.youzan.nsq.client.entity.*;
import com.youzan.nsq.client.exception.*;
import com.youzan.nsq.client.network.frame.ErrorFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.util.HostUtil;
import com.youzan.util.IOUtil;
import com.youzan.util.NamedThreadFactory;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <pre>
 * Use {@link NSQConfig} to set the lookup cluster.
 * It uses one connection pool(client connects to one broker) underlying TCP and uses
 * {@link GenericKeyedObjectPool} which is composed of many sub-pools.
 * </pre>
 *
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public class ProducerImplV2 implements Producer {

    private static final int MAX_PUBLISH_RETRY = 6;

    private static final int MAX_MSG_OUTPUT_LEN = 100;
    private static final Logger logger = LoggerFactory.getLogger(ProducerImplV2.class);
    private volatile boolean started = false;
    private volatile int offset = 0;
    private final GenericKeyedObjectPoolConfig poolConfig;
    private final KeyedPooledConnectionFactory factory;
    private GenericKeyedObjectPool<Address, NSQConnection> bigPool = null;
    private final ScheduledExecutorService scheduler = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory(this.getClass().getName(), Thread.NORM_PRIORITY));
    private final ConcurrentHashMap<Topic, Long> topic_2_lastActiveTime = new ConcurrentHashMap<>();

    private final AtomicInteger success = new AtomicInteger(0);
    private final AtomicInteger total = new AtomicInteger(0);

    private final NSQConfig config;
    private final NSQSimpleClient simpleClient;
    /**
     * @param config NSQConfig
     */
    public ProducerImplV2(NSQConfig config) {
        this.config = config;
        this.simpleClient = new NSQSimpleClient(Role.Producer, this.config.getUserSpecifiedLookupAddress());

        this.poolConfig = new GenericKeyedObjectPoolConfig();
        this.factory = new KeyedPooledConnectionFactory(this.config, this);
    }

    @Override
    public void start() throws NSQException {
        if (!this.started) {
            this.started = true;
            // setting all of the configs
            this.offset = _r.nextInt(100);
            this.poolConfig.setLifo(true);
            this.poolConfig.setFairness(false);
            this.poolConfig.setTestOnBorrow(false);
            this.poolConfig.setTestOnReturn(false);
            this.poolConfig.setTestWhileIdle(true);
            this.poolConfig.setJmxEnabled(true);
            this.poolConfig.setMinEvictableIdleTimeMillis(5 * 60 * 1000);
            this.poolConfig.setSoftMinEvictableIdleTimeMillis(5 * 60 * 1000);
            //1 * 60 * 1000
            this.poolConfig.setTimeBetweenEvictionRunsMillis(60 * 1000);
            this.poolConfig.setMinIdlePerKey(1);
            this.poolConfig.setMaxIdlePerKey(this.config.getConnectionSize());
            this.poolConfig.setMaxTotalPerKey(this.config.getConnectionSize());
            // acquire connection waiting time
            this.poolConfig.setMaxWaitMillis(this.config.getQueryTimeoutInMillisecond());
            this.poolConfig.setBlockWhenExhausted(false);
            // new instance without performing to connect
            this.bigPool = new GenericKeyedObjectPool<>(this.factory, this.poolConfig);
            //simple client starts and LookupAddressUpdate instance initialized there.
            this.simpleClient.start();
            if(this.config.getUserSpecifiedLookupAddress()) {
                LookupAddressUpdate.getInstance().setUpDefaultSeedLookupConfig(this.simpleClient.getLookupLocalID(), this.config.getLookupAddresses());
            }
            scheduler.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    // We make a decision that the resources life time should be less than 2 hours
                    // Normal max lifetime is 1 hour
                    // Extreme max lifetime is 1.5 hours
                    final long allow = System.currentTimeMillis() - 3600 * 1000L;
                    final Set<Topic> expiredTopics = new HashSet<>();
                    for (Map.Entry<Topic, Long> pair : topic_2_lastActiveTime.entrySet()) {
                        if (pair.getValue() < allow) {
                            expiredTopics.add(pair.getKey());
                        }
                    }
                    for (Topic topic : expiredTopics) {
                        topic_2_lastActiveTime.remove(topic);
                        try {
                            simpleClient.removeTopic(topic);
                        } catch (Exception e) {
                            logger.error("Exception", e);
                        }
                    }
                    expiredTopics.clear();
                    logger.info("Publish. Total: {} , Success: {} . Two values do not use a lock action.", total.get(), success.get());
                }
            }, 30, 30, TimeUnit.MINUTES);
        }
        logger.info("The producer has been started.");
    }

    /**
     * Get a connection foreach every broker in one loop till get one available because I don't believe
     * that every broker is down or every pool is busy.
     *
     * @param topic a topic name
     * @return a validated {@link NSQConnection}
     * @throws NSQException that is having done a negotiation
     */
    private NSQConnection getNSQConnection(Topic topic, Object topicShardingID) throws NSQException {
        final Long now = System.currentTimeMillis();
        topic_2_lastActiveTime.put(topic, now);
        //data nodes matches topic sharding returns
        Address[] partitonAddrs = simpleClient.getPartitionNodes(topic, topicShardingID, true);
        final int size = partitonAddrs.length;
        int c = 0, index = (this.offset++);
        while (c++ < size) {
            // current broker | next broker when have a try again
            final int effectedIndex = (index++ & Integer.MAX_VALUE) % size;
            final Address address = partitonAddrs[effectedIndex];
            try {
                return bigPool.borrowObject(address);
            } catch (Exception e) {
                logger.error("Fail to fetch connection for publish. DataNode Size: {} , CurrentRetries: {} , Address: {} , Exception:", size, c, address, e);
            }
        }
        // no available {@link NSQConnection}
        return null;
    }

    public void publish(String message, String topic) throws NSQException {
        publish(message.getBytes(IOUtil.DEFAULT_CHARSET), topic);
    }

    /**
     * publish with shardingID object to passin topic. Message to publish is accept as {@link String}, and it will be
     * convert to {@link byte[]} with {@link java.nio.charset.Charset#defaultCharset()}
     * @param message message to send
     * @param topic topic
     * @param shardingID shardingID object.
     * @throws NSQException exception raised in publish process.
     */
    public void publish(String message, final Topic topic, Object shardingID) throws NSQException {
        Message msg = Message.create(topic, message);
        msg.setTopicShardingIDObject(shardingID);
        publish(msg);
    }

    @Override
    public void publish(Message message) throws NSQException {
        if (message == null || message.getMessageBody().isEmpty()) {
            throw new IllegalArgumentException("Your input message is blank! Please check it!");
        }
        Topic topic = message.getTopic();
        if (null == topic || null == topic.getTopicText() || topic.getTopicText().isEmpty()) {
            throw new IllegalArgumentException("Your input topic name is blank!");
        }
        if (!started) {
            throw new IllegalStateException("Producer must be started before producing messages!");
        }
        total.incrementAndGet();
        try{
            sendPUB(message);
        }catch (NSQPubException pubE){
            logger.error(pubE.getLocalizedMessage());
            pubE.punchExceptions(logger);
            List<? extends NSQException> exceptions = pubE.getNestedExceptions();
            throw exceptions.get(exceptions.size() - 1);
        }
    }

    @Override
    /**
     * publish message to topic, in ALL partitions pass in topic has,
     * function publishes message to topic, in specified partition.
     *
     * @param message message data in byte data array.
     * @param topic topic message publishes to.
     * @throws NSQException
     */
    public void publish(byte[] message, String topic) throws NSQException {
        publish(message, new Topic(topic));
    }

    @Override
    /**
     * publish message to topic, if topic partition id is specified,
     * function publishes message to topic, in specified partition.
     *
     * @param message message data in byte data array.
     * @param topic topic message publishes to.
     * @throws NSQException
     */
    public void publish(byte[] message, Topic topic) throws NSQException {
        Message msg = Message.create(topic, new String(message));
        publish(msg);
    }

    private void sendPUB(final Message msg) throws NSQException {
        int c = 0; // be continuous
        boolean returnCon = true;
        NSQConnection conn;
        List<NSQException> exceptions = new ArrayList<>();
        while (c++ < MAX_PUBLISH_RETRY) {
            if (c > 1) {
                logger.debug("Sleep. CurrentRetries: {}", c);
                sleep((1 << (c - 1)) * this.config.getProducerRetryIntervalBaseInMilliSeconds());
            }
            try {
                conn = getNSQConnection(msg.getTopic(), msg.getTopicShardingId());
                if (conn == null) {
                    exceptions.add(new NSQDataNodesDownException("Could not get NSQd connection for " + msg.getTopic().toString() + ", topic may does not exist, or connection pool resource exhausted."));
                    continue;
                }
            }
            catch (NSQTopicNotFoundException exp) {
                //throw it directly
                throw exp;
            }
            catch(NSQException lookupE) {
                exceptions.add(lookupE);
                continue;
            }
            //create PUB command
            try {
                final Pub pub = createPubCmd(msg);
                final NSQFrame frame = conn.commandAndGetResponse(pub);
                handleResponse(msg.getTopic(), frame, conn);
                success.incrementAndGet();
                if(msg.isTraced() && frame instanceof MessageMetadata && TraceLogger.isTraceLoggerEnabled() && conn.getAddress().isHA())
                    TraceLogger.trace(this, conn, (MessageMetadata) frame);
                return;
            }
            catch(NSQPubFactoryInitializeException expShouldFail) {
                throw expShouldFail;
            }
            catch (Exception e) {
                IOUtil.closeQuietly(conn);
                returnCon = false;
                try {
                    bigPool.invalidateObject(conn.getAddress(), conn);
                    logger.info("Connection invalidated.");
                } catch (Exception e1) {
                    logger.error("Fail to destroy connection {}.", conn.toString(), e1);
                }
                String msgStr = msg.getMessageBody();
                int maxlen = msgStr.length() > MAX_MSG_OUTPUT_LEN ? MAX_MSG_OUTPUT_LEN : msgStr.length();
                logger.error("MaxRetries: {} , CurrentRetries: {} , Address: {} , Topic: {}, MessageLength: {}, RawMessage: {}, Exception:", MAX_PUBLISH_RETRY, c,
                        conn.getAddress(), msg.getTopic(), msgStr.length(), msgStr.substring(0, maxlen), e);
                //as to NSQInvalidMessaegException throw it out after conenction close.
                if(e instanceof NSQInvalidMessageException)
                    throw (NSQInvalidMessageException)e;
                NSQException nsqE = new NSQException(e);
                exceptions.add(nsqE);
                if (c >= MAX_PUBLISH_RETRY) {
                    throw new NSQPubException(exceptions);
                }
            } finally {
                if(returnCon)
                    bigPool.returnObject(conn.getAddress(), conn);
            }
        } // end loop
        if (c >= MAX_PUBLISH_RETRY) {
            throw new NSQPubException(exceptions);
        }
    }

    /**
     * function to create publish command based on traceability
     * @return Pub command
     */
    private Pub createPubCmd(final Message msg) throws NSQPubFactoryInitializeException {
       return PubCmdFactory.getInstance().create(msg, this.config);
    }

    private void handleResponse(final Topic topic, NSQFrame frame, NSQConnection conn) throws NSQException {
        if (frame == null) {
            logger.warn("SDK bug: the frame is null.");
            return;
        }
        switch (frame.getType()) {
            case RESPONSE_FRAME: {
                break;
            }
            case ERROR_FRAME: {
                final ErrorFrame err = (ErrorFrame) frame;
                switch (err.getError()) {
                    case E_BAD_TOPIC: {
                        throw new NSQInvalidTopicException(topic.getTopicText());
                    }
                    case E_BAD_MESSAGE: {
                        throw new NSQInvalidMessageException();
                    }
                    case E_FAILED_ON_NOT_LEADER: {
                    }
                    case E_FAILED_ON_NOT_WRITABLE: {
                    }
                    case E_TOPIC_NOT_EXIST: {
                        logger.error("Address: {} , Frame: {}", conn.getAddress(), frame);
                        //clean topic 2 partitions selector and force a lookup for topic
                        this.simpleClient.invalidatePartitionsSelector(topic, conn.getAddress());
                        logger.info("Partitions info for {} invalidated and related lookup force updated.");
                        throw new NSQInvalidDataNodeException(topic.getTopicText());
                    }
                    default: {
                        throw new NSQException("Unknown response error! The topic is " + topic + " . The error frame is " + err);
                    }
                }
            }
            default: {
                logger.warn("When handle a response, expect FrameTypeResponse or FrameTypeError, but not.");
            }
        }
    }

    @Override
    public void incoming(NSQFrame frame, NSQConnection conn) throws NSQException {
        simpleClient.incoming(frame, conn);
    }


    @Override
    public void backoff(NSQConnection conn) {
    }


    @Override
    public void publishMulti(List<byte[]> messages, String topic) throws NSQException {
    }

    @Override
    public boolean validateHeartbeat(NSQConnection conn) {
        return simpleClient.validateHeartbeat(conn);
    }

    @Override
    public Set<NSQConnection> clearDataNode(Address address) {
        return null;
    }

    @Override
    public void close() {
        IOUtil.closeQuietly(simpleClient);
        if (factory != null) {
            factory.close();
        }
        if (bigPool != null) {
            bigPool.close();
        }
        scheduler.shutdownNow();
        logger.info("The producer has been closed.");
        LookupAddressUpdate.getInstance().closed();
    }

    public void close(NSQConnection conn) {
        conn.close();
    }

    private void sleep(final long millisecond) {
        logger.debug("Sleep {} millisecond.", millisecond);
        try {
            Thread.sleep(millisecond);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Your machine is too busy! Please check it!");
        }
    }

    public String toString(){
        String ipStr = "";
        try {
            ipStr = HostUtil.getLocalIP();
        } catch (IOException e) {
            logger.warn(e.getLocalizedMessage());
        }
        return "[Producer] at " + ipStr;
    }
}
