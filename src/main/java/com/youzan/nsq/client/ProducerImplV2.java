package com.youzan.nsq.client;

import com.youzan.nsq.client.configs.ConfigAccessAgent;
import com.youzan.nsq.client.core.LookupAddressUpdate;
import com.youzan.nsq.client.core.NSQConnection;
import com.youzan.nsq.client.core.NSQSimpleClient;
import com.youzan.nsq.client.core.command.Pub;
import com.youzan.nsq.client.core.pool.producer.KeyedPooledConnectionFactory;
import com.youzan.nsq.client.entity.*;
import com.youzan.nsq.client.exception.*;
import com.youzan.nsq.client.network.frame.ErrorFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.nsq.client.network.frame.ResponseFrame;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
    private static final Logger logger = LoggerFactory.getLogger(ProducerImplV2.class);
    private static final Logger PERF_LOG = LoggerFactory.getLogger(ProducerImplV2.class.getName() + ".perf");

    private static final int MAX_MSG_OUTPUT_LEN = 100;
    private static final int NSQ_LEADER_NOT_READY_TIMEOUT = 100;

    private AtomicBoolean started = new AtomicBoolean(Boolean.FALSE);
    private AtomicBoolean closing = new AtomicBoolean(Boolean.FALSE);

    private volatile int offset = 0;
    private final GenericKeyedObjectPoolConfig poolConfig;
    private final KeyedPooledConnectionFactory factory;
    private GenericKeyedObjectPool<Address, NSQConnection> bigPool = null;
    private final ScheduledExecutorService scheduler = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory(this.getClass().getName(), Thread.NORM_PRIORITY));
    private final ConcurrentHashMap<String, Long> topic_2_lastActiveTime = new ConcurrentHashMap<>();

    private final AtomicInteger success = new AtomicInteger(0);
    private final AtomicInteger total = new AtomicInteger(0);
    private final AtomicLong pubTraceIdGen = new AtomicLong(0);

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

    public NSQConfig getConfig() {
        return this.config;
    }

    private boolean validateLookupdSource() {
        if(this.config.getUserSpecifiedLookupAddress()) {
            String[] lookupdAddresses = this.config.getLookupAddresses();
            if(null == lookupdAddresses || lookupdAddresses.length == 0) {
                logger.error("Seed lookupd addresses is not specified in NSQConfig. Seed lookupd addresses: {}", lookupdAddresses);
                return false;
            }
        } else {
            try {
                ConfigAccessAgent.getInstance();
            } catch (ConfigAccessAgentException e) {
                logger.error("ConfigAccessAgent fail to initialize.");
                return false;
            }
            String[] configRemoteURLS = ConfigAccessAgent.getConfigAccessRemotes();
            String configRemoteEnv = ConfigAccessAgent.getEnv();
            if(null == configRemoteURLS || configRemoteURLS.length == 0 || null == configRemoteEnv) {
                logger.error("Config remote URLs or env is not specified in NSQConfig. URLs: {}, env: {}", configRemoteURLS, configRemoteEnv);
                return false;
            }
        }
        return true;
    }

    class ExpiredTopicCleaner implements Runnable {
        private Long expiration = 3600 * 1000L;

        public void setExpiration(long expration) {
            this.expiration = expration;
        }

        public long getExpiration() {
            return this.expiration;
        }

        @Override
        public void run() {
            final long allow = System.currentTimeMillis() - this.expiration;
            final Map<String, Long> expiredTopicsMap = new HashMap<>();
            for (Map.Entry<String, Long> pair : topic_2_lastActiveTime.entrySet()) {
                if (pair.getValue() < allow) {
                    expiredTopicsMap.put(pair.getKey(), pair.getValue());
                }
            }
            if(expiredTopicsMap.size() > 0) {
                try {
                    long now = System.currentTimeMillis();
                    simpleClient.removeTopics(expiredTopicsMap.keySet());
                    logger.info("Expired {} topic resource cleaner exits in {} milliSec.", expiredTopicsMap.size(), System.currentTimeMillis() - now);
                    logger.info("Publish. Total: {} , Success: {}.", total.get(), success.get());
                } finally {
                    for (Map.Entry<String, Long> pair : expiredTopicsMap.entrySet()) {
                        topic_2_lastActiveTime.remove(pair.getKey(), pair.getValue());
                    }
                }
            } else {
                logger.info("Expired topic not found. expired topic process exit.");
            }
            expiredTopicsMap.clear();
        }
    }

    private final ExpiredTopicCleaner EXPIRED_TOPIC_CLEANER = new ExpiredTopicCleaner();

    public ExpiredTopicCleaner getTopicExpirationCleaner() {
        return this.EXPIRED_TOPIC_CLEANER;
    }

    @Override
    public void start(String... topics) throws NSQException {
        this.start();
        if(null != topics && topics.length > 0) {
            Set<Address> nsqdAddrs = new HashSet<>();
            Object[] noSharding = new Object[]{Message.NO_SHARDING};
            logger.info("start initializing connections for {}", topics);
            for (String topic : topics) {
                try {
                    this.simpleClient.putTopic(topic);
                    nsqdAddrs.addAll(
                            Arrays.asList(
                                    this.simpleClient.getPartitionNodes(new Topic(topic), noSharding, true)
                            )
                    );
                } catch (InterruptedException e) {
                    logger.error("error waiting for topic 2 partition info updating.");
                }
            }
            logger.info("total {} addresses to initialize");
            for(Address addr : nsqdAddrs)
                try {
                    this.bigPool.preparePool(addr);
                } catch (Exception e) {
                    logger.error("fail to initialize connection to {}", addr);
                }
        }
    }

    @Override
    public void start() throws NSQException {
        if (started.compareAndSet(Boolean.FALSE, Boolean.TRUE)) {
            String configJsonStr = NSQConfig.parseNSQConfig(this.config);
            logger.info("Config for producer {}: {}", this, configJsonStr);
            if (!validateLookupdSource()) {
                throw new IllegalArgumentException("Producer could not start with invalid lookupd address sources.");
            }

            // setting all of the configs
            this.offset = _r.nextInt(100);

            this.poolConfig.setLifo(false);
            this.poolConfig.setFairness(false);
            this.poolConfig.setTestOnBorrow(false);
            this.poolConfig.setTestOnReturn(false);
            //If testWhileIdle is true, during idle eviction, examined objects are validated when visited (and removed if invalid);
            //otherwise only objects that have been idle for more than minEvicableIdleTimeMillis are removed.
            this.poolConfig.setTestWhileIdle(true);
            this.poolConfig.setJmxEnabled(true);
            //connection need being validated after idle time, default to 60 * heartbeat interval in millisec
            this.poolConfig.setMinEvictableIdleTimeMillis(30 * config.getHeartbeatIntervalInMillisecond());
            //number of milliseconds to sleep between runs of the idle object evictor thread
            this.poolConfig.setTimeBetweenEvictionRunsMillis(config.getProducerConnectionEvictIntervalInMillSec());
            this.poolConfig.setMinIdlePerKey(this.config.getMinIdleConnectionForProducer());
            this.poolConfig.setMaxIdlePerKey(this.config.getConnectionSize());
            this.poolConfig.setMaxTotalPerKey(this.config.getConnectionSize());
            // acquire connection waiting time
            this.poolConfig.setMaxWaitMillis(this.config.getConnWaitTimeoutForProducerInMilliSec());
            this.poolConfig.setBlockWhenExhausted(false);
            // new instance without performing to connect
            this.bigPool = new GenericKeyedObjectPool<>(this.factory, this.poolConfig);
            if (this.config.getUserSpecifiedLookupAddress()) {
                LookupAddressUpdate.getInstance().setUpDefaultSeedLookupConfig(this.simpleClient.getLookupLocalID(), this.config.getLookupAddresses());
            }
            //simple client starts and LookupAddressUpdate instance initialized there.
            this.simpleClient.start();
            scheduler.scheduleAtFixedRate(EXPIRED_TOPIC_CLEANER, 30, 30, TimeUnit.MINUTES);
            logger.info("The producer {} has been started.", this);
        }
    }

    public GenericKeyedObjectPool<Address, NSQConnection> getConnectionPool() {
        return this.bigPool;
    }

    /**
     * Get a nsqd connection for passin topic. This function first queries simple client with passin topic for partition
     * info or nsqd producer info, then borrows nsqd connection from connection pool.
     *
     * @param topic a topic name
     * @param topicShardingID topic sharding ID
     * @param cxt   context
     * @return a validated {@link NSQConnection}
     * @throws NSQException that is having done a negotiation
     */
    protected NSQConnection getNSQConnection(Topic topic, Object topicShardingID, final Context cxt) throws NSQException {
        Address[] partitonAddrs;
        try {
            partitonAddrs = simpleClient.getPartitionNodes(topic, new Object[]{topicShardingID}, true);
        } catch (InterruptedException e) {
            logger.warn("Thread interrupted waiting for partition selector update, Topic {}. Ignore if SDK is shutting down.", topic.getTopicText());
            return null;
        }
        if(null == partitonAddrs) {
            return null;
        }
        final int size = partitonAddrs.length;
        int c = 0, index = (this.offset++);
        while (c < size) {
            // current broker | next broker when have a try again
            final int effectedIndex = (index++ & Integer.MAX_VALUE) % size;
            final Address address = partitonAddrs[effectedIndex];
            long borrowConnStart = System.currentTimeMillis();
            try {
                return bigPool.borrowObject(address);
            } catch (NSQNoConnectionException badConn){
                logger.error("Fail to create connection. DataNode Size: {} , CurrentRetries: {} , Address: {} , Exception:", size, c, address, badConn);
                if (c >= size) {
                    logger.info("Connection pool tries out of nsqd addresses.");
                    throw badConn;
                }
            } catch (Exception e) {
                logger.error("Fail to fetch connection for publish. DataNode Size: {} , CurrentRetries: {} , Address: {} , Exception:", size, c, address, e);
            } finally {
                long borrowConnEnd = System.currentTimeMillis() - borrowConnStart;
                if(PERF_LOG.isDebugEnabled()) {
                    PERF_LOG.debug("{}: took {} milliSec to borrow connection from producer pool. CurrentRetries is {}", cxt.getTraceID(), borrowConnEnd, c);
                }
                if(borrowConnEnd > PerfTune.getInstance().getNSQConnBorrowLimit()) {
                    PERF_LOG.warn("{}: took {} milliSec to borrow connection from producer pool. Limitation is {}. CurrentRetries is {}", cxt.getTraceID(), borrowConnEnd, PerfTune.getInstance().getNSQConnBorrowLimit(), c);
                }
            }
            c++;
        }
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
    public void publish(final Message message) throws NSQException {
        final Context cxt = new Context();
        if(PERF_LOG.isDebugEnabled()) {
            cxt.setTraceID(pubTraceIdGen.getAndIncrement());
        }
        if (message == null || message.getMessageBody().isEmpty()) {
            throw new IllegalArgumentException("Your input message is blank! Please check it!");
        }
        Topic topic = message.getTopic();
        if (null == topic || null == topic.getTopicText() || topic.getTopicText().isEmpty()) {
            throw new IllegalArgumentException("Your input topic name is blank!");
        }
        if (!started.get() || closing.get()) {
            throw new IllegalStateException("Producer must be started before producing messages!");
        }
        total.incrementAndGet();

        try{
            //TODO: poll before timeout
            sendPUB(message, cxt);
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
        Message msg = Message.create(topic, message);
        publish(msg);
    }

    private void sendPUB(final Message msg, Context cxt) throws NSQException {
        int c = 0; // be continuous
        boolean returnCon;
        NSQConnection conn = null;
        List<NSQException> exceptions = new ArrayList<>();
        long start = System.currentTimeMillis();
        int retry = this.config.getPublishRetry();
        while (c++ < retry) {
            returnCon = true;
//            if (c > 1) {
//                logger.debug("Sleep. CurrentRetries: {}", c);
                //sleep((1 << (c - 1)) * this.config.getProducerRetryIntervalBaseInMilliSeconds());
//            }
            //while put topic, topic expiration is not allowed
            topic_2_lastActiveTime.put(msg.getTopic().getTopicText(), start);
            this.simpleClient.putTopic(msg.getTopic().getTopicText());
            try {
                //performance logging
                long getConnStart = System.currentTimeMillis();
                conn = getNSQConnection(msg.getTopic(), msg.getTopicShardingId(), cxt);
                long getConnEnd = System.currentTimeMillis() - getConnStart;
                if(PERF_LOG.isDebugEnabled()){
                    PERF_LOG.debug("{}: took {} milliSec to get nsq connection.", cxt.getTraceID(), getConnEnd);
                }
                if(getConnEnd > PerfTune.getInstance().getNSQConnElapseLimit()) {
                    PERF_LOG.warn("{}: took {} milliSec to get nsq connection. Limitation is {}", cxt.getTraceID(), getConnEnd, PerfTune.getInstance().getNSQConnElapseLimit());
                }

                if (conn == null) {
                    exceptions.add(new NSQDataNodesDownException("Could not get NSQd connection for " + msg.getTopic().toString() + ", topic may does not exist, or connection pool resource exhausted."));
                    continue;
                }
                //update msg partition with connection address partition
                msg.getTopic().setPartitionID(conn.getAddress().getPartition());
            }
            catch (NSQTopicNotFoundException | NSQSeedLookupConfigNotFoundException exp) {
                //throw it directly
                throw exp;
            }
            catch (NSQNoConnectionException badConnExp) {
                logger.info("Try invalidating partition selectors for {}, due to NSQNoConnectionException.");
                this.simpleClient.invalidatePartitionsSelector(msg.getTopic().getTopicText());
                exceptions.add(badConnExp);
                continue;
            }
            catch(NSQException nsqe) {
                exceptions.add(nsqe);
                continue;
            }
            //create PUB command
            try {
                final Pub pub = createPubCmd(msg);

                //check if address has partition info, if it does, update pub's partition
                if(conn.getAddress().hasPartition()) {
                    pub.overrideDefaultPartition(conn.getAddress().getPartition());
                }

                long pubAndWaitStart = System.currentTimeMillis();
                final NSQFrame frame = conn.commandAndGetResponse(pub);
                long pubAndWaitEnd = System.currentTimeMillis() - pubAndWaitStart;
                if(PERF_LOG.isDebugEnabled()){
                    PERF_LOG.debug("{}: took {} milliSec to send msg to and hear response from nsqd.", cxt.getTraceID(), pubAndWaitEnd);
                }
                if(pubAndWaitEnd > PerfTune.getInstance().getSendMSGLimit()) {
                    PERF_LOG.warn("{}: took {} milliSec to send message. Limitation is {}", cxt.getTraceID(), pubAndWaitEnd, PerfTune.getInstance().getSendMSGLimit());
                }

                handleResponse(msg.getTopic(), frame, conn);
                success.incrementAndGet();
                if(msg.isTraced() && frame instanceof MessageMetadata && TraceLogger.isTraceLoggerEnabled() && conn.getAddress().isHA())
                    TraceLogger.trace(this, conn, (MessageMetadata) frame);
                break;
            }
            catch(NSQPubFactoryInitializeException | NSQTagException | NSQTopicNotExtendableException | NSQExtNotSupportedException expShouldFail) {
                throw expShouldFail;
            }
            catch (Exception e) {
                returnCon = false;
                try {
                    bigPool.invalidateObject(conn.getAddress(), conn);
                    logger.info("Connection to {} invalidated.", conn.getAddress());
                } catch (Exception e1) {
                    logger.error("Fail to destroy connection {}.", conn.toString(), e1);
                }
                String msgStr = msg.getMessageBody();
                int maxlen = msgStr.length() > MAX_MSG_OUTPUT_LEN ? MAX_MSG_OUTPUT_LEN : msgStr.length();
                String errLog = String.format("MaxRetries: %d , CurrentRetries: %d , Address: %s , Topic: %s, MessageLength: %d, RawMessage: %s, ExtJsonHeader: %s, DesiredTag: %s.", retry, c,
                        conn.getAddress(), msg.getTopic(), msgStr.length(), msgStr.substring(0, maxlen), msg.getJsonHeaderExt(), msg.getDesiredTag());
                logger.error(errLog, e);
                //as to NSQInvalidMessageException throw it out after connection close.
                if(e instanceof NSQInvalidMessageException)
                    throw (NSQInvalidMessageException)e;
                NSQException nsqE = new NSQException(errLog, e);
                exceptions.add(nsqE);
                if (c >= retry) {
                    throw new NSQPubException(exceptions);
                }
            } finally {
                if(returnCon) {
                    long returnConnStart = System.currentTimeMillis();
                    bigPool.returnObject(conn.getAddress(), conn);
                    long returnConnEnd = System.currentTimeMillis() - returnConnStart;
                    if(PERF_LOG.isDebugEnabled())
                        PERF_LOG.debug("{}: took {} milliSec to return NSQ connection.", cxt.getTraceID(), returnConnEnd);
                    if(returnConnEnd > PerfTune.getInstance().getNSQConnReturnLimit()) {
                        PERF_LOG.warn("{}: took {} milliSec to return NSQ connection. Limitation is {}", cxt.getTraceID(), returnConnEnd, PerfTune.getInstance().getNSQConnReturnLimit());
                    }
                }
            }
        } // end loop
        if (c >= retry) {
            throw new NSQPubException(exceptions);
        }
        if(PERF_LOG.isDebugEnabled()){
            PERF_LOG.debug("{}: Producer took {} milliSec to send message to {}", cxt.getTraceID(), System.currentTimeMillis() - start, conn.getAddress());
        }
    }

    /**
     * function to create publish command based on traceability, get header() in command is not invoked here
     * @return Pub command
     */
    private Pub createPubCmd(final Message msg) throws NSQPubFactoryInitializeException {
       return PubCmdFactory.getInstance(!this.config.getUserSpecifiedLookupAddress()).create(msg, this.config);
    }

    private void handleResponse(final Topic topic, NSQFrame frame, NSQConnection conn) throws NSQException {
        if (frame == null) {
            logger.warn("the nsq frame is null.");
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
                        this.simpleClient.invalidatePartitionsSelector(topic.getTopicText());
                        //backoff for nsqd consensus, if there is one
                        try {
                            Thread.sleep(NSQ_LEADER_NOT_READY_TIMEOUT);
                        } catch (InterruptedException e) {
                            logger.error("Publish process interrupted waiting for nsqd consensus.");
                        }
                        logger.info("Partitions info for {} invalidated and related lookup force updated.", topic);
                        throw new NSQInvalidDataNodeException(topic.getTopicText());
                    }
                    case E_EXT_NOT_SUPPORT: {
                        logger.error("Json extension header not support in target topic.", err.getMessage());
                        throw new NSQExtNotSupportedException(topic.getTopicText() + " Error:" + err.getMessage());
                    }
                    case E_TAG_NOT_SUPPORT: {
                        logger.error("Tag not support in target topic.", err.getMessage());
                        throw new NSQTagNotSupportedException(topic.getTopicText() + " Error:" + err.getMessage());
                    }
                    case E_PUB_FAILED: {
                        logger.error("Address: {} , Frame: {}", conn.getAddress(), frame);
                        throw new NSQPubFailedException("publish to " + topic.getTopicText() + " failed. Address " + conn.getAddress() + ", Error Frame: " + frame);
                    }
                    case E_INVALID: {
                        throw new NSQInvalidException(err.getMessage());
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
        switch (frame.getType()) {
            case RESPONSE_FRAME: {
                if (frame.isHeartBeat() && conn.isIdentitySent()) {
                    this.simpleClient.incoming(frame, conn);
                } else {
                    conn.addResponseFrame((ResponseFrame) frame);
                }
                break;
            }
            case ERROR_FRAME: {
                final ErrorFrame err = (ErrorFrame) frame;
                conn.addErrorFrame(err);
                logger.warn("Error-Frame from {} , frame: {}", conn.getAddress(), frame);
                break;
            }
            default: {
                logger.warn("Invalid frame-type from {} , frame-type: {} , frame: {}", conn.getAddress(), frame.getType(), frame);
                break;
            }
        }
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
        if(this.started.get() && this.closing.compareAndSet(Boolean.FALSE, Boolean.TRUE)) {
            LookupAddressUpdate.getInstance().removeDefaultSeedLookupConfig(this.simpleClient.getLookupLocalID());
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
