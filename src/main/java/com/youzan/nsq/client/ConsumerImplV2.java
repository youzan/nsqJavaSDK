package com.youzan.nsq.client;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.core.Client;
import com.youzan.nsq.client.core.KeyedConnectionPoolFactory;
import com.youzan.nsq.client.core.NSQConnection;
import com.youzan.nsq.client.core.NSQSimpleClient;
import com.youzan.nsq.client.core.command.Close;
import com.youzan.nsq.client.core.command.Finish;
import com.youzan.nsq.client.core.command.NSQCommand;
import com.youzan.nsq.client.core.command.Rdy;
import com.youzan.nsq.client.core.command.ReQueue;
import com.youzan.nsq.client.core.command.Sub;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.entity.Response;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.network.frame.ErrorFrame;
import com.youzan.nsq.client.network.frame.MessageFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.util.ConcurrentSortedSet;
import com.youzan.util.IOUtil;
import com.youzan.util.NamedThreadFactory;

/**
 * Use {@code NSQConfig} to set the lookup cluster. <br />
 * Expose to Client Code. Connect to one cluster(includes many brokers).
 * 
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class ConsumerImplV2 implements Consumer {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerImplV2.class);
    private volatile boolean started = false;
    private final Client simpleClient;
    private final NSQConfig config;
    private final GenericKeyedObjectPoolConfig poolConfig;
    private final KeyedConnectionPoolFactory factory;
    private GenericKeyedObjectPool<Address, NSQConnection> bigPool = null;
    private final AtomicInteger success = new AtomicInteger(0);
    private final AtomicInteger total = new AtomicInteger(0);
    /**
     * Record the client's request time
     */
    private volatile long lastTimeInMillisOfClientRequest = System.currentTimeMillis();

    /*-
     * =========================================================================
     *                          
     * =========================================================================
     */
    private final ConcurrentHashMap<Address, Set<NSQConnection>> holdingConnections = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory(this.getClass().getName(), Thread.NORM_PRIORITY));

    /*-
     * =========================================================================
     *                          Client delegate to me
     * =========================================================================
     */
    private final MessageHandler handler;
    private final ExecutorService executor = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors() * 2,
            new NamedThreadFactory(this.getClass().getName() + "-ClientBusiness", Thread.MAX_PRIORITY));
    private final Optional<ScheduledFuture<?>> timeout = Optional.empty();

    /**
     * @param config
     * @param handler
     */
    public ConsumerImplV2(NSQConfig config, MessageHandler handler) {
        this.config = config;
        this.handler = handler;

        this.poolConfig = new GenericKeyedObjectPoolConfig();
        this.simpleClient = new NSQSimpleClient(config.getLookupAddresses(), config.getTopic());
        this.factory = new KeyedConnectionPoolFactory(this.config, this);
    }

    @Override
    public void start() throws NSQException {
        final String topic = this.config.getTopic();
        if (topic == null || topic.isEmpty()) {
            throw new NSQException("Please set topic name using {@code NSQConfig}");
        }
        if (this.config.getConsumerName() == null || this.config.getConsumerName().isEmpty()) {
            throw new IllegalArgumentException("Consumer Name is blank! Please check it!");
        }
        if (!this.started) {
            this.started = true;
            // setting all of the configs
            this.poolConfig.setLifo(false);
            this.poolConfig.setFairness(true);
            this.poolConfig.setTestOnBorrow(false);
            this.poolConfig.setJmxEnabled(false);
            this.poolConfig.setMinEvictableIdleTimeMillis(24 * 3600 * 1000L);
            this.poolConfig.setMinIdlePerKey(this.config.getThreadPoolSize4IO());
            this.poolConfig.setMaxIdlePerKey(this.config.getThreadPoolSize4IO());
            this.poolConfig.setMaxTotalPerKey(this.config.getThreadPoolSize4IO());
            // aquire connection waiting time
            this.poolConfig.setMaxWaitMillis(500);
            this.poolConfig.setBlockWhenExhausted(true);
            this.poolConfig.setTestWhileIdle(true);
            this.simpleClient.start();
            createBigPool();
            // POST
            connect();
            keepConnecting();
        }
    }

    /**
     * 
     */
    private void createBigPool() {
        this.bigPool = new GenericKeyedObjectPool<>(this.factory, this.poolConfig);
    }

    /**
     * schedule action
     */
    private void keepConnecting() {
        final int delay = _r.nextInt(120) + 60; // seconds
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                connect();
            } catch (Exception e) {
                logger.error("Exception", e);
            }
        }, delay, 1 * 60, TimeUnit.SECONDS);
    }

    private void connect() {
        final Set<Address> broken = new HashSet<>();
        holdingConnections.values().parallelStream().forEach((conns) -> {
            for (final NSQConnection c : conns) {
                try {
                    if (!c.isConnected()) {
                        c.close();
                        broken.add(c.getAddress());
                    }
                } catch (Exception e) {
                    logger.error("Exception occurs while detecting broken connections!", e);
                }
            }
        });

        final Set<Address> newDataNodes = getDataNodes().newSet();
        final Set<Address> oldDataNodes = new HashSet<>(this.holdingConnections.keySet());
        logger.debug("Prepare to connect new NSQd: {} , old NSQd: {} .", newDataNodes, oldDataNodes);
        if (newDataNodes.isEmpty() && oldDataNodes.isEmpty()) {
            return;
        }
        if (newDataNodes.isEmpty()) {
            logger.error("It can not get new DataNodes (NSQd). It will create a new pool next time!");
        }
        /*-
         * =====================================================================
         *                                Step 1:
         *                    以newDataNodes为主的差集: 新建Brokers
         * =====================================================================
         */
        final Set<Address> except1 = new HashSet<>(newDataNodes);
        except1.removeAll(oldDataNodes);
        if (!except1.isEmpty()) {
            newConnections(except1);
        } else {
            logger.debug("No need to create new NSQd connections!");
        }
        /*-
         * =====================================================================
         *                                Step 2:
         *                    以oldDataNodes为主的差集: 删除Brokers
         * =====================================================================
         */
        final Set<Address> except2 = new HashSet<>(oldDataNodes);
        except2.removeAll(newDataNodes);
        if (except2.isEmpty()) {
            logger.debug("No need to destory old NSQd connections!");
        } else {
            except2.parallelStream().forEach((address) -> {
                if (address == null) {
                    return;
                }
                bigPool.clear(address);
                if (holdingConnections.containsKey(address)) {
                    final Set<NSQConnection> conns = holdingConnections.get(address);
                    if (conns != null) {
                        conns.forEach((c) -> {
                            try {
                                backoff(c);
                            } catch (Exception e) {
                                logger.error("It can not backoff the connection!", e);
                            } finally {
                                IOUtil.closeQuietly(c);
                            }
                        });
                    }
                }
                holdingConnections.remove(address);
            });
        }
        /*-
         * =====================================================================
         *                                Step 3:
         *                          干掉Broken Brokers.
         * =====================================================================
         */
        broken.parallelStream().forEach((address) -> {
            if (address == null) {
                return;
            }
            try {
                holdingConnections.remove(address);
                bigPool.clear(address);
            } catch (Exception e) {
                logger.error("Exception", e);
            }
        });
    }

    /**
     * @param brokers
     */
    private void newConnections(final Set<Address> brokers) {
        brokers.parallelStream().forEach((address) -> {
            try {
                newConnections4OneBroker(address);
            } catch (Exception e) {
                logger.error("Exception", e);
            }
        });
    }

    /**
     * @param address
     */
    private void newConnections4OneBroker(Address address) {
        if (address == null) {
            logger.error("Your input address is blank!");
            return;
        }
        try {
            bigPool.clear(address);
            bigPool.preparePool(address);
        } catch (Exception e) {
            logger.error("Exception", e);
        }
        // create new pool(connect to one broker)
        final List<NSQConnection> okConns = new ArrayList<>(config.getThreadPoolSize4IO());
        for (int i = 0; i < config.getThreadPoolSize4IO(); i++) {
            NSQConnection newConn = null;
            try {
                newConn = bigPool.borrowObject(address);
                initConn(newConn); // subscribe
                if (!holdingConnections.containsKey(address)) {
                    holdingConnections.putIfAbsent(address, new HashSet<>());
                }
                final Set<NSQConnection> conns = holdingConnections.get(address);
                conns.add(newConn);

                okConns.add(newConn);
            } catch (Exception e) {
                logger.error("Exception", e);
                if (newConn != null) {
                    bigPool.returnObject(address, newConn);
                }
            }
        }
        // finally
        for (NSQConnection c : okConns) {
            try {
                bigPool.returnObject(c.getAddress(), c);
            } catch (Exception e) {
                logger.error("Exception", e);
            }
        }
        if (okConns.size() == config.getThreadPoolSize4IO()) {
            logger.info("Having created a pool for one broker, it felt good.");
        } else {
            logger.info("Want the pool size {} , actually the size {}", config.getThreadPoolSize4IO(), okConns.size());
        }
        okConns.clear();
    }

    /**
     * @param newConn
     */
    private void initConn(NSQConnection newConn) {
        newConn.command(new Sub(config.getTopic(), config.getConsumerName()));
        final int initRdy = Runtime.getRuntime().availableProcessors() - 1;
        newConn.command(new Rdy(initRdy));
        logger.info("Rdy {} message! It is new connection!", initRdy);
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

    @Override
    public void incoming(final NSQFrame frame, final NSQConnection conn) {
        if (frame instanceof MessageFrame) {
            final MessageFrame msg = (MessageFrame) frame;
            final NSQMessage message = new NSQMessage(msg.getTimestamp(), msg.getAttempts(), msg.getMessageID(),
                    msg.getMessageBody());
            processMessage(message, conn);
            return;
        }
        simpleClient.incoming(frame, conn);
    }

    protected void processMessage(final NSQMessage message, final NSQConnection conn) {
        if (handler == null) {
            logger.error("No MessageHandler then drop the message {}", message);
            return;
        }
        try {
            executor.execute(() -> {
                logger.debug("Having consume the message {} , client showed great anxiety!", message.toString());
                boolean ok = false;
                int c = 0;
                while (c++ < 2 && !ok) {
                    try {
                        ok = handler.process(message);
                    } catch (Exception e) {
                        ok = false;
                        logger.error("Exception", e);
                    }
                }
                try {
                    final NSQCommand cmd;
                    if (ok) {
                        cmd = new Finish(message.getMessageID());
                    } else {
                        cmd = new ReQueue(message.getMessageID(), 60);
                        if (message.getReadableAttempts() > 10) {
                            logger.info("Processing 10 times is still a failure!");
                        }
                    }
                    conn.command(cmd);
                } catch (Exception e) {
                    logger.error("Exception", e);
                }
            });
        } catch (RejectedExecutionException re) {
            // TODO Halt Flow
        }
        // TODO Halt Flow
    }

    @Override
    public void backoff(NSQConnection conn) {
        simpleClient.backoff(conn);
    }

    @Override
    public void close() {
        cleanClose();
        if (factory != null) {
            factory.close();
        }
        if (bigPool != null) {
            bigPool.close();
        }
    }

    private void cleanClose() {
        holdingConnections.values().parallelStream().forEach((conns) -> {
            for (final NSQConnection c : conns) {
                try {
                    backoff(c);
                    final NSQFrame frame = c.commandAndGetResponse(Close.getInstance());
                    if (frame != null && frame instanceof ErrorFrame) {
                        final Response err = ((ErrorFrame) frame).getError();
                        if (err != null) {
                            logger.error(err.getContent());
                        }
                    }
                } catch (Exception e) {
                    logger.error("Exception", e);
                } finally {
                    IOUtil.closeQuietly(c);
                }
            }
        });
        holdingConnections.clear();
    }

    @Override
    public ConcurrentSortedSet<Address> getDataNodes() {
        return simpleClient.getDataNodes();
    }
}
