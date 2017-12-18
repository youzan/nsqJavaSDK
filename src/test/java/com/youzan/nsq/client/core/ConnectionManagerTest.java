package com.youzan.nsq.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.youzan.nsq.client.IConsumeInfo;
import com.youzan.nsq.client.MockedConsumer;
import com.youzan.nsq.client.MockedNSQConnectionImpl;
import com.youzan.nsq.client.MockedNSQSimpleClient;
import com.youzan.nsq.client.core.command.Identify;
import com.youzan.nsq.client.core.command.Magic;
import com.youzan.nsq.client.core.command.Rdy;
import com.youzan.nsq.client.core.command.Sub;
import com.youzan.nsq.client.entity.*;
import com.youzan.nsq.client.network.netty.NSQClientInitializer;
import com.youzan.nsq.client.utils.TopicUtil;
import com.youzan.util.IOUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by lin on 17/6/26.
 */
public class ConnectionManagerTest {

    private final Logger logger = LoggerFactory.getLogger(ConnectionManagerTest.class.getName());
    private Properties props = new Properties();
    private NSQConfig config = new NSQConfig("BaseConsumer");
    private String lookupAddr;
    private Bootstrap bootstrap;
    private EventLoopGroup eventLoopGroup;

    @BeforeClass
    public void init() throws IOException {
        logger.info("At {} , initialize: {}", System.currentTimeMillis(), this.getClass().getName());
        System.setProperty("nsq.sdk.configFilePath", "src/test/resources/configClientTest.properties");
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }
        final String env = props.getProperty("env");
        logger.debug("The environment is {} .", env);
        lookupAddr = props.getProperty("lookup-addresses");
        final String connTimeout = props.getProperty("connectTimeoutInMillisecond");
        final String msgTimeoutInMillisecond = props.getProperty("msgTimeoutInMillisecond");
        final String threadPoolSize4IO = props.getProperty("threadPoolSize4IO");

        config = new NSQConfig();
        config.setLookupAddresses(lookupAddr);
        config.setConnectTimeoutInMillisecond(Integer.valueOf(connTimeout));
        config.setMsgTimeoutInMillisecond(Integer.valueOf(msgTimeoutInMillisecond));
        config.setThreadPoolSize4IO(Integer.valueOf(threadPoolSize4IO));


        //netty setup
        this.bootstrap = new Bootstrap();
        this.eventLoopGroup = new NioEventLoopGroup(config.getNettyPoolSize());
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeoutInMillisecond());
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new NSQClientInitializer());
    }

    @Test
    public void testBackoff() throws IOException, InterruptedException {
        backoff();
    }


    private ConnectionManager backoff() throws IOException, InterruptedException {
        NSQConfig config = (NSQConfig) this.config.clone();
        String topicName = "JavaTesting-Producer-Base";
        JsonNode lookupResp = IOUtil.readFromUrl(new URL("http://" + lookupAddr + "/lookup?topic=" + topicName + "&access=r"));
        JsonNode partition = lookupResp.get("partitions").get("0");
        Address addr1 = new Address(partition.get("broadcast_address").asText(), partition.get("tcp_port").asText(), partition.get("version").asText(), topicName, 0, false);
        NSQConnection con1 = connect(addr1, topicName, 0, "BaseConsumer", config);

        ConnectionManager conMgr = new ConnectionManager(new IConsumeInfo() {
            @Override
            public float getLoadFactor() {
                return 0;
            }

            @Override
            public int getRdyPerConnection() {
                return 0;
            }

            @Override
            public boolean isConsumptionEstimateElapseTimeout() {
                return false;
            }
        });
        conMgr.subscribe(topicName, con1);
        conMgr.backoff(topicName, null);
        Thread.sleep(100);

        assert 0 == con1.getCurrentRdyCount();
        return conMgr;
    }

    @Test
    public void testResume() throws IOException, InterruptedException {
        ConnectionManager conMgr = backoff();
        conMgr.resume("JavaTesting-Producer-Base", null);
        Thread.sleep(100);

        Assert.assertEquals(conMgr.getSubscribeConnections("JavaTesting-Producer-Base")
                .iterator()
                .next()
                .getConn()
                .getCurrentRdyCount(), 1);
    }

    private NSQConnection connect(Address addr, String topic, int partition, String channel, NSQConfig config) throws InterruptedException {
        ChannelFuture chFuture = bootstrap.connect(addr.getHost(), addr.getPort());
        final CountDownLatch connLatch = new CountDownLatch(1);
        chFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) throws Exception {
                if (channelFuture.isSuccess())
                    connLatch.countDown();
            }
        });
        connLatch.await(500, TimeUnit.MILLISECONDS);
        Channel ch = chFuture.channel();
        MockedNSQConnectionImpl con1 = new MockedNSQConnectionImpl(0, addr, ch, config);
        con1.setTopic(new Topic(topic, partition));
        NSQSimpleClient simpleClient = new MockedNSQSimpleClient(Role.Consumer, false);
        ch.attr(Client.STATE).set(simpleClient);
        ch.attr(NSQConnection.STATE).set(con1);
        con1.command(Magic.getInstance());
        con1.command(new Identify(config, addr.isTopicExtend()));
        Thread.sleep(100);
        con1.command(new Sub(new Topic(topic, partition), channel));
        Thread.sleep(100);
        con1.command(new Rdy(1));
        Thread.sleep(100);

        return con1;
    }

//    @Test
//    public void testRdyDecline() throws Exception {
//        logger.info("[testRdyDecline] starts.");
//        ConnectionManager conMgr = null;
//        String topic = "testRdyDec";
//        String channel = "BaseConsumer";
//        String adminHttp = "http://" + props.getProperty("admin-address");
//        try {
//            TopicUtil.createTopic(adminHttp, topic, 5, 1, channel);
//            TopicUtil.createTopicChannel(adminHttp, topic, channel);
//
//            NSQConfig config = (NSQConfig) this.config.clone();
//            config.setRdy(5);
//            conMgr = new ConnectionManager(new IConsumeInfo() {
//                @Override
//                public float getLoadFactor() {
//                    //water high
//                    return 2;
//                }
//
//                @Override
//                public int getRdyPerConnection() {
//                    return 6;
//                }
//
//                @Override
//                public boolean isConsumptionEstimateElapseTimeout() {
//                    return true;
//                }
//            });
//
//            int partitionNum = 5;
//            JsonNode lookupResp = IOUtil.readFromUrl(new URL("http://" + lookupAddr + "/lookup?topic=" + topic + "&access=r"));
//            List<NSQConnection> connList = new ArrayList<>(partitionNum);
//            for (int i = 0; i < partitionNum; i++) {
//                JsonNode partition = lookupResp.get("partitions").get("" + i);
//                Address addr1 = new Address(partition.get("broadcast_address").asText(), partition.get("tcp_port").asText(), partition.get("version").asText(), topic, 0, false);
//                NSQConnection con = connect(addr1, topic, i, channel, config);
//                conMgr.subscribe(topic, con, 5);
//                connList.add(con);
//            }
//
//            conMgr.start(0);
//            logger.info("Sleep 30sec to wait for rdy declining");
//            Thread.sleep(30000);
//
//            for (NSQConnection conn : connList) {
//                Assert.assertEquals(conn.getCurrentRdyCount(), 1);
//            }
//        } finally {
//            conMgr.close();
//            TopicUtil.deleteTopicChannel(adminHttp, topic, channel);
//            logger.info("[testRdyDecline] ends.");
//        }
//    }

    @Test
    public void testRdyIncrease() throws Exception {
        logger.info("[testRdyIncrease] starts.");
        ConnectionManager conMgr = null;
            NSQConfig config = (NSQConfig) this.config.clone();
            config.setRdy(5);
            conMgr = new ConnectionManager(new IConsumeInfo() {
                @Override
                public float getLoadFactor() {
                    return 0.5f;
                }

                @Override
                public int getRdyPerConnection() {
                    return 5;
                }

                @Override
                public boolean isConsumptionEstimateElapseTimeout() {
                    return false;
                }
            });

            int partitionNum = 5;
            String topic = "testRdyInc";
            String channel = "BaseConsumer";
            String adminHttp = "http://" + props.getProperty("admin-address");
        try{
            TopicUtil.createTopic(adminHttp, topic, 5, 1, channel);
            TopicUtil.createTopicChannel(adminHttp, topic, channel);
            JsonNode lookupResp = IOUtil.readFromUrl(new URL("http://" + lookupAddr + "/lookup?topic=" + topic + "&access=r"));
            List<NSQConnection> connList = new ArrayList<>(partitionNum);
            for (int i = 0; i < partitionNum; i++) {
                JsonNode partition = lookupResp.get("partitions").get("" + i);
                Address addr1 = new Address(partition.get("broadcast_address").asText(), partition.get("tcp_port").asText(), partition.get("version").asText(), topic, 0, false);
                NSQConnection con = connect(addr1, topic, i, "BaseConsumer", config);
                con.setExpectedRdy(5);
                conMgr.subscribe(topic, con);
                connList.add(con);
            }

            conMgr.start(0);
            logger.info("Sleep 30sec to wait for rdy increasing");
            Thread.sleep(30000);

            for (NSQConnection conn : connList) {
                Assert.assertEquals(conn.getCurrentRdyCount(), 5);
            }
        } finally {
            conMgr.close();
            TopicUtil.deleteTopic(adminHttp, topic);
            logger.info("[testRdyIncrease] ends.");
        }
    }

    public void testSettingRdy() throws Exception {
        logger.info("[testSettingRdy] starts.");
        //default with
        ConnectionManager conMgr = null;
        String topic = "testSettingRey";
        String adminHttp = "http://" + props.getProperty("admin-address");
        try {
            TopicUtil.createTopic(adminHttp, topic, 2, 1, "default");
            TopicUtil.createTopicChannel(adminHttp, topic, "default");
        } finally {

        }
    }

    @Test
    public void testExpectedRdy() throws Exception {
        logger.info("[testExpectedRdy] starts.");
        ConnectionManager conMgr = null;
        String topic = "test5Par1Rep";
        String adminHttp = "http://" + props.getProperty("admin-address");
        try {
            TopicUtil.createTopic(adminHttp, topic, 5, 1, "default");
            TopicUtil.createTopicChannel(adminHttp, topic, "default");

            NSQConfig config = (NSQConfig) this.config.clone();
            config.setRdy(6);
            conMgr = new ConnectionManager(new IConsumeInfo() {
                @Override
                public float getLoadFactor() {
                    return 0.5f;
                }

                @Override
                public int getRdyPerConnection() {
                    return 4;
                }

                @Override
                public boolean isConsumptionEstimateElapseTimeout() {
                    return false;
                }
            });

            int partitionNum = 5;
            JsonNode lookupResp = IOUtil.readFromUrl(new URL("http://" + lookupAddr + "/lookup?topic=" + topic + "&access=r"));
            List<NSQConnection> connList = new ArrayList<>(partitionNum);
            for (int i = 0; i < partitionNum; i++) {
                JsonNode partition = lookupResp.get("partitions").get("" + i);
                Address addr1 = new Address(partition.get("broadcast_address").asText(), partition.get("tcp_port").asText(), partition.get("version").asText(), topic, 0, false);
                NSQConnection con = connect(addr1, topic, i, "BaseConsumer", config);
                conMgr.subscribe(topic, con);
                connList.add(con);
            }
            //pick 2 connection and fix another rdy
            connList.get(0).declineExpectedRdy();

            connList.get(1).declineExpectedRdy();
            connList.get(1).declineExpectedRdy();
            connList.get(1).declineExpectedRdy();
            connList.get(1).declineExpectedRdy();

            conMgr.start(0);
            Thread.sleep(10000);

            Assert.assertEquals(connList.get(0).getCurrentRdyCount(), 2);
            Assert.assertEquals(connList.get(1).getCurrentRdyCount(), 1);
        } finally {
            conMgr.close();
            TopicUtil.deleteTopic(adminHttp, topic);
            logger.info("[testExpectedRdy] ends.");
        }
    }


    @Test
    public void testRemoveConnectionWrapper() throws Exception {
        logger.info("[testRemoveConnectionWrapper] starts.");
        String topic = "testRemoveConWrapper";
        int par1 = 5;

        String topicJ = "testRemoveConWrapper_j";
        int par2 = 1;

        String adminHttp = "http://" + props.getProperty("admin-address");
        String channel = "BaseConsumer";
        try {
            TopicUtil.createTopic(adminHttp, topic, par1, 1, channel);
            TopicUtil.createTopicChannel(adminHttp, topic, channel);

            TopicUtil.createTopic(adminHttp, topicJ, par2, 1, channel);
            TopicUtil.createTopicChannel(adminHttp, topicJ, channel);

            NSQConfig config = (NSQConfig) this.config.clone();
            config.setRdy(6);
            ConnectionManager conMgr = new ConnectionManager(new IConsumeInfo() {
                @Override
                public float getLoadFactor() {
                    return 0;
                }

                @Override
                public int getRdyPerConnection() {
                    return 6;
                }

                @Override
                public boolean isConsumptionEstimateElapseTimeout() {
                    return false;
                }
            });

            JsonNode lookupResp = IOUtil.readFromUrl(new URL("http://" + lookupAddr + "/lookup?topic=" + topic + "&access=r"));
            List<NSQConnection> connList = new ArrayList<>(par1);
            for (int i = 0; i < par1; i++) {
                JsonNode partition = lookupResp.get("partitions").get("" + i);
                Address addr1 = new Address(partition.get("broadcast_address").asText(), partition.get("tcp_port").asText(), partition.get("version").asText(), topic, i, false);
                NSQConnection con = connect(addr1, topic, i, "BaseConsumer", config);
                conMgr.subscribe(topic, con);
                connList.add(con);
            }

            //pick another topic
            lookupResp = IOUtil.readFromUrl(new URL("http://" + lookupAddr + "/lookup?topic=" + topicJ + "&access=r"));
            for (int i = 0; i < par2; i++) {
                JsonNode partition = lookupResp.get("partitions").get("" + i);
                Address addr1 = new Address(partition.get("broadcast_address").asText(), partition.get("tcp_port").asText(), partition.get("version").asText(), topicJ, i, false);
                NSQConnection con = connect(addr1, topicJ, i, "BaseConsumer", config);
                conMgr.subscribe(topicJ, con);
                connList.add(con);
            }

            //remove par0 par1 for test5Par1Rep
            Map<String, List<ConnectionManager.NSQConnectionWrapper>> removeMap = new HashMap<>();
            removeMap.put(topic, new ArrayList<ConnectionManager.NSQConnectionWrapper>());
            List<ConnectionManager.NSQConnectionWrapper> conLists = removeMap.get(topic);
            conLists.add(new ConnectionManager.NSQConnectionWrapper(connList.get(0)));
            conLists.add(new ConnectionManager.NSQConnectionWrapper(connList.get(1)));

            Assert.assertTrue(conMgr.remove(removeMap));
            Assert.assertEquals(((ConnectionManager.ConnectionWrapperSet)conMgr.getSubscribeConnections(topic)).getTotalRdy(), 3, "total rdy for " + topic + " does not equal.");

            Set<ConnectionManager.NSQConnectionWrapper> conWprs = conMgr.getSubscribeConnections(topic);
            Assert.assertEquals(conWprs.size(), 3);
            conWprs = conMgr.getSubscribeConnections(topicJ);
            Assert.assertEquals(conWprs.size(), 1);

            conLists.add(new ConnectionManager.NSQConnectionWrapper(connList.get(2)));

            Assert.assertTrue(conMgr.remove(removeMap));
            Assert.assertEquals(((ConnectionManager.ConnectionWrapperSet)conMgr.getSubscribeConnections(topic)).getTotalRdy(), 2, "total rdy for " + topic + " does not equal.");

            conWprs = conMgr.getSubscribeConnections(topic);
            Assert.assertEquals(conWprs.size(), 2);

            //add another topic-> conn into map
            removeMap.put(topicJ, new ArrayList<ConnectionManager.NSQConnectionWrapper>());
            List<ConnectionManager.NSQConnectionWrapper> conListsJ = removeMap.get(topicJ);
            conListsJ.add(new ConnectionManager.NSQConnectionWrapper(connList.get(5)));

            Assert.assertTrue(conMgr.remove(removeMap));

            conWprs = conMgr.getSubscribeConnections(topicJ);
            Assert.assertNull(conWprs);

        } finally {
            TopicUtil.deleteTopic(adminHttp, topic);
            TopicUtil.deleteTopic(adminHttp, topicJ);
            logger.info("[testRemoveConnectionWrapper] ends.");
        }
    }

    @Test
    public void testSubscribeConnWhileBackoff() throws Exception {
        logger.info("[testSubscribeConnWhileBackoff] starts.");
        int par = 5;
        final String topic = "test5Par1Rep";
        String channel = "BaseConsumer";
        String adminHttp = "http://" + props.getProperty("admin-address");
        try{
            TopicUtil.createTopic(adminHttp, topic, 5, 1, channel);
            TopicUtil.createTopicChannel(adminHttp, topic, channel);
            NSQConfig config = (NSQConfig) this.config.clone();
            config.setRdy(6);
            final ConnectionManager conMgr = new ConnectionManager(new IConsumeInfo() {
                @Override
                public float getLoadFactor() {
                    return 0;
                }

                @Override
                public int getRdyPerConnection() {
                    return 6;
                }

                @Override
                public boolean isConsumptionEstimateElapseTimeout() {
                    return false;
                }
            });

            JsonNode lookupResp = IOUtil.readFromUrl(new URL("http://" + lookupAddr + "/lookup?topic=" + topic + "&access=r"));
            final List<NSQConnection> connList = new ArrayList<>(par);
            //subscribe 3 of 5 partitions
            for (int i = 0; i < par; i++) {
                JsonNode partition = lookupResp.get("partitions").get("" + i);
                Address addr = new Address(partition.get("broadcast_address").asText(), partition.get("tcp_port").asText(), partition.get("version").asText(), topic, i, false);
                NSQConnection con = connect(addr, topic, i, "BaseConsumer", config);
                connList.add(con);
            }

            //subscribe first
            conMgr.subscribe(topic, connList.get(0));

            //backoff & subscribe, backoff should always works
            ExecutorService exec = Executors.newCachedThreadPool();
            for(int i = 0; i < connList.size() - 1; i++ ) {
                final int idx = i;
                exec.submit(new Runnable() {
                    @Override
                    public void run() {
                        conMgr.subscribe(topic, connList.get(idx));
                    }
                });
            }

            Thread.sleep(10);
            CountDownLatch latch = new CountDownLatch(1);
            conMgr.backoff(topic, latch);
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
            conMgr.subscribe(topic, connList.get(4));
            Thread.sleep(100);

            //assert all connections are backoff
            Set<ConnectionManager.NSQConnectionWrapper> conns = conMgr.getSubscribeConnections(topic);
            ConnectionManager.ConnectionWrapperSet ws = (ConnectionManager.ConnectionWrapperSet) conns;
            Assert.assertTrue(ws.isBackoff());
            for(ConnectionManager.NSQConnectionWrapper wrapper:conns) {
                Assert.assertTrue(wrapper.getConn().isBackoff());
            }

            //resume consumption
            CountDownLatch latchR = new CountDownLatch(1);
            conMgr.resume(topic, latchR);
            Assert.assertTrue(latchR.await(10, TimeUnit.SECONDS));
            Assert.assertTrue(!ws.isBackoff());
            for(ConnectionManager.NSQConnectionWrapper wrapper:conns) {
                Assert.assertFalse(wrapper.getConn().isBackoff());
            }
        }finally {
            TopicUtil.deleteTopic(adminHttp, topic);
            logger.info("[testSubscribeConnWhileBackoff] ends");
        }
    }

    @Test
    public void testInvalidateConnection() throws Exception {
        logger.info("[testInvalidateConnection] starts.");
        NSQConfig localConfig = (NSQConfig) config.clone();
        localConfig.setConsumerName("BaseConsumer");
        final ConnectionManager conMgr = new ConnectionManager(new IConsumeInfo() {
            @Override
            public float getLoadFactor() {
                return 0;
            }

            @Override
            public int getRdyPerConnection() {
                return 6;
            }

            @Override
            public boolean isConsumptionEstimateElapseTimeout() {
                return false;
            }
        });

        final String topic = "testInvalidateConnection";
        int par = 5;
        String adminHttp = "http://" + props.getProperty("admin-address");
        try{
            TopicUtil.createTopic(adminHttp, topic, 5, 1, "default");
            TopicUtil.createTopicChannel(adminHttp, topic, "default");
            JsonNode lookupResp = IOUtil.readFromUrl(new URL("http://" + lookupAddr + "/lookup?topic=" + topic + "&access=r"));
            MockedConsumer consumer = new MockedConsumer(localConfig, null);
            consumer.setConnectionManager(conMgr);
            consumer.start();
            Set<String> topics = new HashSet<>();
            topics.add(topic);
            for (int i = 0; i < par; i++) {
                JsonNode partition = lookupResp.get("partitions").get("" + i);
                Address addr = new Address(partition.get("broadcast_address").asText(), partition.get("tcp_port").asText(), partition.get("version").asText(), topic, i, false);
                consumer.connect(addr);
            }
            Map<Address, NSQConnection> addr2Conns = consumer.getAddress2Conn();
            //invalidate conenction by closing
            for(NSQConnection con : addr2Conns.values()) {
               con.close();
            }
            //invalidate connection
            consumer.connect();
            addr2Conns = consumer.getAddress2Conn();
            Assert.assertTrue(addr2Conns.keySet().size() == 0);
        } finally {
            TopicUtil.deleteTopic(adminHttp, topic);
            logger.info("[testInvalidateConnection] ends.");
        }
    }

    @Test(invocationCount = 3)
    public void testConcurrentBackoffAndResume() throws Exception {
        logger.info("[testConcurrentBackoffAncResume] starts");
        final String topicName = "testConBackoffResume";
        String channel = "default";
        int parNum = 5;
        String adminUrl = "http://" + props.getProperty("admin-address");
        final ConnectionManager conMgr = new ConnectionManager(new IConsumeInfo() {
            @Override
            public float getLoadFactor() {
                return 0;
            }

            @Override
            public int getRdyPerConnection() {
                return 3;
            }

            @Override
            public boolean isConsumptionEstimateElapseTimeout() {
                return false;
            }
        });

        try {
            TopicUtil.createTopic(adminUrl, topicName, 5, 1, channel, false, false);
            TopicUtil.createTopicChannel(adminUrl, topicName, channel);
            //then we have 5 channel

            final List<NSQConnection> connList = new ArrayList<>(parNum);
            JsonNode lookupResp = IOUtil.readFromUrl(new URL("http://" + lookupAddr + "/lookup?topic=" + topicName + "&access=r"));
            for (int i = 0; i < parNum; i++) {
                JsonNode partition = lookupResp.get("partitions").get("" + i);
                Address addr = new Address(partition.get("broadcast_address").asText(), partition.get("tcp_port").asText(), partition.get("version").asText(), topicName, i, false);
                NSQConnection con = connect(addr, topicName, i, channel, config);
                connList.add(con);
            }

            //subscribe first connection
            conMgr.subscribe(topicName, connList.get(0));
            conMgr.start(0);

            ExecutorService exec = Executors.newCachedThreadPool();
            final Semaphore endSignal = new Semaphore(0);
            final Semaphore startSignal = new Semaphore(0);

            //backoff
            exec.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        startSignal.acquire();
                    } catch (InterruptedException e) {
                        logger.error("interrupted waiting for start signal", e);
                        Assert.fail();
                    }
                    while (!endSignal.tryAcquire()) {
                        CountDownLatch latch = new CountDownLatch(1);
                        conMgr.backoff(topicName, latch);
                        try {
                            if (!latch.await(1, TimeUnit.SECONDS)) {
                                Assert.fail("timeout waiting for backoff");
                            }
                        } catch (InterruptedException e) {
                            logger.error("interrupted.", e);
                            Assert.fail("interrupted waiting for backoff finish");
                        }
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            //ignore
                        }
                    }
                    logger.info("backoff loop ends");
                }
            });

            //resume
            exec.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        startSignal.acquire();
                    } catch (InterruptedException e) {
                        logger.error("interrupted waiting for start signal", e);
                        Assert.fail();
                    }
                    while (!endSignal.tryAcquire()) {
                        CountDownLatch latch = new CountDownLatch(1);
                        conMgr.resume(topicName, latch);
                        try {
                            if (!latch.await(1, TimeUnit.SECONDS)) {
                                Assert.fail("timeout waiting for backoff");
                            }
                        } catch (InterruptedException e) {
                            logger.error("interrupted.", e);
                            Assert.fail("interrupted waiting for backoff finish");
                        }
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            //ignore
                        }
                    }
                    logger.info("resume loop ends.");
                }
            });

            startSignal.release(2);
            Thread.sleep(1000);
            for (int i = 1; i < connList.size(); i++) {
                conMgr.subscribe(topicName, connList.get(i));
                Thread.sleep(1000);
            }
            Thread.sleep(10000);
            //now stop everything
            endSignal.release(2);
            Thread.sleep(1000);

            boolean base = connList.get(0).isBackoff();
            logger.info("Connection 0 is backoff: {}", base);
            for (NSQConnection con : connList) {
                Assert.assertTrue(con.isConnected());
                Assert.assertEquals(con.isBackoff(), base, "" + con + " isBackoff state does not equal to " + base);
            }

            CountDownLatch latch = new CountDownLatch(1);
            conMgr.resume(topicName, latch);
            Assert.assertTrue(latch.await(1, TimeUnit.SECONDS));
            Thread.sleep(20000);

            for (NSQConnection con : connList) {
                Assert.assertTrue(con.isConnected());
                Assert.assertEquals(con.getCurrentRdyCount(), config.getRdy());
            }

        }finally {
            conMgr.backoff(topicName, null);
            conMgr.close();
            TopicUtil.deleteTopic(adminUrl, topicName);
        }
    }

    @Test
    public void testProofreadTotalRdy() throws Exception {
        logger.info("[testProofreadTotalRdy] starts.");
        NSQConfig localConfig = (NSQConfig) config.clone();
        localConfig.setConsumerName("BaseConsumer");
        final ConnectionManager conMgr = new ConnectionManager(new IConsumeInfo() {
            @Override
            public float getLoadFactor() {
                return 0;
            }

            @Override
            public int getRdyPerConnection() {
                return 4;
            }

            @Override
            public boolean isConsumptionEstimateElapseTimeout() {
                return false;
            }
        });

        MockedConsumer consumer = null;
        final String topic = "testProofreadTotalRdy";
        int par = 5;
        String adminHttp = "http://" + props.getProperty("admin-address");
        try{
            TopicUtil.createTopic(adminHttp, topic, 5, 1, "default");
            TopicUtil.createTopicChannel(adminHttp, topic, "default");
            JsonNode lookupResp = IOUtil.readFromUrl(new URL("http://" + lookupAddr + "/lookup?topic=" + topic + "&access=r"));
            //set it 4 rdy per connection and make total 4*5 = 20
            localConfig.setRdy(4);
            consumer = new MockedConsumer(localConfig, null);
            consumer.setConnectionManager(conMgr);
            consumer.start();
            Set<String> topics = new HashSet<>();
            topics.add(topic);
            for (int i = 0; i < par; i++) {
                JsonNode partition = lookupResp.get("partitions").get("" + i);
                Address addr = new Address(partition.get("broadcast_address").asText(), partition.get("tcp_port").asText(), partition.get("version").asText(), topic, i, false);
                consumer.connect(addr);
            }

            conMgr.start(0);
            ConnectionManager.ConnectionWrapperSet cws = (ConnectionManager.ConnectionWrapperSet)conMgr.getSubscribeConnections(topic);
            for(ConnectionManager.NSQConnectionWrapper wrapper:cws) {
                wrapper.getConn().setExpectedRdy(4);
            }
            //sleep 30 for rdy to increase
            Thread.sleep(30000);
            cws.setTotalRdy(40);
            conMgr.proofreadTotalRdy(topic);

            Assert.assertEquals(cws.getTotalRdy(), 4*5);
        } finally {
            conMgr.close();
            consumer.close();
            TopicUtil.deleteTopic(adminHttp, topic);
            logger.info("[testProofreadTotalRdy] ends.");
        }
    }

    @Test
    public void testMakebadOfRdyRedistribute() throws Exception {
        logger.info("[testMakebadOfRdyRedistribute] starts.");
        String topicName = "testMakebadOfRdyDist";
        String channel = "default";
        String admin = "http://" + props.getProperty("admin-address");

        final ConnectionManager conMgr = new ConnectionManager(new IConsumeInfo() {
            @Override
            public float getLoadFactor() {
                return 0;
            }

            @Override
            public int getRdyPerConnection() {
                return 4;
            }

            @Override
            public boolean isConsumptionEstimateElapseTimeout() {
                return false;
            }
        });

        try{
            int parNum = 5;
            TopicUtil.createTopic(admin, topicName, parNum, 1, channel, false, false);
            TopicUtil.createTopicChannel(admin, topicName, channel);
            //then we have 5 channel
//            conMgr.setRdyUpdatePolicyClass(BadRdyUpdatePolicy.class.getName());

            final List<NSQConnection> connList = new ArrayList<>(parNum);
            JsonNode lookupResp = IOUtil.readFromUrl(new URL("http://" + lookupAddr + "/lookup?topic=" + topicName + "&access=r"));
            for (int i = 0; i < parNum; i++) {
                JsonNode partition = lookupResp.get("partitions").get("" + i);
                Address addr = new Address(partition.get("broadcast_address").asText(), partition.get("tcp_port").asText(), partition.get("version").asText(), topicName, i, false);
                NSQConnection con = connect(addr, topicName, i, channel, config);
                connList.add(con);
                conMgr.subscribe(topicName, con);
            }
            conMgr.start(0);
            Thread.sleep(10000);
            //check if redistribute is still alive
            conMgr.getRedistributeRunnable().run();

        }finally {
            TopicUtil.deleteTopic(admin, topicName);
            conMgr.close();
            logger.info("[testMakebadOfRdyRedistribute] ends.");
        }
    }

    @DataProvider(name = "topicNums")
    public static Object[][] topicNums() {
        return new Object[][]{
//                {new Integer(2)},
//                {new Integer(4)},
                {new Integer(6)}
//                {new Integer(8)}
        };
    }

    @Test(dataProvider = "topicNums", dataProviderClass = ConnectionManagerTest.class)
    public void testConsumeMultiTopicsRdy(int topicsNum) throws Exception {
        int rdyMax = Runtime.getRuntime().availableProcessors() * 4;
        String adminHtp = "http://" + props.getProperty("admin-address");
        final List<String> list = new ArrayList<>();
        int parNum = 4;
        int repNum = 1;
        if(parNum * topicsNum > rdyMax) {
            rdyMax = parNum * topicsNum;
        }
        for(int i = 0; i < topicsNum; i++) {
            list.add("testConsume_" + i);
        }
        MockedConsumer consumer = null;
        try {
            for (String topic : list) {
                TopicUtil.createTopic(adminHtp, topic, parNum, repNum, "default");
                TopicUtil.createTopicChannel(adminHtp, topic, "default");
            }

            NSQConfig localConfig = new NSQConfig();
            localConfig.setConsumerName("default");

            final ConnectionManager conMgr = new ConnectionManager(new IConsumeInfo() {
                @Override
                public float getLoadFactor() {
                    return 0;
                }

                @Override
                public int getRdyPerConnection() {
                    return 4;
                }

                @Override
                public boolean isConsumptionEstimateElapseTimeout() {
                    return false;
                }
            });

            consumer = new MockedConsumer(localConfig, null);
            consumer.setConnectionManager(conMgr);
            consumer.start();

            for(String topic : list) {
                JsonNode lookupResp = IOUtil.readFromUrl(new URL("http://" + lookupAddr + "/lookup?topic=" + topic + "&access=r"));
                for (int i = 0; i < parNum; i++) {
                    JsonNode partition = lookupResp.get("partitions").get("" + i);
                    Address addr = new Address(partition.get("broadcast_address").asText(), partition.get("tcp_port").asText(), partition.get("version").asText(), topic, i, false);
                    consumer.connect(addr);
                }
            }

            conMgr.start(0);
            Thread.sleep(topicsNum * 5000);

            //assert all topics has same rdy
            for(String topic : list) {
                logger.info("Check rdy for {}", topic);
                ConnectionManager.ConnectionWrapperSet conSet = (ConnectionManager.ConnectionWrapperSet) conMgr.getSubscribeConnections(topic);
                Assert.assertEquals(conSet.getTotalRdy(), 3 * parNum, "total rdy does not match");
                for(ConnectionManager.NSQConnectionWrapper connWrapper : conSet) {
                    Assert.assertEquals( connWrapper.getConn().getCurrentRdyCount(), 3);
                }
            }

        }finally {
            logger.info("[testConsumeMultiTopicsRdy] ends");
            consumer.close();
            for(String topic:list) {
                TopicUtil.deleteTopic(adminHtp, topic);
            }
        }
    }
}
